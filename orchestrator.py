"""
orchestrator.py — Full pipeline coordinator.

Decision flow:
                      ┌─────────────┐
                      │  Fetch Page │ (Playwright)
                      └──────┬──────┘
                             │
                      ┌──────▼──────┐
                      │   Change    │ Compare DOM + visual hash
                      │  Detection  │ against stored baseline
                      └──────┬──────┘
               drift?        │        no drift?
           ┌─────────────────┤        
           │                 │        ┌──────────────┐
           ▼                 ▼        │  Cache Lookup │
     Invalidate         ┌────────┐    └──────┬───────┘
     Cache              │ Cache  │     hit?  │  miss?
                        │ Lookup │     ┌─────┤──────┐
                        └────────┘     │     │      │
                                       ▼     │      ▼
                                  ┌─────┐   │  ┌──────────┐
                                  │Fast │   │  │   LLM    │
                                  │Extr │   │  │ Extractor│
                                  └──┬──┘   │  └────┬─────┘
                                     │      │       │
                                     └──────┴───────┘
                                             │
                                      ┌──────▼──────┐
                                      │  Validate   │ Type check + null rate
                                      │  + Schema   │
                                      └──────┬──────┘
                                             │
                                      ┌──────▼──────┐
                                      │   Storage   │ SQLite + audit log
                                      └─────────────┘
"""

import asyncio
import logging
import sys
import os
from typing import Optional

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schema import ProductSchema, SelectorMap, ExtractionResult
from change_detector import ChangeDetector
from llm_extractor import LLMExtractor
from fast_extractor import FastExtractor
from selector_cache import SelectorCache
from db import CrawlerDB

logger = logging.getLogger(__name__)

SCHEMA_NAME = "product_v1"


class CrawlerOrchestrator:
    """
    Full self-healing crawler pipeline.
    Coordinates: page fetch → change detection → cache → extraction → storage.
    """

    def __init__(
        self,
        cache_path: str = "/tmp/crawler_selector_cache.json",
        db_path: str = "/tmp/crawler_data.db",
        headless: bool = True,
        use_llm: bool = True,
    ):
        self.change_detector = ChangeDetector()
        self.llm_extractor = LLMExtractor() if use_llm else None
        self.fast_extractor = FastExtractor()
        self.cache = SelectorCache(cache_path)
        self.db = CrawlerDB(db_path)
        self.headless = headless
        self.use_llm = use_llm
        self._browser = None
        self._playwright = None

    async def _start_browser(self):
        from playwright.async_api import async_playwright
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-http2",
            ]
        )
        logger.info("Browser started")

    async def _stop_browser(self):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def _fetch_page(self, url: str) -> tuple[str, Optional[bytes]]:
        """
        Fetch page HTML and screenshot using Playwright.
        Returns (html, screenshot_bytes).
        """
        if not self._browser:
            await self._start_browser()

        context = await self._browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )

        page = await context.new_page()

        try:
            if url.startswith("file://") or url.startswith("/"):
                # Local file
                if not url.startswith("file://"):
                    url = f"file://{url}"
                await page.goto(url, wait_until="domcontentloaded")
            else:
                try:
                    await page.goto(url, wait_until="load", timeout=30000)
                except Exception:
                    # Some sites never reach "load" — grab whatever rendered so far
                    pass

            # Wait for JS to render content
            await page.wait_for_timeout(3000)

            html = await page.content()
            screenshot = await page.screenshot(full_page=False, type="png")

            return html, screenshot

        except Exception as e:
            logger.error(f"Page fetch failed for {url}: {e}")
            try:
                html = await page.content() if page else ""
            except Exception:
                html = ""
            return html, None
        finally:
            await context.close()

    def _load_local_html(self, path: str) -> tuple[str, None]:
        """Load HTML directly from file (no browser needed for mock)."""
        with open(path) as f:
            return f.read(), None

    async def crawl(self, url: str, local_html_path: Optional[str] = None) -> ExtractionResult:
        """
        Main entry point: crawl a URL and return structured product data.

        Args:
            url: Target URL (used as cache key even for local files)
            local_html_path: If set, load HTML from local file instead of fetching
        """
        logger.info(f"Starting crawl: {url}")

        # ─── STEP 1: Fetch page ───────────────────────────────────────────
        if local_html_path:
            html, screenshot = self._load_local_html(local_html_path)
        else:
            html, screenshot = await self._fetch_page(url)

        if not html:
            return ExtractionResult(
                url=url,
                data=ProductSchema(),
                method="error",
                error="Failed to fetch page",
            )

        # ─── STEP 2: Change detection ─────────────────────────────────────
        cache_entry = self.cache.get(url, SCHEMA_NAME)
        baseline = cache_entry.to_baseline() if cache_entry else None

        drift_report = self.change_detector.detect(html, screenshot, baseline)

        if drift_report.has_drift:
            logger.warning(
                f"Drift detected [{drift_report.drift_severity}]: "
                f"structural={drift_report.structural_similarity:.2f}, "
                f"visual={drift_report.visual_similarity:.2f}"
            )
            # Invalidate stale selectors
            self.cache.invalidate(url, SCHEMA_NAME, reason=f"drift_{drift_report.drift_severity}")
            cache_entry = None

        # ─── STEP 3: Cache lookup ─────────────────────────────────────────
        if cache_entry and not drift_report.has_drift:
            logger.info("Cache HIT — using fast CSS extraction")
            raw_data = self.fast_extractor.extract(html, cache_entry.selectors)
            method = "cache_hit"
            tokens_used = 0
            selectors_used = cache_entry.selectors

            # Record hit/miss per field for canary validation
            null_count = sum(1 for v in raw_data.values() if v is None)
            if null_count == 0:
                self.cache.record_hit(url, SCHEMA_NAME)
            else:
                self.cache.record_miss(url, SCHEMA_NAME)

        elif self.use_llm:
            # ─── STEP 4a: LLM extraction ──────────────────────────────────
            logger.info("Cache MISS — running LLM semantic extraction")
            raw_data, selectors_dict, tokens_used = self.llm_extractor.extract(html, url)
            method = "llm_extraction"
            selectors_used = selectors_dict

            if selectors_dict:
                # Store selectors in cache for future fast extraction
                self.cache.set(
                    url=url,
                    schema_name=SCHEMA_NAME,
                    selectors=selectors_dict,
                    structural_hash=drift_report.structural_hash,
                    visual_hash=drift_report.visual_hash,
                    llm_tokens_spent=tokens_used,
                )
                logger.info(f"Selectors cached for future use (tokens_spent={tokens_used})")
        else:
            # ─── STEP 4b: Fallback (no LLM configured) ────────────────────
            logger.warning("No cache and LLM disabled — returning empty result")
            raw_data = {}
            selectors_used = {}
            tokens_used = 0
            method = "llm_fallback"

        # ─── STEP 5: Schema validation + parsing ─────────────────────────
        product = ProductSchema(**raw_data) if raw_data else ProductSchema()
        confidence = product.confidence_score()
        null_rate = product.null_rate()

        # ─── STEP 6: Persist ──────────────────────────────────────────────
        self.db.save_extraction(
            url=url,
            data=product.model_dump(),
            method=method,
            cache_hit=(method == "cache_hit"),
            drift_detected=drift_report.has_drift,
            confidence=confidence,
        )

        self.db.log_audit(
            url=url,
            method=method,
            cache_hit=(method == "cache_hit"),
            drift_detected=drift_report.has_drift,
            drift_severity=drift_report.drift_severity,
            structural_hash=drift_report.structural_hash,
            visual_hash=drift_report.visual_hash or "",
            null_rate=null_rate,
            confidence=confidence,
            llm_tokens_used=tokens_used,
            selectors_used=selectors_used,
        )

        result = ExtractionResult(
            url=url,
            data=product,
            method=method,
            selectors_used=SelectorMap(**{k:v for k,v in (selectors_used or {}).items() if k in SelectorMap.__dataclass_fields__}) if selectors_used else None,
            llm_tokens_used=tokens_used,
            cache_hit=(method == "cache_hit"),
            drift_detected=drift_report.has_drift,
            confidence=confidence,
            raw_html_hash=drift_report.structural_hash[:12],
        )

        logger.info(
            f"Crawl complete: method={method}, confidence={confidence:.2f}, "
            f"null_rate={null_rate:.1%}, tokens={tokens_used}"
        )

        return result

    async def close(self):
        await self._stop_browser()

    def get_stats(self) -> dict:
        return {
            "cache": self.cache.stats(),
            "db": self.db.summary_stats(),
        }
