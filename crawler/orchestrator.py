"""
orchestrator.py — Full pipeline coordinator.

Decision flow:
  fetch → drift-detection → cache-lookup → extraction → validation → storage

The schema name is read from settings (CRAWLER_SCHEMA_NAME env var) and can
be overridden per-instance, making the orchestrator reusable across multiple
site schemas without code changes.
"""

from __future__ import annotations


import logging
from typing import Optional

from crawler.config import settings
from crawler.schema import ProductSchema, SelectorMap, ExtractionResult
from crawler.change_detector import ChangeDetector
from crawler.llm_extractor import LLMExtractor
from crawler.fast_extractor import FastExtractor
from crawler.selector_cache import SelectorCache
from crawler.db import CrawlerDB
from crawler.exceptions import CrawlerFetchError, CrawlerExtractionError, CrawlerLLMError

logger = logging.getLogger(__name__)

_PRODUCT_SCHEMA_FIELDS = frozenset(ProductSchema.model_fields.keys())


class CrawlerOrchestrator:
    """
    Self-healing crawler pipeline.
    Coordinates: page fetch → change detection → cache → extraction → storage.

    Usage:
        orchestrator = CrawlerOrchestrator()
        result = await orchestrator.crawl("https://example.com/product/123")
        await orchestrator.close()

    Or use as an async context manager:
        async with CrawlerOrchestrator() as crawler:
            result = await crawler.crawl(url)
    """

    def __init__(
        self,
        cache_path: Optional[str] = None,
        db_path: Optional[str] = None,
        schema_name: Optional[str] = None,
        headless: Optional[bool] = None,
        use_llm: bool = True,
    ) -> None:
        self.schema_name = schema_name or settings.schema_name
        self._headless = headless if headless is not None else settings.browser_headless

        self.change_detector = ChangeDetector()
        self.llm_extractor = LLMExtractor() if use_llm else None
        self.fast_extractor = FastExtractor()
        self.cache = SelectorCache(cache_path)
        self.db = CrawlerDB(db_path)
        self._use_llm = use_llm

        self._browser = None
        self._playwright = None

    # ── Browser lifecycle ─────────────────────────────────────────────────────

    async def _start_browser(self) -> None:
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self._headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-http2",
            ],
        )
        logger.info("Browser started (headless=%s)", self._headless)

    async def _stop_browser(self) -> None:
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    # ── Page fetching ─────────────────────────────────────────────────────────

    async def _fetch_page(self, url: str) -> tuple[str, Optional[bytes]]:
        """Fetch HTML + screenshot via Playwright. Raises CrawlerFetchError on failure."""
        if not self._browser:
            await self._start_browser()

        context = await self._browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()
        try:
            if url.startswith("file://") or url.startswith("/"):
                nav_url = url if url.startswith("file://") else f"file://{url}"
                await page.goto(nav_url, wait_until="domcontentloaded")
            else:
                try:
                    await page.goto(
                        url,
                        wait_until="load",
                        timeout=settings.page_timeout_ms,
                    )
                except Exception:
                    # Some sites never reach "load" — use whatever has rendered
                    pass

            await page.wait_for_timeout(settings.page_wait_ms)
            html = await page.content()
            screenshot = await page.screenshot(full_page=False, type="png")
            return html, screenshot

        except Exception as exc:
            logger.error("Page fetch failed for %s: %s", url, exc)
            try:
                html = await page.content()
            except Exception:
                html = ""
            if not html:
                raise CrawlerFetchError(f"Failed to fetch {url}: {exc}") from exc
            return html, None
        finally:
            await context.close()

    def _load_local_html(self, path: str) -> tuple[str, None]:
        """Load HTML from a local file (no browser required)."""
        with open(path, encoding="utf-8") as f:
            return f.read(), None

    # ── Main pipeline ─────────────────────────────────────────────────────────

    async def crawl(
        self, url: str, local_html_path: Optional[str] = None
    ) -> ExtractionResult:
        """
        Crawl a URL and return structured product data.

        Args:
            url:             Target URL (used as cache key even for local files).
            local_html_path: If set, load HTML from file instead of using Playwright.
        """
        logger.info("Crawl start: %s", url)
        error_message: Optional[str] = None

        # ── 1. Fetch ──────────────────────────────────────────────────────────
        try:
            if local_html_path:
                html, screenshot = self._load_local_html(local_html_path)
            else:
                html, screenshot = await self._fetch_page(url)
        except CrawlerFetchError as exc:
            logger.error("Fetch failed: %s", exc)
            return ExtractionResult(
                url=url,
                data=ProductSchema(),
                method="error",
                error=str(exc),
            )

        if not html:
            return ExtractionResult(
                url=url,
                data=ProductSchema(),
                method="error",
                error="Empty HTML response",
            )

        # ── 2. Drift detection ────────────────────────────────────────────────
        cache_entry = self.cache.get(url, self.schema_name)
        baseline = cache_entry.to_baseline() if cache_entry else None
        drift_report = self.change_detector.detect(html, screenshot, baseline)

        if drift_report.has_drift:
            logger.warning(
                "Drift [%s]: structural=%.2f visual=%.2f",
                drift_report.drift_severity,
                drift_report.structural_similarity,
                drift_report.visual_similarity,
            )
            self.cache.invalidate(
                url, self.schema_name, reason=f"drift_{drift_report.drift_severity}"
            )
            cache_entry = None

        # ── 3. Extract ────────────────────────────────────────────────────────
        raw_data: dict = {}
        selectors_used: dict = {}
        tokens_used: int = 0
        method: str

        if cache_entry and not drift_report.has_drift:
            logger.info("Cache HIT — fast CSS extraction")
            raw_data = self.fast_extractor.extract(html, cache_entry.selectors)
            method = "cache_hit"
            selectors_used = cache_entry.selectors
            null_count = sum(1 for v in raw_data.values() if v is None)
            if null_count == 0:
                self.cache.record_hit(url, self.schema_name)
            else:
                self.cache.record_miss(url, self.schema_name)

        elif self._use_llm and self.llm_extractor:
            logger.info("Cache MISS — LLM semantic extraction")
            try:
                raw_data, selectors_dict, tokens_used = await self.llm_extractor.extract(
                    html, url
                )
                method = "llm_extraction"
                selectors_used = selectors_dict

                if selectors_dict:
                    self.cache.set(
                        url=url,
                        schema_name=self.schema_name,
                        selectors=selectors_dict,
                        structural_hash=drift_report.structural_hash,
                        structural_shingles=drift_report._current_shingles,
                        visual_hash=drift_report.visual_hash,
                        llm_tokens_spent=tokens_used,
                    )
                    logger.info("Selectors cached (tokens=%d)", tokens_used)

            except (CrawlerLLMError, CrawlerExtractionError) as exc:
                logger.error("LLM extraction failed: %s", exc)
                error_message = str(exc)
                method = "error"

        else:
            logger.warning("No cache and LLM disabled — returning empty result")
            method = "no_extractor"

        # ── 4. Validate ───────────────────────────────────────────────────────
        # Filter raw_data to only known ProductSchema fields
        clean_data = {k: v for k, v in raw_data.items() if k in _PRODUCT_SCHEMA_FIELDS}
        product = ProductSchema(**clean_data)
        confidence = product.confidence_score()
        null_rate = product.null_rate()

        # ── 5. Persist ────────────────────────────────────────────────────────
        is_cache_hit = method == "cache_hit"
        await self.db.save_extraction(
            url=url,
            data=product.model_dump(),
            method=method,
            cache_hit=is_cache_hit,
            drift_detected=drift_report.has_drift,
            confidence=confidence,
        )
        await self.db.log_audit(
            url=url,
            method=method,
            cache_hit=is_cache_hit,
            drift_detected=drift_report.has_drift,
            drift_severity=drift_report.drift_severity,
            structural_hash=drift_report.structural_hash,
            visual_hash=drift_report.visual_hash,
            null_rate=null_rate,
            confidence=confidence,
            llm_tokens_used=tokens_used,
            selectors_used=selectors_used or None,
            error=error_message,
        )

        logger.info(
            "Crawl done: method=%s confidence=%.2f null_rate=%.1f%% tokens=%d",
            method, confidence, null_rate * 100, tokens_used,
        )

        selector_map: Optional[SelectorMap] = None
        if selectors_used:
            known = {k: v for k, v in selectors_used.items() if k in _PRODUCT_SCHEMA_FIELDS}
            selector_map = SelectorMap(**known)

        return ExtractionResult(
            url=url,
            data=product,
            method=method,
            selectors_used=selector_map,
            llm_tokens_used=tokens_used,
            cache_hit=is_cache_hit,
            drift_detected=drift_report.has_drift,
            confidence=confidence,
            raw_html_hash=drift_report.structural_hash[:12],
            error=error_message,
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def close(self) -> None:
        """Flush buffered cache counters and shut down the browser."""
        self.cache.flush()
        await self._stop_browser()

    async def get_stats(self) -> dict:
        return {
            "cache": self.cache.stats(),
            "db": await self.db.summary_stats(),
        }

    # ── Async context manager ─────────────────────────────────────────────────

    async def __aenter__(self) -> "CrawlerOrchestrator":
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()
