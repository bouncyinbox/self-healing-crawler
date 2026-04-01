"""
Integration tests for CrawlerOrchestrator — the 4-step self-healing demo
verified with mocked LLM and real CSS extraction, cache, and DB.
"""

import json
from unittest.mock import AsyncMock, patch

import pytest

from crawler.orchestrator import CrawlerOrchestrator
from crawler.schema import ProductSchema


MOCK_EXTRACTED = {
    "title": "TechGear Pro X500 Wireless Headphones",
    "price": "$299.99",
    "currency": "$",
    "in_stock": "In Stock",
    "rating": "4.7",
    "review_count": "2,847",
    "sku": "SKU: TG-X500-BLK",
    "brand": "TechGear",
    "description": "Premium wireless headphones with noise cancellation.",
}

SELECTORS_V1 = {
    "title": "#product-title",
    "price": "#product-price",
    "currency": None,
    "in_stock": "#stock-status",
    "rating": "#product-rating",
    "review_count": "#review-count",
    "sku": "#product-sku",
    "brand": "#product-brand",
    "description": "#product-description",
}

SELECTORS_V2 = {
    "title": ".item-headline",
    "price": ".amount-display",
    "currency": None,
    "in_stock": ".availability-badge",
    "rating": ".score-value",
    "review_count": ".review-tally",
    "sku": ".product-ref",
    "brand": ".manufacturer-tag",
    "description": ".item-summary",
}

URL = "https://mockshop.com/product/techgear-pro-x500"


@pytest.fixture
def orchestrator(tmp_cache_path, tmp_db_path):
    return CrawlerOrchestrator(
        cache_path=tmp_cache_path,
        db_path=tmp_db_path,
        use_llm=True,
    )


async def _mock_llm_extract(html, url):
    """Mock LLM that returns v1 selectors."""
    return MOCK_EXTRACTED.copy(), SELECTORS_V1.copy(), 600


async def _mock_llm_extract_v2(html, url):
    """Mock LLM that returns v2 selectors after redesign."""
    return MOCK_EXTRACTED.copy(), SELECTORS_V2.copy(), 650


class TestFourStepDemoPipeline:
    """
    Simulate the canonical 4-run scenario:
    1. First visit → LLM extraction, selectors cached
    2. Second visit → Cache hit, fast CSS extraction (no LLM)
    3. Redesigned page → Drift detected, cache invalidated, LLM re-extracts
    4. Revisit redesigned page → Cache hit with new selectors
    """

    @pytest.mark.asyncio
    async def test_run1_llm_extraction_and_cache_set(self, orchestrator, html_v1):
        with patch.object(orchestrator.llm_extractor, "extract", new=AsyncMock(side_effect=_mock_llm_extract)):
            result = await orchestrator.crawl(url=URL, local_html_path=None)
            # Inject html directly since we aren't using real Playwright
            result = await _crawl_with_html(orchestrator, URL, html_v1, _mock_llm_extract)

        assert result.method == "llm_extraction"
        assert result.cache_hit is False
        assert result.data.title is not None
        # Selectors should now be in cache
        cached = orchestrator.cache.get(URL, orchestrator.schema_name)
        assert cached is not None

    @pytest.mark.asyncio
    async def test_run2_cache_hit(self, orchestrator, html_v1):
        # First visit to populate cache
        await _crawl_with_html(orchestrator, URL, html_v1, _mock_llm_extract)

        # Second visit — LLM should NOT be called
        llm_mock = AsyncMock(side_effect=_mock_llm_extract)
        with patch.object(orchestrator.llm_extractor, "extract", new=llm_mock):
            result = await _crawl_with_html(orchestrator, URL, html_v1, None)

        assert result.method == "cache_hit"
        assert result.cache_hit is True
        assert result.llm_tokens_used == 0
        llm_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_run3_drift_triggers_llm(self, orchestrator, html_v1, html_v2):
        # Populate cache with v1 selectors
        await _crawl_with_html(orchestrator, URL, html_v1, _mock_llm_extract)

        # Serve v2 HTML — structural drift should be detected
        result = await _crawl_with_html(orchestrator, URL, html_v2, _mock_llm_extract_v2)

        assert result.drift_detected is True
        assert result.method == "llm_extraction"
        assert result.cache_hit is False

    @pytest.mark.asyncio
    async def test_run4_cache_hit_with_new_selectors(self, orchestrator, html_v1, html_v2):
        # Populate cache with v1
        await _crawl_with_html(orchestrator, URL, html_v1, _mock_llm_extract)
        # Serve v2 → drift → re-extraction with v2 selectors
        await _crawl_with_html(orchestrator, URL, html_v2, _mock_llm_extract_v2)
        # Serve v2 again → should cache-hit with new selectors
        llm_mock = AsyncMock(side_effect=_mock_llm_extract_v2)
        result = await _crawl_with_html(orchestrator, URL, html_v2, None, llm_mock=llm_mock)

        assert result.method == "cache_hit"
        assert result.cache_hit is True
        llm_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_extracted_data_validated(self, orchestrator, html_v1):
        result = await _crawl_with_html(orchestrator, URL, html_v1, _mock_llm_extract)
        d = result.data
        assert isinstance(d, ProductSchema)
        assert d.price == 299.99        # parsed from "$299.99"
        assert d.currency == "USD"      # mapped from "$"
        assert d.in_stock is True       # parsed from "In Stock"
        assert d.rating == 4.7
        assert d.review_count == 2847

    @pytest.mark.asyncio
    async def test_db_records_written(self, orchestrator, html_v1):
        await _crawl_with_html(orchestrator, URL, html_v1, _mock_llm_extract)
        stats = await orchestrator.db.summary_stats()
        assert stats["total_extractions"] == 1
        assert stats["llm_runs"] == 1

    @pytest.mark.asyncio
    async def test_context_manager(self, tmp_cache_path, tmp_db_path, html_v1):
        async with CrawlerOrchestrator(
            cache_path=tmp_cache_path,
            db_path=tmp_db_path,
            use_llm=True,
        ) as crawler:
            result = await _crawl_with_html(crawler, URL, html_v1, _mock_llm_extract)
        assert result.data.title is not None


# ── Helper ────────────────────────────────────────────────────────────────────

async def _crawl_with_html(orchestrator, url, html, llm_fn, llm_mock=None):
    """
    Run orchestrator.crawl() with a pre-loaded HTML string (no browser),
    patching LLM extraction as needed.
    """
    # Patch _load_local_html to return our html string
    original_load = orchestrator._load_local_html
    orchestrator._load_local_html = lambda path: (html, None)

    if llm_fn is not None:
        with patch.object(orchestrator.llm_extractor, "extract", new=AsyncMock(side_effect=llm_fn)):
            result = await orchestrator.crawl(url=url, local_html_path="/fake/path.html")
    elif llm_mock is not None:
        with patch.object(orchestrator.llm_extractor, "extract", new=llm_mock):
            result = await orchestrator.crawl(url=url, local_html_path="/fake/path.html")
    else:
        result = await orchestrator.crawl(url=url, local_html_path="/fake/path.html")

    orchestrator._load_local_html = original_load
    return result
