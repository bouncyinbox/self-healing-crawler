"""
Unit tests for FastExtractor — CSS selector-based extraction.
"""

import pytest
from crawler.fast_extractor import FastExtractor


@pytest.fixture
def extractor():
    return FastExtractor()


class TestExtract:
    def test_id_selector_hit(self, extractor, html_v1):
        results = extractor.extract(html_v1, {"title": "#product-title"})
        assert results["title"] == "TechGear Pro X500 Wireless Headphones"

    def test_class_selector_hit(self, extractor, html_v2):
        results = extractor.extract(html_v2, {"title": ".item-headline"})
        assert results["title"] == "TechGear Pro X500 Wireless Headphones"

    def test_selector_miss_returns_none(self, extractor, html_v1):
        results = extractor.extract(html_v1, {"title": ".nonexistent-class"})
        assert results["title"] is None

    def test_none_selector_returns_none(self, extractor, html_v1):
        results = extractor.extract(html_v1, {"currency": None})
        assert results["currency"] is None

    def test_multiple_fields(self, extractor, html_v1, selectors_v1):
        results = extractor.extract(html_v1, selectors_v1)
        assert results["title"] is not None
        assert results["price"] is not None
        assert results["in_stock"] is not None

    def test_bad_selector_does_not_raise(self, extractor, html_v1):
        # Invalid CSS selector should return None, not raise
        results = extractor.extract(html_v1, {"title": "::invalid>>selector"})
        assert results["title"] is None

    def test_v1_selectors_fail_on_v2(self, extractor, html_v2, selectors_v1):
        """Cached v1 selectors should return None on the redesigned v2 page."""
        results = extractor.extract(html_v2, selectors_v1)
        # #product-title doesn't exist in v2
        assert results["title"] is None

    def test_price_whitespace_stripped(self, extractor, html_v1):
        results = extractor.extract(html_v1, {"price": "#product-price"})
        assert results["price"] is not None
        assert results["price"].strip() == results["price"]


class TestNullRate:
    def test_all_hit_null_rate_zero(self, extractor, html_v1, selectors_v1):
        # Remove currency (None selector) so it doesn't skew the test
        sels = {k: v for k, v in selectors_v1.items() if v is not None}
        rate = extractor.null_rate(html_v1, sels)
        assert rate == 0.0

    def test_all_miss_null_rate_one(self, extractor, html_v1):
        rate = extractor.null_rate(html_v1, {"a": ".nope", "b": ".also-nope"})
        assert rate == 1.0

    def test_empty_selectors_null_rate_one(self, extractor, html_v1):
        rate = extractor.null_rate(html_v1, {})
        assert rate == 1.0


class TestValidateSelectors:
    def test_valid_selector(self, extractor, html_v1):
        report = extractor.validate_selectors(html_v1, {"title": "#product-title"})
        assert report["title"]["valid"] is True
        assert report["title"]["value"] is not None

    def test_invalid_selector(self, extractor, html_v1):
        report = extractor.validate_selectors(html_v1, {"title": ".nope"})
        assert report["title"]["valid"] is False
        assert report["title"]["value"] is None
