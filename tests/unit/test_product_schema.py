"""
Unit tests for ProductSchema field validators.
All parsers must be lenient (return None, not raise) on bad input.
"""

import pytest
from crawler.schema import ProductSchema, SelectorMap


class TestPriceParser:
    def test_numeric_float(self):
        p = ProductSchema(price=29.99)
        assert p.price == 29.99

    def test_string_with_symbol(self):
        p = ProductSchema(price="$299.99")
        assert p.price == 299.99

    def test_comma_separated_thousands(self):
        p = ProductSchema(price="$1,299.99")
        assert p.price == 1299.99

    def test_none_returns_none(self):
        p = ProductSchema(price=None)
        assert p.price is None

    def test_non_numeric_string_returns_none(self):
        p = ProductSchema(price="free")
        assert p.price is None

    def test_integer_input(self):
        p = ProductSchema(price=100)
        assert p.price == 100.0


class TestCurrencyParser:
    def test_dollar_symbol(self):
        p = ProductSchema(currency="$")
        assert p.currency == "USD"

    def test_euro_symbol(self):
        p = ProductSchema(currency="€")
        assert p.currency == "EUR"

    def test_rupee_symbol(self):
        p = ProductSchema(currency="₹")
        assert p.currency == "INR"

    def test_iso_code_passthrough(self):
        p = ProductSchema(currency="GBP")
        assert p.currency == "GBP"

    def test_none_returns_none(self):
        p = ProductSchema(currency=None)
        assert p.currency is None


class TestRatingParser:
    def test_valid_rating(self):
        p = ProductSchema(rating="4.7")
        assert p.rating == 4.7

    def test_rating_with_text(self):
        p = ProductSchema(rating="4.5 out of 5")
        assert p.rating == 4.5

    def test_out_of_range_returns_none(self):
        p = ProductSchema(rating="6.0")
        assert p.rating is None

    def test_negative_returns_none(self):
        p = ProductSchema(rating="-1")
        assert p.rating is None

    def test_none_returns_none(self):
        p = ProductSchema(rating=None)
        assert p.rating is None


class TestReviewCountParser:
    def test_plain_number(self):
        p = ProductSchema(review_count="1234")
        assert p.review_count == 1234

    def test_comma_formatted(self):
        p = ProductSchema(review_count="2,847")
        assert p.review_count == 2847

    def test_parentheses_format(self):
        p = ProductSchema(review_count="(2,847 reviews)")
        assert p.review_count == 2847

    def test_none_returns_none(self):
        p = ProductSchema(review_count=None)
        assert p.review_count is None


class TestInStockParser:
    @pytest.mark.parametrize("text", ["In Stock", "in stock", "Available", "Add to Cart", "Buy Now"])
    def test_in_stock_variants(self, text):
        p = ProductSchema(in_stock=text)
        assert p.in_stock is True

    @pytest.mark.parametrize("text", ["Out of Stock", "Sold Out", "Unavailable"])
    def test_out_of_stock_variants(self, text):
        p = ProductSchema(in_stock=text)
        assert p.in_stock is False

    def test_bool_true(self):
        p = ProductSchema(in_stock=True)
        assert p.in_stock is True

    def test_unknown_text_returns_none(self):
        p = ProductSchema(in_stock="Limited quantities")
        assert p.in_stock is None

    def test_none_returns_none(self):
        p = ProductSchema(in_stock=None)
        assert p.in_stock is None


class TestFieldTruncation:
    def test_title_truncated_at_500(self):
        p = ProductSchema(title="x" * 600)
        assert len(p.title) == 500

    def test_description_truncated_at_1000(self):
        p = ProductSchema(description="x" * 1200)
        assert len(p.description) == 1000

    def test_brand_truncated_at_200(self):
        p = ProductSchema(brand="x" * 300)
        assert len(p.brand) == 200

    def test_sku_truncated_at_100(self):
        p = ProductSchema(sku="x" * 150)
        assert len(p.sku) == 100


class TestMetrics:
    def test_null_rate_all_none(self):
        p = ProductSchema()
        assert p.null_rate() == 1.0

    def test_null_rate_all_filled(self):
        p = ProductSchema(
            title="T", price=10.0, currency="USD", in_stock=True,
            rating=4.5, review_count=100, sku="SKU", brand="B",
            description="D",
        )
        assert p.null_rate() == 0.0

    def test_confidence_all_critical_missing(self):
        p = ProductSchema()
        assert p.confidence_score() == 0.0

    def test_confidence_all_critical_present(self):
        p = ProductSchema(title="T", price=10.0, in_stock=True)
        assert p.confidence_score() == 1.0

    def test_confidence_partial(self):
        p = ProductSchema(title="T", price=None, in_stock=None)
        assert abs(p.confidence_score() - 1 / 3) < 0.01


class TestSelectorMap:
    def test_coverage_all_present(self, selectors_v1):
        sm = SelectorMap(**selectors_v1)
        # currency is None in the fixture
        assert sm.coverage() < 1.0

    def test_coverage_all_none(self):
        sm = SelectorMap()
        assert sm.coverage() == 0.0
