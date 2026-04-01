"""
conftest.py — Shared pytest fixtures for all tests.
"""

import tempfile
from pathlib import Path

import pytest


# ── Sample HTML fixtures ──────────────────────────────────────────────────────

SAMPLE_HTML_V1 = """
<html>
<head><title>TechGear Pro X500</title></head>
<body>
  <div class="product-page">
    <span id="product-brand">TechGear</span>
    <h1 id="product-title">TechGear Pro X500 Wireless Headphones</h1>
    <div id="product-rating">4.7</div>
    <span id="review-count">(2,847 reviews)</span>
    <div id="product-price">$299.99</div>
    <span id="stock-status">In Stock</span>
    <span id="product-sku">SKU: TG-X500-BLK</span>
    <p id="product-description">Premium wireless headphones with noise cancellation.</p>
  </div>
</body>
</html>
"""

SAMPLE_HTML_V2 = """
<html>
<head><title>TechGear Pro X500</title></head>
<body>
  <div class="pdp-container">
    <span class="manufacturer-tag">TechGear</span>
    <h1 class="item-headline">TechGear Pro X500 Wireless Headphones</h1>
    <div class="score-section"><span class="score-value">4.7</span></div>
    <span class="review-tally">(2,847 reviews)</span>
    <div class="purchase-card">
      <span class="amount-display">$299.99</span>
      <span class="availability-badge">In Stock</span>
    </div>
    <span class="product-ref">Ref: TG-X500-BLK</span>
    <p class="item-summary">Premium wireless headphones with noise cancellation.</p>
  </div>
</body>
</html>
"""

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

MOCK_LLM_RESPONSE = {
    "extracted": {
        "title": "TechGear Pro X500 Wireless Headphones",
        "price": "$299.99",
        "currency": "$",
        "in_stock": "In Stock",
        "rating": "4.7",
        "review_count": "2,847",
        "sku": "SKU: TG-X500-BLK",
        "brand": "TechGear",
        "description": "Premium wireless headphones with noise cancellation.",
    },
    "selectors": SELECTORS_V1,
    "confidence": 0.95,
    "notes": "All fields found via ID selectors.",
}


@pytest.fixture
def html_v1() -> str:
    return SAMPLE_HTML_V1


@pytest.fixture
def html_v2() -> str:
    return SAMPLE_HTML_V2


@pytest.fixture
def selectors_v1() -> dict:
    return dict(SELECTORS_V1)


@pytest.fixture
def mock_llm_response() -> dict:
    return MOCK_LLM_RESPONSE


@pytest.fixture
def tmp_cache_path(tmp_path: Path) -> str:
    return str(tmp_path / "test_cache.json")


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> str:
    return str(tmp_path / "test_db.sqlite")
