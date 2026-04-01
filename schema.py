"""
schema.py — Typed contract for extracted product data.
Uses dataclasses (stdlib) for zero-dependency operation.
In production: swap to Pydantic v2 for full validation, serialization, and FastAPI integration.
"""

import re
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class ProductSchema:
    """
    Typed contract for e-commerce product extraction.
    All fields are optional — missing fields are tracked for null-rate monitoring.
    """
    title: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    in_stock: Optional[bool] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    sku: Optional[str] = None
    brand: Optional[str] = None
    description: Optional[str] = None

    def __post_init__(self):
        self.price = self._parse_price(self.price)
        self.currency = self._parse_currency(self.currency)
        self.rating = self._parse_rating(self.rating)
        self.review_count = self._parse_int(self.review_count)
        self.in_stock = self._parse_in_stock(self.in_stock)
        if self.title:
            self.title = str(self.title).strip()[:500]
        if self.brand:
            self.brand = str(self.brand).strip()[:200]
        if self.sku:
            self.sku = str(self.sku).strip()[:100]
        if self.description:
            self.description = str(self.description).strip()[:1000]

    @staticmethod
    def _parse_price(v) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        cleaned = re.sub(r"[^\d.]", "", str(v))
        try:
            return float(cleaned) if cleaned else None
        except ValueError:
            return None

    @staticmethod
    def _parse_currency(v) -> Optional[str]:
        if v is None:
            return None
        v = str(v).strip()
        currency_map = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY", "₹": "INR"}
        return currency_map.get(v, v.upper()[:3])

    @staticmethod
    def _parse_rating(v) -> Optional[float]:
        if v is None:
            return None
        cleaned = re.sub(r"[^\d.]", "", str(v))
        try:
            r = float(cleaned)
            return r if 0 <= r <= 5 else None
        except ValueError:
            return None

    @staticmethod
    def _parse_int(v) -> Optional[int]:
        if v is None:
            return None
        cleaned = re.sub(r"[^\d]", "", str(v))
        return int(cleaned) if cleaned else None

    @staticmethod
    def _parse_in_stock(v) -> Optional[bool]:
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        v_lower = str(v).lower()
        if any(x in v_lower for x in ["in stock", "available", "add to cart", "buy now"]):
            return True
        if any(x in v_lower for x in ["out of stock", "unavailable", "sold out"]):
            return False
        return None

    def model_dump(self) -> dict:
        return asdict(self)

    def null_rate(self) -> float:
        vals = [self.title, self.price, self.currency, self.in_stock,
                self.rating, self.review_count, self.sku, self.brand, self.description]
        return sum(1 for v in vals if v is None) / len(vals)

    def confidence_score(self) -> float:
        critical = [self.title, self.price, self.in_stock]
        return sum(1 for v in critical if v is not None) / len(critical)


@dataclass
class SelectorMap:
    title: Optional[str] = None
    price: Optional[str] = None
    currency: Optional[str] = None
    in_stock: Optional[str] = None
    rating: Optional[str] = None
    review_count: Optional[str] = None
    sku: Optional[str] = None
    brand: Optional[str] = None
    description: Optional[str] = None

    def coverage(self) -> float:
        vals = [self.title, self.price, self.currency, self.in_stock,
                self.rating, self.review_count, self.sku, self.brand, self.description]
        return sum(1 for v in vals if v is not None) / len(vals)


@dataclass
class ExtractionResult:
    url: str
    data: ProductSchema
    method: str
    selectors_used: Optional[SelectorMap] = None
    llm_tokens_used: int = 0
    cache_hit: bool = False
    drift_detected: bool = False
    confidence: float = 0.0
    raw_html_hash: str = ""
    error: Optional[str] = None
