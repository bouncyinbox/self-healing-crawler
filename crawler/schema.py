"""
schema.py — Typed contracts for extracted product data (Pydantic v2).

ProductSchema:  Validated product fields with coercing parsers.
SelectorMap:    CSS selectors that produce those fields.
ExtractionResult: Full pipeline output with metadata.
"""

from __future__ import annotations


import re
from typing import Optional, Any

from pydantic import BaseModel, Field, field_validator, model_validator


_CURRENCY_MAP = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY", "₹": "INR"}

# Fields that must be present for a result to be considered high-confidence.
_CRITICAL_FIELDS = ("title", "price", "in_stock")


class ProductSchema(BaseModel):
    """
    Validated contract for e-commerce product extraction.
    All fields are optional; missing fields are tracked for null-rate monitoring.
    Parsers are lenient — they return None rather than raising on bad input.
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

    # ── Field-level validators (run before assignment) ────────────────────

    @field_validator("title", mode="before")
    @classmethod
    def _cap_title(cls, v: Any) -> Optional[str]:
        return str(v).strip()[:500] if v is not None else None

    @field_validator("brand", mode="before")
    @classmethod
    def _cap_brand(cls, v: Any) -> Optional[str]:
        return str(v).strip()[:200] if v is not None else None

    @field_validator("sku", mode="before")
    @classmethod
    def _cap_sku(cls, v: Any) -> Optional[str]:
        return str(v).strip()[:100] if v is not None else None

    @field_validator("description", mode="before")
    @classmethod
    def _cap_description(cls, v: Any) -> Optional[str]:
        return str(v).strip()[:1000] if v is not None else None

    @field_validator("price", mode="before")
    @classmethod
    def _parse_price(cls, v: Any) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        cleaned = re.sub(r"[^\d.]", "", str(v))
        try:
            return float(cleaned) if cleaned else None
        except ValueError:
            return None

    @field_validator("currency", mode="before")
    @classmethod
    def _parse_currency(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        v = str(v).strip()
        return _CURRENCY_MAP.get(v, v.upper()[:3]) if v else None

    @field_validator("rating", mode="before")
    @classmethod
    def _parse_rating(cls, v: Any) -> Optional[float]:
        if v is None:
            return None
        # Extract the FIRST floating-point number in the string.
        # "4.5 out of 5" → 4.5, not 4.55 (wrong) or 45 (wrong).
        m = re.search(r"-?\d+(?:\.\d+)?", str(v))
        if not m:
            return None
        try:
            r = float(m.group())
            return r if 0.0 <= r <= 5.0 else None
        except ValueError:
            return None

    @field_validator("review_count", mode="before")
    @classmethod
    def _parse_review_count(cls, v: Any) -> Optional[int]:
        if v is None:
            return None
        cleaned = re.sub(r"[^\d]", "", str(v))
        return int(cleaned) if cleaned else None

    @field_validator("in_stock", mode="before")
    @classmethod
    def _parse_in_stock(cls, v: Any) -> Optional[bool]:
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        v_lower = str(v).lower()
        # Check negative variants FIRST — "unavailable" contains "available".
        if any(x in v_lower for x in ("out of stock", "unavailable", "sold out")):
            return False
        if any(x in v_lower for x in ("in stock", "available", "add to cart", "buy now")):
            return True
        return None

    # ── Computed metrics ──────────────────────────────────────────────────

    def null_rate(self) -> float:
        """Fraction of fields that are None (0.0 = fully populated)."""
        fields = [
            self.title, self.price, self.currency, self.in_stock,
            self.rating, self.review_count, self.sku, self.brand, self.description,
        ]
        return sum(1 for f in fields if f is None) / len(fields)

    def confidence_score(self) -> float:
        """Score based on critical fields being present (title, price, in_stock)."""
        present = sum(
            1 for name in _CRITICAL_FIELDS
            if getattr(self, name) is not None
        )
        return present / len(_CRITICAL_FIELDS)


class SelectorMap(BaseModel):
    """Maps product field names to CSS selectors for fast extraction."""

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
        """Fraction of fields that have a non-None selector."""
        vals = [
            self.title, self.price, self.currency, self.in_stock,
            self.rating, self.review_count, self.sku, self.brand, self.description,
        ]
        return sum(1 for v in vals if v is not None) / len(vals)

    def as_dict(self) -> dict[str, Optional[str]]:
        return self.model_dump()


class ExtractionResult(BaseModel):
    """Full pipeline output: extracted data + execution metadata."""

    url: str
    data: ProductSchema
    method: str                              # 'cache_hit' | 'llm_extraction' | 'error'
    selectors_used: Optional[SelectorMap] = None
    llm_tokens_used: int = 0
    cache_hit: bool = False
    drift_detected: bool = False
    confidence: float = 0.0
    raw_html_hash: str = ""
    error: Optional[str] = None
