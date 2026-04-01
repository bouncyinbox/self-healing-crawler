"""
fast_extractor.py — Cheap, fast CSS-selector-based extraction.

This is the hot path: used whenever we have valid cached selectors.
Cost: near-zero (pure DOM parsing, no LLM calls).
Latency: ~1-5ms per page.

When selectors return null, the null rate is recorded in the cache.
If null rate crosses threshold, cache is invalidated → triggers LLM path.
"""

import logging
from typing import Optional
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class FastExtractor:
    """
    CSS selector-based extractor. Uses BeautifulSoup for reliability.
    In production at scale: use lxml directly for 10x speed improvement.
    """

    def __init__(self):
        pass

    def extract(self, html: str, selectors: dict) -> dict:
        """
        Apply CSS selectors to HTML and return extracted field values.

        Args:
            html: Raw page HTML
            selectors: dict of field_name → CSS selector

        Returns:
            dict of field_name → extracted text (or None if not found)
        """
        soup = BeautifulSoup(html, "lxml")
        results = {}

        for field, selector in selectors.items():
            if not selector:
                results[field] = None
                continue

            try:
                element = soup.select_one(selector)
                if element is None:
                    logger.debug(f"Selector miss: field={field}, selector='{selector}'")
                    results[field] = None
                else:
                    # Extract text, strip whitespace
                    text = element.get_text(separator=" ", strip=True)
                    results[field] = text if text else None
                    logger.debug(f"Selector hit: field={field} → '{text[:50]}'")

            except Exception as e:
                logger.warning(f"Selector error: field={field}, selector='{selector}': {e}")
                results[field] = None

        null_fields = [f for f, v in results.items() if v is None]
        if null_fields:
            logger.debug(f"Null fields after fast extraction: {null_fields}")

        return results

    def validate_selectors(self, html: str, selectors: dict) -> dict:
        """
        Test selectors against HTML and return per-field validity.
        Used for canary validation without recording to cache.

        Returns:
            dict of field_name → {"valid": bool, "value": Optional[str]}
        """
        results = self.extract(html, selectors)
        return {
            field: {"valid": value is not None, "value": value}
            for field, value in results.items()
        }

    def null_rate(self, html: str, selectors: dict) -> float:
        """Quick null rate check — what fraction of selectors return no result."""
        results = self.extract(html, selectors)
        if not results:
            return 1.0
        nulls = sum(1 for v in results.values() if v is None)
        return nulls / len(results)
