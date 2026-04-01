"""
fast_extractor.py — CSS-selector-based extraction (the hot path).

Used whenever valid cached selectors exist. Near-zero cost: pure DOM parsing.
Uses lxml directly (via BeautifulSoup's lxml parser) for maximum speed.
At very high scale (>10k pages/sec), use lxml.etree with cssselect directly
to skip the BeautifulSoup abstraction layer entirely.
"""

from __future__ import annotations


import logging
from typing import Optional

from bs4 import BeautifulSoup

from crawler.exceptions import CrawlerExtractionError

logger = logging.getLogger(__name__)


class FastExtractor:
    """
    Apply a dict of CSS selectors to HTML and return extracted field values.
    Selector errors are isolated per-field — one bad selector won't block others.
    """

    def extract(self, html: str, selectors: dict[str, Optional[str]]) -> dict[str, Optional[str]]:
        """
        Args:
            html:      Raw page HTML.
            selectors: Mapping of field_name → CSS selector (None skips the field).

        Returns:
            Mapping of field_name → extracted text (None if selector missed).
        """
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as exc:
            raise CrawlerExtractionError(f"HTML parse failed: {exc}") from exc

        results: dict[str, Optional[str]] = {}
        for field, selector in selectors.items():
            if not selector:
                results[field] = None
                continue
            try:
                element = soup.select_one(selector)
                if element is None:
                    logger.debug("Selector miss: field=%s selector=%r", field, selector)
                    results[field] = None
                else:
                    text = element.get_text(separator=" ", strip=True)
                    results[field] = text or None
                    logger.debug("Selector hit: field=%s value=%r", field, (text or "")[:60])
            except Exception as exc:
                logger.warning(
                    "Selector error: field=%s selector=%r error=%s", field, selector, exc
                )
                results[field] = None

        null_fields = [f for f, v in results.items() if v is None]
        if null_fields:
            logger.debug("Null fields after fast extraction: %s", null_fields)

        return results

    def validate_selectors(
        self, html: str, selectors: dict[str, Optional[str]]
    ) -> dict[str, dict]:
        """
        Test selectors without recording to cache.
        Returns per-field validity report.
        """
        results = self.extract(html, selectors)
        return {
            field: {"valid": value is not None, "value": value}
            for field, value in results.items()
        }

    def null_rate(self, html: str, selectors: dict[str, Optional[str]]) -> float:
        """Fraction of selectors that return no result for this HTML."""
        results = self.extract(html, selectors)
        if not results:
            return 1.0
        return sum(1 for v in results.values() if v is None) / len(results)
