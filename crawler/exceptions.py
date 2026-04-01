"""
exceptions.py — Typed exception hierarchy for the crawler.

Callers can catch CrawlerError for broad handling, or specific subclasses
to implement targeted retry/alert logic.
"""

from __future__ import annotations



class CrawlerError(Exception):
    """Base class for all crawler errors."""


class CrawlerFetchError(CrawlerError):
    """Page fetch failed — network timeout, browser crash, or empty response."""


class CrawlerExtractionError(CrawlerError):
    """Data extraction failed — no matching selectors, bad HTML structure."""


class CrawlerLLMError(CrawlerExtractionError):
    """LLM API call failed after all retry attempts."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class CrawlerCacheError(CrawlerError):
    """Cache read/write operation failed."""


class CrawlerSchemaError(CrawlerError):
    """Schema validation or type-parsing failed on extracted data."""
