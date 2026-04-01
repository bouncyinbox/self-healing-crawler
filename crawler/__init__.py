"""
crawler — Self-healing web crawler package.

Core pipeline:
  fetch → drift-detection → cache-lookup → extraction → validation → storage
"""

from __future__ import annotations


from crawler.config import settings
from crawler.exceptions import (
    CrawlerError,
    CrawlerFetchError,
    CrawlerExtractionError,
    CrawlerLLMError,
    CrawlerCacheError,
    CrawlerSchemaError,
)

__all__ = [
    "settings",
    "CrawlerError",
    "CrawlerFetchError",
    "CrawlerExtractionError",
    "CrawlerLLMError",
    "CrawlerCacheError",
    "CrawlerSchemaError",
]
