"""
selector_cache.py — Persistent selector cache with TTL and invalidation.

Design:
- Primary store: JSON file (POC). In production: Redis with TTL.
- Key: URL pattern (normalized) + schema name
- Value: SelectorMap + baseline fingerprints + metadata
- Invalidation: explicit (on drift) or TTL-based

In production at scale, this would be:
  Redis HSET with EXPIRE — O(1) reads, atomic updates, cluster-ready.
  Key pattern: "crawler:selectors:{schema}:{url_pattern_hash}"
"""

import json
import time
import hashlib
import logging
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 60 * 60 * 24 * 7  # 7 days default


@dataclass
class CacheEntry:
    url_pattern: str
    schema_name: str
    selectors: dict                  # SelectorMap as dict
    structural_hash: str             # DOM fingerprint at time of caching
    visual_hash: Optional[str]       # Visual fingerprint at time of caching
    created_at: float
    last_validated_at: float
    hit_count: int
    miss_count: int                  # How many times selectors returned null
    llm_tokens_spent: int
    ttl_seconds: int = CACHE_TTL_SECONDS

    @property
    def is_expired(self) -> bool:
        return (time.time() - self.created_at) > self.ttl_seconds

    @property
    def null_rate(self) -> float:
        total = self.hit_count + self.miss_count
        return self.miss_count / total if total > 0 else 0.0

    def to_baseline(self) -> dict:
        return {
            "structural_hash": self.structural_hash,
            "visual_hash": self.visual_hash,
        }


class SelectorCache:
    """
    JSON-backed selector cache for POC.
    Designed so the interface is identical to a Redis-backed implementation —
    swap the backend without changing calling code.
    """

    def __init__(self, cache_path: str = "/tmp/crawler_selector_cache.json"):
        self.cache_path = Path(cache_path)
        self._data: dict = self._load()

    def _load(self) -> dict:
        if self.cache_path.exists():
            with open(self.cache_path) as f:
                return json.load(f)
        return {}

    def _save(self):
        with open(self.cache_path, "w") as f:
            json.dump(self._data, f, indent=2)

    def _make_key(self, url: str, schema_name: str) -> str:
        """Normalize URL to pattern key. Strips query params, normalizes path."""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        # In production: extract URL template (e.g., /product/{id} → /product/*)
        pattern = f"{parsed.netloc}{parsed.path}"
        pattern_hash = hashlib.md5(pattern.encode()).hexdigest()[:12]
        return f"{schema_name}:{pattern_hash}"

    def get(self, url: str, schema_name: str) -> Optional[CacheEntry]:
        """Retrieve cached entry. Returns None on miss or expiry."""
        key = self._make_key(url, schema_name)
        raw = self._data.get(key)
        if not raw:
            logger.debug(f"Cache MISS for key={key}")
            return None

        entry = CacheEntry(**raw)

        if entry.is_expired:
            logger.info(f"Cache EXPIRED for key={key}, age={(time.time()-entry.created_at)/3600:.1f}h")
            self.invalidate(url, schema_name)
            return None

        logger.debug(f"Cache HIT for key={key}, hits={entry.hit_count}")
        return entry

    def set(
        self,
        url: str,
        schema_name: str,
        selectors: dict,
        structural_hash: str,
        visual_hash: Optional[str],
        llm_tokens_spent: int = 0,
    ) -> CacheEntry:
        """Store a new cache entry."""
        key = self._make_key(url, schema_name)
        now = time.time()
        entry = CacheEntry(
            url_pattern=url,
            schema_name=schema_name,
            selectors=selectors,
            structural_hash=structural_hash,
            visual_hash=visual_hash,
            created_at=now,
            last_validated_at=now,
            hit_count=0,
            miss_count=0,
            llm_tokens_spent=llm_tokens_spent,
        )
        self._data[key] = asdict(entry)
        self._save()
        logger.info(f"Cache SET for key={key}, selectors={list(selectors.keys())}")
        return entry

    def invalidate(self, url: str, schema_name: str, reason: str = "unspecified"):
        """Invalidate a cache entry — triggers LLM re-extraction on next crawl."""
        key = self._make_key(url, schema_name)
        if key in self._data:
            del self._data[key]
            self._save()
            logger.warning(f"Cache INVALIDATED key={key}, reason={reason}")

    def record_hit(self, url: str, schema_name: str):
        """Increment hit counter — used for cache statistics."""
        key = self._make_key(url, schema_name)
        if key in self._data:
            self._data[key]["hit_count"] += 1
            self._save()

    def record_miss(self, url: str, schema_name: str):
        """
        Increment miss counter (selector returned null).
        If null rate exceeds threshold → auto-invalidate (canary validation).
        """
        key = self._make_key(url, schema_name)
        if key in self._data:
            self._data[key]["miss_count"] += 1
            entry = CacheEntry(**self._data[key])
            self._save()

            # Auto-invalidate if null rate crosses threshold
            NULL_RATE_THRESHOLD = 0.25  # 25% nulls → something is broken
            MIN_SAMPLES = 5             # Don't invalidate on first few misses
            total = entry.hit_count + entry.miss_count
            if total >= MIN_SAMPLES and entry.null_rate > NULL_RATE_THRESHOLD:
                logger.warning(
                    f"Canary validation FAILED: null_rate={entry.null_rate:.1%} "
                    f"for key={key}. Auto-invalidating."
                )
                self.invalidate(url, schema_name, reason="high_null_rate")

    def stats(self) -> dict:
        """Summary statistics across all cached entries."""
        entries = [CacheEntry(**v) for v in self._data.values()]
        if not entries:
            return {"total_entries": 0}
        return {
            "total_entries": len(entries),
            "total_hits": sum(e.hit_count for e in entries),
            "total_misses": sum(e.miss_count for e in entries),
            "total_llm_tokens": sum(e.llm_tokens_spent for e in entries),
            "expired": sum(1 for e in entries if e.is_expired),
            "avg_null_rate": sum(e.null_rate for e in entries) / len(entries),
        }
