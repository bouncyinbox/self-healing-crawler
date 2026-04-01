"""
selector_cache.py — Persistent selector cache with TTL, write-behind, and thread safety.

Design decisions for production scale:
- Thread-safe in-memory dict protected by threading.Lock.
- Write-behind: hit/miss counters are buffered in memory; only set() and
  invalidate() write immediately (these are durable events). Call flush()
  at shutdown to persist counters.
- In production: replace the JSON backend with Redis (same public interface).
  Key pattern: "crawler:selectors:{schema}:{url_pattern_hash}"
"""

from __future__ import annotations


import json
import time
import hashlib
import logging
import threading
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from crawler.config import settings
from crawler.exceptions import CrawlerCacheError

logger = logging.getLogger(__name__)


class CacheEntry:
    """Immutable view of a cached selector set with its baseline fingerprints."""

    __slots__ = (
        "url_pattern",
        "schema_name",
        "selectors",
        "structural_hash",
        "structural_shingles",
        "visual_hash",
        "created_at",
        "last_validated_at",
        "hit_count",
        "miss_count",
        "llm_tokens_spent",
        "ttl_seconds",
    )

    def __init__(self, **kwargs: object) -> None:
        for k in self.__slots__:
            setattr(self, k, kwargs.get(k))
        if self.ttl_seconds is None:
            self.ttl_seconds = settings.cache_ttl_seconds

    @property
    def is_expired(self) -> bool:
        return (time.time() - self.created_at) > self.ttl_seconds  # type: ignore[operator]

    @property
    def null_rate(self) -> float:
        total = (self.hit_count or 0) + (self.miss_count or 0)
        return (self.miss_count or 0) / total if total > 0 else 0.0

    def to_baseline(self) -> dict:
        """Return the dict passed to ChangeDetector.detect as the baseline."""
        return {
            "structural_hash": self.structural_hash,
            "structural_shingles": list(self.structural_shingles or []),
            "visual_hash": self.visual_hash,
        }

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__slots__}


class SelectorCache:
    """
    JSON-backed selector cache for development / small-scale deployments.

    The interface is designed to be identical to a Redis-backed implementation;
    swap the backend without changing any calling code.

    Thread safety: all public methods acquire self._lock before mutating state.
    Write-behind: hit/miss counters are not flushed on every call. Call flush()
    at shutdown to persist them.
    """

    def __init__(self, cache_path: Optional[str] = None) -> None:
        self._path = Path(cache_path or settings.cache_path)
        self._lock = threading.Lock()
        self._data: dict[str, dict] = self._load()
        self._dirty: bool = False  # True when in-memory counters differ from disk

    # ── I/O ──────────────────────────────────────────────────────────────────

    def _load(self) -> dict[str, dict]:
        if self._path.exists():
            try:
                with open(self._path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Cache file unreadable (%s), starting empty", exc)
        return {}

    def _save(self) -> None:
        """Write in-memory state to disk. Caller must hold self._lock."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._path, "w") as f:
                json.dump(self._data, f, indent=2)
            self._dirty = False
        except OSError as exc:
            raise CrawlerCacheError(f"Failed to write cache to {self._path}: {exc}") from exc

    def flush(self) -> None:
        """Persist buffered hit/miss counters to disk. Call at shutdown."""
        with self._lock:
            if self._dirty:
                self._save()

    # ── Key derivation ────────────────────────────────────────────────────────

    @staticmethod
    def _make_key(url: str, schema_name: str) -> str:
        """
        Derive a stable cache key from URL + schema name.
        Strips query params and fragments; uses only scheme + netloc + path.
        """
        parsed = urlparse(url)
        pattern = f"{parsed.netloc}{parsed.path}".rstrip("/")
        pattern_hash = hashlib.md5(pattern.encode()).hexdigest()[:12]
        return f"{schema_name}:{pattern_hash}"

    # ── Public API ────────────────────────────────────────────────────────────

    def get(self, url: str, schema_name: str) -> Optional[CacheEntry]:
        """Return a CacheEntry on hit, None on miss or expiry."""
        key = self._make_key(url, schema_name)
        with self._lock:
            raw = self._data.get(key)
        if not raw:
            logger.debug("Cache MISS key=%s", key)
            return None

        entry = CacheEntry(**raw)
        if entry.is_expired:
            logger.info(
                "Cache EXPIRED key=%s age=%.1fh",
                key,
                (time.time() - entry.created_at) / 3600,
            )
            self.invalidate(url, schema_name, reason="ttl_expired")
            return None

        logger.debug("Cache HIT key=%s hits=%s", key, entry.hit_count)
        return entry

    def set(
        self,
        url: str,
        schema_name: str,
        selectors: dict,
        structural_hash: str,
        structural_shingles: frozenset[str],
        visual_hash: Optional[str],
        llm_tokens_spent: int = 0,
    ) -> CacheEntry:
        """Store selectors and baseline fingerprints. Writes through immediately."""
        key = self._make_key(url, schema_name)
        now = time.time()
        entry = CacheEntry(
            url_pattern=url,
            schema_name=schema_name,
            selectors=selectors,
            structural_hash=structural_hash,
            structural_shingles=list(structural_shingles),
            visual_hash=visual_hash,
            created_at=now,
            last_validated_at=now,
            hit_count=0,
            miss_count=0,
            llm_tokens_spent=llm_tokens_spent,
            ttl_seconds=settings.cache_ttl_seconds,
        )
        with self._lock:
            self._data[key] = entry.to_dict()
            self._save()
        logger.info("Cache SET key=%s selectors=%s", key, list(selectors.keys()))
        return entry

    def invalidate(self, url: str, schema_name: str, reason: str = "unspecified") -> None:
        """Remove a cache entry and flush immediately."""
        key = self._make_key(url, schema_name)
        with self._lock:
            if key in self._data:
                del self._data[key]
                self._save()
                logger.warning("Cache INVALIDATED key=%s reason=%s", key, reason)

    def record_hit(self, url: str, schema_name: str) -> None:
        """Increment hit counter in memory (write-behind)."""
        key = self._make_key(url, schema_name)
        with self._lock:
            if key in self._data:
                self._data[key]["hit_count"] = self._data[key].get("hit_count", 0) + 1
                self._dirty = True

    def record_miss(self, url: str, schema_name: str) -> None:
        """
        Increment miss counter in memory (write-behind).
        Auto-invalidates if null rate exceeds threshold over enough samples.
        """
        key = self._make_key(url, schema_name)
        with self._lock:
            if key not in self._data:
                return
            self._data[key]["miss_count"] = self._data[key].get("miss_count", 0) + 1
            self._dirty = True
            entry = CacheEntry(**self._data[key])

        # Check canary threshold outside lock to avoid re-entrant invalidate
        total = entry.hit_count + entry.miss_count
        if (
            total >= settings.cache_null_rate_min_samples
            and entry.null_rate > settings.cache_null_rate_threshold
        ):
            logger.warning(
                "Canary validation FAILED: null_rate=%.1f%% key=%s — auto-invalidating",
                entry.null_rate * 100,
                key,
            )
            self.invalidate(url, schema_name, reason="high_null_rate")

    def stats(self) -> dict:
        """Summary statistics across all cached entries."""
        with self._lock:
            entries = [CacheEntry(**v) for v in self._data.values()]
        if not entries:
            return {"total_entries": 0}
        return {
            "total_entries": len(entries),
            "total_hits": sum(e.hit_count or 0 for e in entries),
            "total_misses": sum(e.miss_count or 0 for e in entries),
            "total_llm_tokens": sum(e.llm_tokens_spent or 0 for e in entries),
            "expired": sum(1 for e in entries if e.is_expired),
            "avg_null_rate": sum(e.null_rate for e in entries) / len(entries),
        }

    def __del__(self) -> None:
        """Best-effort flush on garbage collection."""
        try:
            self.flush()
        except Exception:
            pass
