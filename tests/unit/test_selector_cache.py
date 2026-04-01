"""
Unit tests for SelectorCache — get/set/invalidate, TTL, write-behind,
thread safety, and canary validation.
"""

import time
import threading
from pathlib import Path

import pytest
from crawler.selector_cache import SelectorCache


SELECTORS = {"title": "#product-title", "price": "#product-price"}
URL = "https://example.com/product/123"
SCHEMA = "product_v1"
SHINGLES = frozenset(["a\nb\nc", "b\nc\nd", "c\nd\ne"])


@pytest.fixture
def cache(tmp_cache_path) -> SelectorCache:
    return SelectorCache(cache_path=tmp_cache_path)


class TestGetSet:
    def test_miss_on_empty_cache(self, cache):
        assert cache.get(URL, SCHEMA) is None

    def test_set_then_get_round_trip(self, cache):
        cache.set(URL, SCHEMA, SELECTORS, "abc123", SHINGLES, None)
        entry = cache.get(URL, SCHEMA)
        assert entry is not None
        assert entry.selectors == SELECTORS

    def test_get_returns_none_after_invalidate(self, cache):
        cache.set(URL, SCHEMA, SELECTORS, "abc123", SHINGLES, None)
        cache.invalidate(URL, SCHEMA)
        assert cache.get(URL, SCHEMA) is None

    def test_different_urls_have_different_keys(self, cache):
        url2 = "https://example.com/product/456"
        cache.set(URL, SCHEMA, SELECTORS, "abc123", SHINGLES, None)
        assert cache.get(url2, SCHEMA) is None

    def test_query_params_stripped_from_key(self, cache):
        url_with_params = URL + "?ref=google&utm=test"
        cache.set(URL, SCHEMA, SELECTORS, "abc123", SHINGLES, None)
        entry = cache.get(url_with_params, SCHEMA)
        assert entry is not None

    def test_baseline_round_trip(self, cache):
        cache.set(URL, SCHEMA, SELECTORS, "deadbeef", SHINGLES, "visual123")
        entry = cache.get(URL, SCHEMA)
        baseline = entry.to_baseline()
        assert baseline["structural_hash"] == "deadbeef"
        assert baseline["visual_hash"] == "visual123"
        assert set(baseline["structural_shingles"]) == SHINGLES


class TestTTL:
    def test_expired_entry_returns_none(self, tmp_cache_path):
        cache = SelectorCache(cache_path=tmp_cache_path)
        cache.set(URL, SCHEMA, SELECTORS, "abc", SHINGLES, None)
        # Manually expire the entry
        key = cache._make_key(URL, SCHEMA)
        cache._data[key]["created_at"] = time.time() - 999_999
        cache._save()

        fresh_cache = SelectorCache(cache_path=tmp_cache_path)
        assert fresh_cache.get(URL, SCHEMA) is None


class TestWriteBehind:
    def test_record_hit_does_not_immediately_flush(self, cache, tmp_cache_path):
        cache.set(URL, SCHEMA, SELECTORS, "abc", SHINGLES, None)
        # Read current disk state
        import json
        with open(tmp_cache_path) as f:
            before = json.load(f)

        cache.record_hit(URL, SCHEMA)

        with open(tmp_cache_path) as f:
            after = json.load(f)

        # Disk state unchanged because record_hit is write-behind
        key = list(before.keys())[0]
        assert before[key]["hit_count"] == after[key]["hit_count"]

    def test_flush_persists_counters(self, cache, tmp_cache_path):
        import json
        cache.set(URL, SCHEMA, SELECTORS, "abc", SHINGLES, None)
        cache.record_hit(URL, SCHEMA)
        cache.record_hit(URL, SCHEMA)
        cache.flush()

        with open(tmp_cache_path) as f:
            data = json.load(f)
        key = list(data.keys())[0]
        assert data[key]["hit_count"] == 2

    def test_invalidate_writes_through(self, cache, tmp_cache_path):
        import json
        cache.set(URL, SCHEMA, SELECTORS, "abc", SHINGLES, None)
        cache.invalidate(URL, SCHEMA)
        with open(tmp_cache_path) as f:
            data = json.load(f)
        assert len(data) == 0


class TestCanaryValidation:
    def test_auto_invalidate_on_high_null_rate(self, tmp_cache_path):
        from crawler.config import settings
        cache = SelectorCache(cache_path=tmp_cache_path)
        cache.set(URL, SCHEMA, SELECTORS, "abc", SHINGLES, None)

        min_samples = settings.cache_null_rate_min_samples
        # Record enough misses to exceed threshold
        for _ in range(min_samples + 1):
            cache.record_miss(URL, SCHEMA)

        # Should be auto-invalidated
        assert cache.get(URL, SCHEMA) is None

    def test_no_invalidate_below_threshold(self, cache):
        cache.set(URL, SCHEMA, SELECTORS, "abc", SHINGLES, None)
        # Record a miss but also many hits to stay below threshold
        for _ in range(20):
            cache.record_hit(URL, SCHEMA)
        cache.record_miss(URL, SCHEMA)
        # Still valid
        assert cache.get(URL, SCHEMA) is not None


class TestThreadSafety:
    def test_concurrent_record_hit(self, cache):
        cache.set(URL, SCHEMA, SELECTORS, "abc", SHINGLES, None)
        errors = []

        def do_hits():
            try:
                for _ in range(50):
                    cache.record_hit(URL, SCHEMA)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=do_hits) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Concurrent hits raised: {errors}"

        key = cache._make_key(URL, SCHEMA)
        assert cache._data[key]["hit_count"] == 250


class TestStats:
    def test_empty_stats(self, cache):
        s = cache.stats()
        assert s == {"total_entries": 0}

    def test_stats_after_set(self, cache):
        cache.set(URL, SCHEMA, SELECTORS, "abc", SHINGLES, None, llm_tokens_spent=500)
        s = cache.stats()
        assert s["total_entries"] == 1
        assert s["total_llm_tokens"] == 500
        assert s["avg_null_rate"] == 0.0
