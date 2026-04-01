"""
db.py — SQLite storage for extractions and audit trail.

Uses aiosqlite for non-blocking async I/O in the event loop.
WAL mode is enabled so concurrent readers don't block writers.

In production at scale: replace with PostgreSQL (asyncpg) or ClickHouse
for analytics workloads. The public interface (save_extraction, log_audit,
get_recent_extractions, summary_stats) is identical.
"""

from __future__ import annotations


import json
import logging
import time
from typing import Optional

import aiosqlite

from crawler.config import settings
from crawler.exceptions import CrawlerError

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS extractions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT    NOT NULL,
    crawled_at      REAL    NOT NULL,
    title           TEXT,
    price           REAL,
    currency        TEXT,
    in_stock        INTEGER,
    rating          REAL,
    review_count    INTEGER,
    sku             TEXT,
    brand           TEXT,
    description     TEXT,
    confidence      REAL,
    method          TEXT,
    cache_hit       INTEGER,
    drift_detected  INTEGER
);

CREATE TABLE IF NOT EXISTS audit_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    url                 TEXT    NOT NULL,
    crawled_at          REAL    NOT NULL,
    method              TEXT    NOT NULL,
    cache_hit           INTEGER,
    drift_detected      INTEGER,
    drift_severity      TEXT,
    structural_hash     TEXT,
    visual_hash         TEXT,
    null_rate           REAL,
    confidence          REAL,
    llm_tokens_used     INTEGER,
    selectors_used      TEXT,
    error               TEXT
);

CREATE INDEX IF NOT EXISTS idx_extractions_url        ON extractions(url);
CREATE INDEX IF NOT EXISTS idx_extractions_crawled_at ON extractions(crawled_at);
CREATE INDEX IF NOT EXISTS idx_audit_url              ON audit_log(url);
CREATE INDEX IF NOT EXISTS idx_audit_crawled_at       ON audit_log(crawled_at);
"""


class CrawlerDB:
    """Async SQLite storage with WAL mode for concurrent access."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._path = db_path or settings.db_path
        self._ready = False

    async def _ensure_ready(self) -> None:
        """Initialise schema on first use (lazy, to avoid blocking __init__)."""
        if self._ready:
            return
        async with aiosqlite.connect(self._path) as conn:
            await conn.executescript(_SCHEMA_SQL)
            await conn.commit()
        self._ready = True
        logger.debug("Database initialised: %s", self._path)

    async def save_extraction(
        self,
        url: str,
        data: dict,
        method: str,
        cache_hit: bool,
        drift_detected: bool,
        confidence: float,
    ) -> None:
        await self._ensure_ready()
        async with aiosqlite.connect(self._path) as conn:
            await conn.execute(
                """
                INSERT INTO extractions
                    (url, crawled_at, title, price, currency, in_stock, rating,
                     review_count, sku, brand, description, confidence, method,
                     cache_hit, drift_detected)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    url,
                    time.time(),
                    data.get("title"),
                    data.get("price"),
                    data.get("currency"),
                    int(data["in_stock"]) if data.get("in_stock") is not None else None,
                    data.get("rating"),
                    data.get("review_count"),
                    data.get("sku"),
                    data.get("brand"),
                    data.get("description"),
                    confidence,
                    method,
                    int(cache_hit),
                    int(drift_detected),
                ),
            )
            await conn.commit()
        logger.debug("Saved extraction for %s", url)

    async def log_audit(
        self,
        url: str,
        method: str,
        cache_hit: bool,
        drift_detected: bool,
        drift_severity: str,
        structural_hash: str,
        visual_hash: Optional[str],
        null_rate: float,
        confidence: float,
        llm_tokens_used: int,
        selectors_used: Optional[dict],
        error: Optional[str] = None,
    ) -> None:
        await self._ensure_ready()
        async with aiosqlite.connect(self._path) as conn:
            await conn.execute(
                """
                INSERT INTO audit_log
                    (url, crawled_at, method, cache_hit, drift_detected, drift_severity,
                     structural_hash, visual_hash, null_rate, confidence, llm_tokens_used,
                     selectors_used, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    url,
                    time.time(),
                    method,
                    int(cache_hit),
                    int(drift_detected),
                    drift_severity,
                    structural_hash,
                    visual_hash,
                    null_rate,
                    confidence,
                    llm_tokens_used,
                    json.dumps(selectors_used) if selectors_used else None,
                    error,
                ),
            )
            await conn.commit()

    async def get_recent_extractions(self, url: str, limit: int = 10) -> list[dict]:
        await self._ensure_ready()
        async with aiosqlite.connect(self._path) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(
                "SELECT * FROM extractions WHERE url = ? ORDER BY crawled_at DESC LIMIT ?",
                (url, limit),
            )
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_null_rate_history(self, url: str) -> list[dict]:
        """Null rate trend — used to detect gradual selector drift."""
        await self._ensure_ready()
        async with aiosqlite.connect(self._path) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(
                "SELECT crawled_at, null_rate, method, drift_detected "
                "FROM audit_log WHERE url = ? ORDER BY crawled_at",
                (url,),
            )
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def summary_stats(self) -> dict:
        """Dashboard-level statistics."""
        await self._ensure_ready()
        async with aiosqlite.connect(self._path) as conn:
            async def scalar(sql: str) -> int:
                cur = await conn.execute(sql)
                row = await cur.fetchone()
                return row[0] if row else 0

            total = await scalar("SELECT COUNT(*) FROM extractions")
            llm_runs = await scalar("SELECT COUNT(*) FROM audit_log WHERE cache_hit = 0")
            cache_runs = await scalar("SELECT COUNT(*) FROM audit_log WHERE cache_hit = 1")
            drift_events = await scalar("SELECT COUNT(*) FROM audit_log WHERE drift_detected = 1")
            total_tokens = await scalar(
                "SELECT COALESCE(SUM(llm_tokens_used), 0) FROM audit_log"
            )

        denom = cache_runs + llm_runs
        return {
            "total_extractions": total,
            "llm_runs": llm_runs,
            "cache_hits": cache_runs,
            "drift_events": drift_events,
            "total_llm_tokens": total_tokens,
            "cache_hit_rate": cache_runs / denom if denom > 0 else 0.0,
        }
