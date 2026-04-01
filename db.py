"""
db.py — SQLite storage for extracted product data and audit trail.

Two tables:
1. extractions — the actual product data extracted
2. audit_log — every run: method used, drift detected, tokens spent, etc.

In production: PostgreSQL or a columnar store like ClickHouse for analytics.
SQLite is fine for POC and single-node deployments.
"""

import sqlite3
import json
import time
import logging
from pathlib import Path
from typing import Optional, List
from contextlib import contextmanager

logger = logging.getLogger(__name__)

DB_PATH = "/tmp/crawler_data.db"


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS extractions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT NOT NULL,
    crawled_at      REAL NOT NULL,
    title           TEXT,
    price           REAL,
    currency        TEXT,
    in_stock        INTEGER,        -- SQLite has no BOOL
    rating          REAL,
    review_count    INTEGER,
    sku             TEXT,
    brand           TEXT,
    description     TEXT,
    confidence      REAL,
    method          TEXT,           -- 'cache_hit' | 'llm_extraction' | 'llm_fallback'
    cache_hit       INTEGER,
    drift_detected  INTEGER
);

CREATE TABLE IF NOT EXISTS audit_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    url                 TEXT NOT NULL,
    crawled_at          REAL NOT NULL,
    method              TEXT NOT NULL,
    cache_hit           INTEGER,
    drift_detected      INTEGER,
    drift_severity      TEXT,
    structural_hash     TEXT,
    visual_hash         TEXT,
    null_rate           REAL,
    confidence          REAL,
    llm_tokens_used     INTEGER,
    selectors_used      TEXT,       -- JSON blob
    error               TEXT
);

CREATE INDEX IF NOT EXISTS idx_extractions_url ON extractions(url);
CREATE INDEX IF NOT EXISTS idx_audit_url ON audit_log(url);
CREATE INDEX IF NOT EXISTS idx_audit_crawled_at ON audit_log(crawled_at);
"""


class CrawlerDB:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript(SCHEMA_SQL)

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def save_extraction(self, url: str, data: dict, method: str,
                        cache_hit: bool, drift_detected: bool, confidence: float):
        """Persist extracted product data."""
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO extractions
                    (url, crawled_at, title, price, currency, in_stock, rating,
                     review_count, sku, brand, description, confidence, method,
                     cache_hit, drift_detected)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                url,
                time.time(),
                data.get("title"),
                data.get("price"),
                data.get("currency"),
                int(data.get("in_stock")) if data.get("in_stock") is not None else None,
                data.get("rating"),
                data.get("review_count"),
                data.get("sku"),
                data.get("brand"),
                data.get("description"),
                confidence,
                method,
                int(cache_hit),
                int(drift_detected),
            ))
        logger.debug(f"Saved extraction for {url}")

    def log_audit(
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
    ):
        """Append to audit log — used for monitoring and debugging."""
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO audit_log
                    (url, crawled_at, method, cache_hit, drift_detected, drift_severity,
                     structural_hash, visual_hash, null_rate, confidence, llm_tokens_used,
                     selectors_used, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
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
            ))

    def get_recent_extractions(self, url: str, limit: int = 10) -> List[dict]:
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT * FROM extractions
                WHERE url = ? ORDER BY crawled_at DESC LIMIT ?
            """, (url, limit)).fetchall()
        return [dict(r) for r in rows]

    def get_null_rate_history(self, url: str) -> List[dict]:
        """Get null rate trend for a URL — used to spot gradual drift."""
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT crawled_at, null_rate, method, drift_detected
                FROM audit_log WHERE url = ? ORDER BY crawled_at
            """, (url,)).fetchall()
        return [dict(r) for r in rows]

    def summary_stats(self) -> dict:
        """Dashboard-level statistics."""
        with self._conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM extractions").fetchone()[0]
            llm_runs = conn.execute(
                "SELECT COUNT(*) FROM audit_log WHERE cache_hit = 0"
            ).fetchone()[0]
            cache_runs = conn.execute(
                "SELECT COUNT(*) FROM audit_log WHERE cache_hit = 1"
            ).fetchone()[0]
            drift_events = conn.execute(
                "SELECT COUNT(*) FROM audit_log WHERE drift_detected = 1"
            ).fetchone()[0]
            total_tokens = conn.execute(
                "SELECT COALESCE(SUM(llm_tokens_used), 0) FROM audit_log"
            ).fetchone()[0]

        return {
            "total_extractions": total,
            "llm_runs": llm_runs,
            "cache_hits": cache_runs,
            "drift_events": drift_events,
            "total_llm_tokens": total_tokens,
            "cache_hit_rate": cache_runs / (cache_runs + llm_runs) if (cache_runs + llm_runs) > 0 else 0,
        }
