"""
config.py — Centralised configuration via pydantic-settings.

Every constant that was previously scattered across modules lives here.
Override any setting with an environment variable: CRAWLER_<FIELD_NAME>=value
or via a .env file in the project root.
"""

from __future__ import annotations


from pydantic_settings import BaseSettings, SettingsConfigDict


class CrawlerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CRAWLER_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── LLM ──────────────────────────────────────────────────────────────
    anthropic_api_key: str = ""
    llm_model: str = "claude-sonnet-4-6"
    llm_max_tokens: int = 2048
    llm_max_html_chars: int = 15_000
    llm_retry_attempts: int = 3
    llm_retry_base_delay: float = 1.0  # seconds; doubles each attempt

    # ── Cache ─────────────────────────────────────────────────────────────
    cache_path: str = "/tmp/crawler_selector_cache.json"
    cache_ttl_seconds: int = 60 * 60 * 24 * 7  # 7 days
    cache_null_rate_threshold: float = 0.25     # auto-invalidate above this
    cache_null_rate_min_samples: int = 5        # minimum samples before auto-invalidation

    # ── Change detection ──────────────────────────────────────────────────
    structural_drift_threshold: float = 0.85   # Jaccard similarity below this → drift
    visual_drift_threshold: float = 0.80       # Hamming similarity below this → drift
    dom_shingle_size: int = 3                  # n-gram size for DOM shingling

    # ── Storage ───────────────────────────────────────────────────────────
    db_path: str = "/tmp/crawler_data.db"

    # ── Schema / extraction ───────────────────────────────────────────────
    schema_name: str = "product_v1"

    # ── Browser ───────────────────────────────────────────────────────────
    browser_headless: bool = True
    page_timeout_ms: int = 30_000
    page_wait_ms: int = 3_000


# Module-level singleton — import this everywhere instead of using constructors.
settings = CrawlerSettings()
