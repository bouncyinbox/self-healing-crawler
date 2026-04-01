# Self-Healing Web Crawler

A production-quality web crawler that automatically detects when a website redesigns, invalidates stale CSS selectors, and uses Claude (LLM) to re-extract data and generate new selectors — all without any manual rule updates.

## The Problem

CSS-selector-based scrapers break silently when websites redesign. Maintaining hundreds of scraper rules across dozens of domains is expensive, slow, and fragile. A redesign at 2am that flips all IDs to class names will go undetected until someone notices bad data hours later.

## The Solution

A two-path extraction system that is both fast and resilient:

- **Fast path (99% of requests):** Use cached CSS selectors — near-zero cost, ~1–5ms per page.
- **Healing path (on drift or first visit):** Detect structural change via DOM fingerprinting, invalidate stale selectors, call the LLM to semantically re-extract and generate new selectors, cache them for the next visit.

---

## System Architecture

```
                          ┌─────────────────────────┐
                          │      Crawler Request      │
                          │   (URL or local HTML)     │
                          └────────────┬────────────┘
                                       │
                          ┌────────────▼────────────┐
                          │      Page Fetcher        │
                          │   Playwright (async)     │
                          │  Returns HTML + PNG      │
                          └────────────┬────────────┘
                                       │
                          ┌────────────▼────────────┐
                          │     Change Detector      │
                          │                          │
                          │  Structural fingerprint  │
                          │  Jaccard similarity on   │
                          │  DOM shingle n-grams      │
                          │                          │
                          │  Visual fingerprint      │
                          │  pHash on screenshot     │
                          │  Hamming distance        │
                          └────────┬────────┬────────┘
                                   │        │
                          drift?   │        │  no drift
                                   │        │
              ┌────────────────────┘        └─────────────────────┐
              │                                                    │
  ┌───────────▼──────────┐                         ┌──────────────▼──────────┐
  │   Invalidate Cache   │                         │     Cache Lookup        │
  │  (reason logged)     │                         │  key = schema:url_hash  │
  └───────────┬──────────┘                         └──────┬──────────┬───────┘
              │                                    miss   │          │  hit
              │                                           │          │
              │                          ┌────────────────┘          │
              │                          │                            │
  ┌───────────▼──────────────────────────▼─┐          ┌─────────────▼──────────┐
  │           LLM Extractor                │          │     Fast Extractor     │
  │      AsyncAnthropic (Claude)           │          │  BeautifulSoup + lxml  │
  │                                        │          │  CSS selector apply    │
  │  1. Content-aware HTML truncation      │          │  ~1–5ms, zero tokens   │
  │     (finds <main>/<article> first)     │          └─────────────┬──────────┘
  │  2. Semantic field extraction          │                        │
  │  3. CSS selector generation            │                        │
  │  4. Exponential backoff retry          │                        │
  │     (RateLimitError, NetworkError)     │                        │
  └───────────┬────────────────────────────┘                        │
              │ new selectors + extracted data                       │
              │                                                      │
  ┌───────────▼──────────┐                                          │
  │    Selector Cache    │◄─────────── Canary Validation ───────────┘
  │  JSON (dev) / Redis  │  record_hit() / record_miss()
  │  (production)        │  null_rate > 25% over 5+ samples
  │                      │  → auto-invalidate
  │  Write-behind buffer │
  │  Thread-safe Lock    │
  │  TTL: 7 days         │
  └──────────────────────┘
              │
  ┌───────────▼──────────────────────────────────────────────────────┐
  │                     Schema Validation                             │
  │              Pydantic v2  ProductSchema                           │
  │  price parser · currency map · rating bounds · in_stock parser   │
  │  null_rate() · confidence_score() · model_dump()                 │
  └───────────┬──────────────────────────────────────────────────────┘
              │
  ┌───────────▼──────────────────────────────────────────────────────┐
  │                      Async SQLite (WAL)                           │
  │  extractions table — structured product data                     │
  │  audit_log table   — method, drift severity, tokens, selectors   │
  └──────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
self-healing-crawler/
├── pyproject.toml              # Packaging, dependencies, pytest config
├── requirements.txt            # Flat dep list for tools that need it
├── .env.example                # All configurable env vars documented
│
├── crawler/                    # Installable Python package
│   ├── config.py               # CrawlerSettings (pydantic-settings)
│   ├── exceptions.py           # Typed exception hierarchy
│   ├── schema.py               # Pydantic v2 ProductSchema, SelectorMap, ExtractionResult
│   ├── change_detector.py      # DOM shingling + Jaccard similarity + pHash
│   ├── selector_cache.py       # Thread-safe JSON cache with write-behind buffer
│   ├── fast_extractor.py       # CSS selector extraction (BeautifulSoup/lxml)
│   ├── llm_extractor.py        # Async Claude API + retry + content-aware truncation
│   ├── db.py                   # Async SQLite via aiosqlite, WAL mode
│   ├── orchestrator.py         # Full pipeline, async context manager
│   └── main.py                 # Click CLI (demo / crawl / local / stats)
│
├── tests/
│   ├── conftest.py             # Shared fixtures (sample HTML, mock LLM response)
│   ├── unit/
│   │   ├── test_product_schema.py      # 42 tests — all field parsers + edge cases
│   │   ├── test_change_detector.py     # 22 tests — Jaccard correctness, drift logic
│   │   ├── test_selector_cache.py      # 15 tests — write-behind, TTL, thread safety
│   │   ├── test_fast_extractor.py      # 13 tests — selector hits/misses, null rate
│   │   └── test_llm_extractor.py       # 10 tests — retry logic, content truncation
│   └── integration/
│       └── test_orchestrator.py        # 7 tests — full 4-step self-healing pipeline
│
├── product_v1.html             # Mock product page (original design, ID-based selectors)
└── product_v2.html             # Mock product page (redesign, class-based selectors)
```

---

## Key Design Decisions

### 1. Structural Similarity: Jaccard on DOM Shingles

The original POC used XOR comparison on SHA256 hashes — fundamentally wrong. SHA256 is a cryptographic hash: a 1-character DOM change flips ~50% of output bits by design, making the similarity score meaningless (identical pages would score ~0.5).

**Fix:** DOM skeleton text is broken into overlapping 3-grams (shingles) of consecutive tag tokens. Jaccard similarity over these sets gives a true locality-sensitive score:

```
similarity = |shingles_A ∩ shingles_B| / |shingles_A ∪ shingles_B|
```

- Identical DOM → 1.0
- Text-only change (same tags, new content) → 1.0 (skeleton strips text)
- Complete redesign → near 0.0
- Threshold: < 0.85 triggers drift

Visual drift uses perceptual hash (pHash) with Hamming distance — consistent with how imagehash works.

### 2. Async LLM with Exponential Backoff

LLM calls are inherently slow (2–8s). Running them synchronously inside an async orchestrator would serialize all concurrent crawls through the event loop.

**Fix:** `AsyncAnthropic` client with `asyncio.wait_for` (60s timeout). Retryable errors (`RateLimitError`, `APIConnectionError`, `InternalServerError`) retry with delays of 1s → 2s → 4s. `AuthenticationError` fails immediately without retrying.

### 3. Cache Write-Behind Buffer

The original cache called `json.dump()` on every `record_hit()`. At 100 requests/sec with a 1000-entry cache this means ~5 MB/sec of disk I/O just for hit counters — purely advisory statistics.

**Fix:** Hit/miss counters update in-memory only. `set()` and `invalidate()` (durable events) write through immediately. `flush()` is called at orchestrator shutdown. Thread safety is provided by `threading.Lock` around all dict mutations.

### 4. Content-Aware HTML Truncation

Naively truncating HTML at `[:15000]` on product pages with heavy navigation often means the LLM sees 15KB of nav menus and never reaches the `<main>` section with the actual product data.

**Fix:** Before truncating, try a priority list of product section selectors (`[itemtype*='Product']`, `main`, `[role='main']`, `.product`, `article`, etc.). The first match with > 500 chars of content is used as the extraction target. Falls back to full-page truncation if none match.

### 5. Canary Validation

Gradual DOM drift (e.g., a class rename that only affects some pages) won't trigger the structural similarity threshold immediately. Canary validation catches slow degradation:

- Every `record_miss()` increments a counter.
- After ≥ 5 samples, if `miss_count / total > 25%` → auto-invalidate the cache entry.
- Thresholds are configurable via `CRAWLER_CACHE_NULL_RATE_THRESHOLD` and `CRAWLER_CACHE_NULL_RATE_MIN_SAMPLES`.

---

## Cost Model

| Scenario | Latency | LLM Tokens |
|---|---|---|
| Cache hit | ~1–5ms | 0 |
| First visit (LLM extraction) | ~3–8s | ~600–1200 |
| Drift detected (LLM re-extraction) | ~3–8s | ~600–1200 |

At a 95% cache hit rate across 10,000 daily crawls:
- 9,500 cache hits → 0 tokens
- 500 LLM calls × ~900 tokens avg → ~450,000 tokens/day
- ~95% cost reduction vs. calling the LLM on every request

---

## Setup

### What you need

| Requirement | Why | Where to get it |
|---|---|---|
| Python 3.9+ | Runtime | [python.org](https://www.python.org/downloads/) |
| Anthropic API key | Powers LLM extraction and selector generation | [console.anthropic.com](https://console.anthropic.com/) |
| Chromium (via Playwright) | Renders JS-heavy pages for real-URL crawling | Installed automatically — see step 3 below |

> **No API key?** The demo still runs — change detection, caching, and CSS extraction all work. LLM extraction is skipped and all product fields will be null. To see the full self-healing loop you need the key.

---

### Step 1 — Clone and install

```bash
git clone https://github.com/bouncyinbox/self-healing-crawler.git
cd self-healing-crawler

# Install the package and all dependencies (including dev/test tools)
pip install -e ".[dev]"
```

This installs: `anthropic`, `pydantic`, `pydantic-settings`, `playwright`, `beautifulsoup4`, `lxml`, `aiosqlite`, `imagehash`, `click`, `rich`, and test tools (`pytest`, `pytest-asyncio`, `pytest-mock`).

### Step 2 — Set your API key

```bash
cp .env.example .env
```

Open `.env` and set:

```bash
ANTHROPIC_API_KEY=sk-ant-your-key-here
```

Get a key at [console.anthropic.com](https://console.anthropic.com/) → API Keys → Create Key. The demo uses `claude-sonnet-4-6` and consumes ~600–1200 tokens per LLM extraction (roughly $0.003–0.006 per run).

### Step 3 — Install the browser (only needed for live URLs)

```bash
playwright install chromium
```

This downloads a ~120 MB Chromium binary. It is **only required** if you plan to crawl live URLs (`crawler crawl <url>`). The demo and local file commands (`crawler demo`, `crawler local`) do not use the browser.

---

### Run the demo

```bash
python main.py demo
```

This runs 4 crawls against the two bundled mock HTML files (`product_v1.html` and `product_v2.html`) — no browser, no network, no extra setup needed beyond Step 1 and 2.

**What each run does:**

| Run | HTML served | What happens |
|---|---|---|
| 1 | `product_v1.html` | No cache → LLM extracts product data + generates CSS selectors → selectors cached |
| 2 | `product_v1.html` | Cache hit → fast CSS extraction, 0 LLM tokens used |
| 3 | `product_v2.html` (same URL) | Drift detected (IDs removed, class names changed) → cache invalidated → LLM re-extracts with new selectors |
| 4 | `product_v2.html` | Cache hit with new selectors → fast extraction again |

**Expected output (with API key set):**

```
RUN 1  method=LLM_EXTRACTION   cache_hit=NO   drift=NONE      confidence=100%
RUN 2  method=CACHE_HIT        cache_hit=YES  drift=NONE      confidence=100%  tokens=0
RUN 3  method=LLM_EXTRACTION   cache_hit=NO   drift=DETECTED  confidence=100%
RUN 4  method=CACHE_HIT        cache_hit=YES  drift=NONE      confidence=100%  tokens=0
```

**Without API key:** All runs show `method=NO_EXTRACTOR` and null product fields. The drift detection still fires on RUN 3 (different DOM hash), but no extraction happens.

---

### Other commands

```bash
# Crawl a live URL (requires Playwright browser installed)
crawler crawl https://example.com/product/123

# Crawl a local HTML file
crawler local product_v1.html

# Show cache and DB statistics
crawler stats

# Override the schema name
crawler demo --schema product_v2

# Verbose logging
crawler --log-level DEBUG demo
```

---

### Configuration

All settings have sensible defaults and can be overridden via environment variables (prefix: `CRAWLER_`) or in your `.env` file:

```bash
# ── Required ───────────────────────────────────────────────────────
ANTHROPIC_API_KEY=sk-ant-...

# ── LLM ────────────────────────────────────────────────────────────
CRAWLER_LLM_MODEL=claude-sonnet-4-6       # Claude model to use
CRAWLER_LLM_MAX_HTML_CHARS=15000          # HTML chars sent to LLM per request
CRAWLER_LLM_RETRY_ATTEMPTS=3             # Retries on rate-limit / network error

# ── Cache ───────────────────────────────────────────────────────────
CRAWLER_CACHE_PATH=/tmp/crawler_selector_cache.json
CRAWLER_CACHE_TTL_SECONDS=604800          # Selector TTL — 7 days
CRAWLER_CACHE_NULL_RATE_THRESHOLD=0.25   # Auto-invalidate above 25% null rate
CRAWLER_CACHE_NULL_RATE_MIN_SAMPLES=5    # Minimum samples before auto-invalidation

# ── Change detection ─────────────────────────────────────────────────
CRAWLER_STRUCTURAL_DRIFT_THRESHOLD=0.85  # Jaccard similarity below this → drift
CRAWLER_VISUAL_DRIFT_THRESHOLD=0.80      # pHash Hamming similarity below this → drift

# ── Storage ──────────────────────────────────────────────────────────
CRAWLER_DB_PATH=/tmp/crawler_data.db

# ── Schema / extraction ───────────────────────────────────────────────
CRAWLER_SCHEMA_NAME=product_v1           # Schema used as cache namespace

# ── Browser ───────────────────────────────────────────────────────────
CRAWLER_BROWSER_HEADLESS=true            # Set false to watch the browser open
```

---

## Running Tests

```bash
# All tests
python3 -m pytest tests/ -v

# Unit tests only (no API calls, fast)
python3 -m pytest tests/unit/ -v

# With coverage
python3 -m pytest tests/ --cov=crawler --cov-report=term-missing
```

**109 tests, all passing:**

| Suite | Tests | Covers |
|---|---|---|
| `test_product_schema.py` | 42 | All field parsers, truncation, null rate, confidence |
| `test_change_detector.py` | 22 | Jaccard similarity, shingles, drift thresholds, severity |
| `test_selector_cache.py` | 15 | get/set/invalidate, TTL, write-behind, thread safety, canary |
| `test_fast_extractor.py` | 13 | Selector hits/misses, null rate, v1 selectors fail on v2 |
| `test_llm_extractor.py` | 10 | Retry logic, auth error, content-aware truncation, JSON parsing |
| `test_orchestrator.py` | 7 | Full 4-step pipeline, schema validation, DB writes |

---

## Scaling to Production

The current implementation uses JSON file caching and SQLite. The interfaces are designed for straightforward backend swaps:

| Component | Current (dev) | Production swap |
|---|---|---|
| Selector cache | JSON file | Redis `HSET` with `EXPIRE` — O(1) reads, atomic, cluster-ready |
| Database | SQLite (aiosqlite) | PostgreSQL (asyncpg) or ClickHouse for analytics |
| Browser | Single Playwright instance | Browser pool with `asyncio.Semaphore` concurrency control |
| LLM client | `AsyncAnthropic` | Same — already async, add rate limiter per API key |
| Orchestrator | Single process | Celery / RQ / asyncio task queue for distributed crawling |

No calling code changes are required for cache or DB swaps — both expose the same async interface.

---

## Exception Hierarchy

```
CrawlerError
├── CrawlerFetchError        Page fetch failed (timeout, browser crash)
├── CrawlerCacheError        Cache read/write failed
├── CrawlerSchemaError       Schema validation failed
└── CrawlerExtractionError   Data extraction failed
    └── CrawlerLLMError      LLM API call failed after retries
                             (carries status_code for observability)
```

---

## License

MIT
