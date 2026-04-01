# Self-Healing Web Crawler — POC

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        CRAWLER PIPELINE                         │
│                                                                 │
│  ┌──────────┐    ┌──────────────┐    ┌────────────────────┐    │
│  │ Playwright│───▶│ Change       │───▶│ Selector Cache     │    │
│  │ (fetch   │    │ Detector     │    │ (Redis/JSON store) │    │
│  │  page)   │    │              │    │                    │    │
│  └──────────┘    └──────┬───────┘    └────────┬───────────┘    │
│                         │ drift?               │ hit?           │
│                         ▼ yes                 ▼ no             │
│                  ┌──────────────┐    ┌────────────────────┐    │
│                  │ LLM Semantic │    │ Fast CSS/XPath     │    │
│                  │ Extractor    │    │ Extraction         │    │
│                  │ (Claude API) │    └────────────────────┘    │
│                  └──────┬───────┘                              │
│                         │ new selectors                        │
│                         ▼                                      │
│                  ┌──────────────┐    ┌────────────────────┐    │
│                  │ Selector     │───▶│ Validator          │    │
│                  │ Generator    │    │ (type + null check)│    │
│                  └──────────────┘    └────────┬───────────┘    │
│                                               │                │
│                                               ▼                │
│                                      ┌────────────────┐        │
│                                      │ SQLite Storage │        │
│                                      │ (data + audit) │        │
│                                      └────────────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

## Components

| Component | File | Responsibility |
|-----------|------|----------------|
| Schema | `crawler/schema.py` | Pydantic typed contract for product fields |
| Change Detector | `crawler/change_detector.py` | DOM hash + visual drift detection |
| Selector Cache | `cache/selector_cache.py` | Store/retrieve/invalidate cached selectors |
| LLM Extractor | `crawler/llm_extractor.py` | Claude API semantic extraction + selector generation |
| Fast Extractor | `crawler/fast_extractor.py` | CSS/XPath extraction using cached selectors |
| Validator | `crawler/validator.py` | Field-level null rate, type checks, anomaly detection |
| Storage | `storage/db.py` | SQLite: extracted data + change audit log |
| Orchestrator | `crawler/orchestrator.py` | Main pipeline coordination |
| Mock Site | `mock_site/` | Two versions of a product page to simulate redesign |
| CLI | `main.py` | Entry point |

## How Self-Healing Works

1. **First visit to a URL pattern:** No cached selectors exist → LLM extracts data semantically and generates CSS selectors → cached with TTL.
2. **Subsequent visits:** Cached selectors used directly (fast, cheap).
3. **Canary validation:** Every N requests, re-validate cached selectors. If null rate > 5% or type errors spike → cache invalidated.
4. **Change detection:** DOM structural hash + perceptual visual hash compared against baseline. Significant drift → immediate cache invalidation + LLM re-extraction.
5. **Audit trail:** Every extraction (method, confidence, selectors used) logged to SQLite.

## Running the POC

```bash
# Install dependencies
pip install -r requirements.txt
playwright install chromium

# Set your API key
export ANTHROPIC_API_KEY=your_key_here

# Run against mock site v1 (initial state)
python main.py --url mock_v1 --schema product

# Simulate site redesign - run against mock v2
python main.py --url mock_v2 --schema product

# Watch the self-healing in action
python main.py --demo
```
