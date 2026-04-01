"""
llm_extractor.py — Async LLM-powered semantic extraction via Claude API.

Two responsibilities:
1. EXTRACT: Given raw HTML, extract product fields semantically.
2. GENERATE SELECTORS: Produce CSS selectors for fast future extraction.

Production considerations:
- Async: uses AsyncAnthropic so the event loop is never blocked.
- Retry: exponential backoff on rate-limit and transient network errors.
- Content-aware truncation: locates product content sections in the DOM
  before truncating, rather than blindly taking the first N characters.
- Token tracking: every call reports usage for cost monitoring.
"""

from __future__ import annotations


import asyncio
import json
import logging
import re
from typing import Optional

from bs4 import BeautifulSoup

from crawler.config import settings
from crawler.exceptions import CrawlerLLMError, CrawlerExtractionError

logger = logging.getLogger(__name__)

# Heuristic selectors for finding the main product content section.
# Tried in order; first match with >500 chars of content wins.
_PRODUCT_CONTENT_SELECTORS = [
    "[itemtype*='Product']",
    "main",
    "[role='main']",
    "#product",
    ".product",
    "#pdp",
    ".pdp",
    "article",
    ".product-detail",
    ".product-page",
]

_EXTRACTION_SYSTEM_PROMPT = """\
You are an expert web scraping assistant.
You will be given HTML from an e-commerce product page.

Your job is to:
1. Extract the product data fields listed in the schema.
2. Generate precise CSS selectors that will reliably find each field.

Return ONLY a valid JSON object with this exact structure:
{
  "extracted": {
    "title": "<product title or null>",
    "price": "<price as string with currency symbol, e.g. '$29.99' or null>",
    "currency": "<currency symbol or code, e.g. '$' or 'USD' or null>",
    "in_stock": "<availability text like 'In Stock' or 'Out of Stock' or null>",
    "rating": "<rating like '4.5' or null>",
    "review_count": "<number of reviews like '1,234' or null>",
    "sku": "<SKU or product ID or null>",
    "brand": "<brand name or null>",
    "description": "<short product description, max 200 chars, or null>"
  },
  "selectors": {
    "title": "<CSS selector for title element or null>",
    "price": "<CSS selector for price element or null>",
    "currency": "<CSS selector for currency element or null>",
    "in_stock": "<CSS selector for stock status element or null>",
    "rating": "<CSS selector for rating element or null>",
    "review_count": "<CSS selector for review count element or null>",
    "sku": "<CSS selector for SKU element or null>",
    "brand": "<CSS selector for brand element or null>",
    "description": "<CSS selector for description element or null>"
  },
  "confidence": <float 0.0 to 1.0 reflecting your confidence>,
  "notes": "<any observations about the page structure>"
}

Rules for selectors:
- Prefer ID selectors (#product-title) over class selectors.
- Prefer data-* attributes (data-testid, data-qa) over generic classes.
- Avoid positional selectors like :nth-child unless necessary.
- Selectors should match exactly ONE element on the page.
- If you cannot find a field, set both extracted and selector to null.
- Return pure JSON only — no markdown code fences."""


def _clean_html_for_llm(html: str) -> str:
    """
    Prepare HTML for the LLM:
    1. Strip noise (scripts, styles, hidden elements).
    2. Locate product content section to avoid wasting tokens on navigation.
    3. Truncate to MAX_HTML_CHARS.
    """
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "svg", "iframe", "link", "meta", "head"]):
        tag.decompose()
    for tag in soup.find_all(style=re.compile(r"display\s*:\s*none", re.I)):
        tag.decompose()

    # Try to find the product section first so we don't truncate navigation into it
    max_chars = settings.llm_max_html_chars
    for selector in _PRODUCT_CONTENT_SELECTORS:
        try:
            node = soup.select_one(selector)
            if node:
                content = str(node)
                if len(content) > 500:
                    if len(content) > max_chars:
                        logger.debug(
                            "HTML truncated from %d to %d chars (product section found)",
                            len(content),
                            max_chars,
                        )
                        return content[:max_chars] + "... [truncated]"
                    return content
        except Exception:
            continue

    # Fallback: full cleaned HTML
    full = str(soup)
    if len(full) > max_chars:
        logger.debug("HTML truncated from %d to %d chars (no product section found)", len(full), max_chars)
        return full[:max_chars] + "... [truncated]"
    return full


class LLMExtractor:
    """
    Async semantic extractor using the Claude API.
    Extracts product fields AND generates CSS selectors for caching.
    """

    def __init__(self) -> None:
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                import anthropic
                api_key = settings.anthropic_api_key or None
                self._client = anthropic.AsyncAnthropic(api_key=api_key)
            except ImportError as exc:
                raise CrawlerLLMError(
                    "anthropic package not installed. Run: pip install anthropic"
                ) from exc
        return self._client

    async def _call_with_retry(self, **kwargs) -> object:
        """
        Call client.messages.create with exponential backoff on transient errors.
        Raises CrawlerLLMError after all attempts are exhausted.
        """
        import anthropic

        client = self._get_client()
        attempts = settings.llm_retry_attempts
        delay = settings.llm_retry_base_delay

        _retryable = (
            anthropic.RateLimitError,
            anthropic.APIConnectionError,
            anthropic.InternalServerError,
        )

        for attempt in range(1, attempts + 1):
            try:
                return await asyncio.wait_for(
                    client.messages.create(**kwargs),
                    timeout=60.0,
                )
            except _retryable as exc:
                if attempt == attempts:
                    raise CrawlerLLMError(
                        f"LLM call failed after {attempts} attempts: {exc}",
                        status_code=getattr(exc, "status_code", None),
                    ) from exc
                wait = delay * (2 ** (attempt - 1))
                logger.warning(
                    "LLM transient error (attempt %d/%d), retrying in %.1fs: %s",
                    attempt, attempts, wait, exc,
                )
                await asyncio.sleep(wait)
            except anthropic.AuthenticationError as exc:
                raise CrawlerLLMError(
                    "Anthropic authentication failed — check ANTHROPIC_API_KEY",
                    status_code=401,
                ) from exc
            except asyncio.TimeoutError as exc:
                if attempt == attempts:
                    raise CrawlerLLMError("LLM call timed out after 60s") from exc
                await asyncio.sleep(delay * attempt)

        raise CrawlerLLMError(f"LLM call failed after {attempts} attempts")

    async def extract(self, html: str, url: str) -> tuple[dict, dict, int]:
        """
        Run LLM extraction on HTML.

        Returns:
            (extracted_fields, selectors, tokens_used)
            extracted_fields: dict[field_name → raw_value]
            selectors:        dict[field_name → CSS selector]
            tokens_used:      int (input + output)
        """
        cleaned = _clean_html_for_llm(html)
        logger.info("LLM extraction for %s (%d chars of HTML)", url, len(cleaned))

        response = await self._call_with_retry(
            model=settings.llm_model,
            max_tokens=settings.llm_max_tokens,
            system=_EXTRACTION_SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Extract product data from this e-commerce page.\n"
                        f"URL: {url}\n\n"
                        f"HTML:\n{cleaned}"
                    ),
                }
            ],
        )

        tokens_used = response.usage.input_tokens + response.usage.output_tokens
        raw_text = response.content[0].text.strip()

        # Strip markdown fences if the model added them despite the instruction
        raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
        raw_text = re.sub(r"\s*```$", "", raw_text)

        try:
            result = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise CrawlerExtractionError(
                f"LLM returned invalid JSON: {exc}\nRaw response (first 500 chars): {raw_text[:500]}"
            ) from exc

        extracted = result.get("extracted", {})
        selectors = result.get("selectors", {})
        confidence = result.get("confidence", 0.5)
        notes = result.get("notes", "")

        logger.info(
            "LLM extraction complete: confidence=%.2f tokens=%d notes=%r",
            confidence, tokens_used, notes[:80],
        )

        return extracted, selectors, tokens_used

    async def explain_drift(self, old_html: str, new_html: str) -> str:
        """
        Ask the LLM to describe what changed between two HTML versions.
        Useful for audit logs and alerting.
        """
        max_chars = settings.llm_max_html_chars // 3
        old_clean = _clean_html_for_llm(old_html)[:max_chars]
        new_clean = _clean_html_for_llm(new_html)[:max_chars]

        response = await self._call_with_retry(
            model=settings.llm_model,
            max_tokens=512,
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Compare these two versions of an e-commerce product page and "
                        "briefly describe what structural changes would break a CSS-selector-based scraper.\n\n"
                        f"OLD HTML:\n{old_clean}\n\n"
                        f"NEW HTML:\n{new_clean}"
                    ),
                }
            ],
        )
        return response.content[0].text.strip()
