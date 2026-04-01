"""
llm_extractor.py — LLM-powered semantic extraction using Claude API.

Two responsibilities:
1. EXTRACT: Given raw HTML, extract product fields semantically.
2. GENERATE SELECTORS: Produce CSS selectors for each field so future
   extractions can skip the LLM entirely (cache-first approach).

Cost control:
- HTML is cleaned and truncated before sending to LLM
- Selectors are cached → LLM called only on first visit or after drift
- Token usage is tracked per extraction
"""

import json
import re
import logging
from typing import Optional, Tuple
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# How much HTML to send to the LLM — balance between context and cost.
# In production: use a sliding window around likely product content areas.
MAX_HTML_CHARS = 15_000


def _clean_html_for_llm(html: str) -> str:
    """
    Strip noise from HTML before sending to LLM:
    - Remove scripts, styles, SVG, hidden elements
    - Remove comments
    - Collapse whitespace
    - Truncate to MAX_HTML_CHARS
    """
    soup = BeautifulSoup(html, "lxml")

    # Remove noise elements
    for tag in soup(["script", "style", "noscript", "svg", "iframe",
                     "link", "meta", "head"]):
        tag.decompose()

    # Remove hidden elements
    for tag in soup.find_all(style=re.compile(r"display\s*:\s*none")):
        tag.decompose()

    cleaned = str(soup)
    cleaned = re.sub(r"\s+", " ", cleaned)

    if len(cleaned) > MAX_HTML_CHARS:
        logger.debug(f"HTML truncated from {len(cleaned)} to {MAX_HTML_CHARS} chars")
        cleaned = cleaned[:MAX_HTML_CHARS] + "... [truncated]"

    return cleaned


EXTRACTION_SYSTEM_PROMPT = """You are an expert web scraping assistant. 
You will be given HTML from an e-commerce product page.

Your job is to:
1. Extract the product data fields listed in the schema
2. Generate precise CSS selectors that would reliably find each field

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
- Prefer ID selectors (#product-title) over class selectors
- Prefer data-* attributes (data-testid, data-qa) over generic classes
- Avoid positional selectors like :nth-child unless necessary
- Selectors should be specific enough to find exactly ONE element
- If you cannot find a field, set both extracted and selector to null
- Do NOT include markdown code fences in your response — pure JSON only"""


class LLMExtractor:
    """
    Semantic extractor using Claude API.
    Extracts product fields AND generates CSS selectors for caching.
    """

    def __init__(self, model: str = "claude-sonnet-4-6"):
        self.model = model
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                import anthropic
                self._client = anthropic.Anthropic()
            except Exception as e:
                raise RuntimeError(
                    f"Failed to initialize Anthropic client. "
                    f"Ensure ANTHROPIC_API_KEY is set. Error: {e}"
                )
        return self._client

    def extract(self, html: str, url: str) -> Tuple[dict, dict, int]:
        """
        Run LLM extraction on HTML.

        Returns:
            (extracted_fields, selectors, tokens_used)
            extracted_fields: dict of field_name → raw_value
            selectors: dict of field_name → CSS selector
            tokens_used: int
        """
        client = self._get_client()
        cleaned = _clean_html_for_llm(html)

        logger.info(f"Running LLM extraction for {url} ({len(cleaned)} chars of HTML)")

        try:
            response = client.messages.create(
                model=self.model,
                max_tokens=2048,
                system=EXTRACTION_SYSTEM_PROMPT,
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

            # Parse JSON response
            # Strip markdown fences if LLM added them despite instructions
            raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
            raw_text = re.sub(r"\s*```$", "", raw_text)

            result = json.loads(raw_text)

            extracted = result.get("extracted", {})
            selectors = result.get("selectors", {})
            confidence = result.get("confidence", 0.5)
            notes = result.get("notes", "")

            logger.info(
                f"LLM extraction complete: confidence={confidence:.2f}, "
                f"tokens={tokens_used}, notes='{notes[:80]}'"
            )

            return extracted, selectors, tokens_used

        except json.JSONDecodeError as e:
            logger.error(f"LLM returned invalid JSON: {e}\nRaw: {raw_text[:500]}")
            return {}, {}, 0
        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            return {}, {}, 0

    def explain_drift(self, old_html: str, new_html: str) -> str:
        """
        Ask LLM to explain what changed between two page versions.
        Useful for audit logs and alerting.
        """
        client = self._get_client()
        old_clean = _clean_html_for_llm(old_html)[:5000]
        new_clean = _clean_html_for_llm(new_html)[:5000]

        response = client.messages.create(
            model=self.model,
            max_tokens=512,
            messages=[{
                "role": "user",
                "content": (
                    f"Compare these two versions of an e-commerce product page HTML "
                    f"and briefly describe what structural changes occurred that would "
                    f"break a CSS-selector-based scraper.\n\n"
                    f"OLD HTML:\n{old_clean}\n\n"
                    f"NEW HTML:\n{new_clean}"
                )
            }]
        )
        return response.content[0].text.strip()
