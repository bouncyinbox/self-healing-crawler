"""
Unit tests for LLMExtractor — mocking the Anthropic client to test
retry logic, JSON parsing, content-aware truncation, and error handling.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crawler.llm_extractor import LLMExtractor, _clean_html_for_llm
from crawler.exceptions import CrawlerLLMError, CrawlerExtractionError


@pytest.fixture
def extractor():
    return LLMExtractor()


def _make_response(payload: dict) -> MagicMock:
    """Build a mock Anthropic message response."""
    response = MagicMock()
    response.content = [MagicMock(text=json.dumps(payload))]
    response.usage = MagicMock(input_tokens=500, output_tokens=100)
    return response


class TestCleanHtmlForLlm:
    def test_strips_scripts(self):
        html = "<html><body><script>alert(1)</script><h1>Title</h1></body></html>"
        result = _clean_html_for_llm(html)
        assert "alert" not in result
        assert "Title" in result

    def test_selects_main_element(self):
        # main section must be >500 chars to pass the minimum-content threshold
        main_content = "<h1 id='product-title'>Widget Pro</h1>" + "<p>" + "Product detail. " * 50 + "</p>"
        html = f"""
        <html><body>
          <nav>Big nav block {"x" * 2000}</nav>
          <main>{main_content}</main>
        </body></html>
        """
        result = _clean_html_for_llm(html)
        assert "Widget Pro" in result
        # The result should be the main section, not the full page with nav first
        assert result.strip().startswith("<main")

    def test_truncates_at_max_chars(self, monkeypatch):
        monkeypatch.setattr("crawler.llm_extractor.settings.llm_max_html_chars", 100)
        html = "<html><body>" + "x" * 10000 + "</body></html>"
        result = _clean_html_for_llm(html)
        assert len(result) <= 115  # 100 + "[truncated]" suffix

    def test_falls_back_to_full_html_if_no_product_section(self):
        html = "<html><body><div class='unrelated'>Some content</div></body></html>"
        result = _clean_html_for_llm(html)
        assert "Some content" in result


class TestExtract:
    @pytest.mark.asyncio
    async def test_successful_extraction(self, extractor, mock_llm_response, html_v1):
        mock_response = _make_response(mock_llm_response)
        with patch.object(extractor, "_call_with_retry", new=AsyncMock(return_value=mock_response)):
            fields, selectors, tokens = await extractor.extract(html_v1, "https://example.com")

        assert fields["title"] == "TechGear Pro X500 Wireless Headphones"
        assert selectors["title"] == "#product-title"
        assert tokens == 600  # 500 + 100

    @pytest.mark.asyncio
    async def test_invalid_json_raises_extraction_error(self, extractor, html_v1):
        bad_response = MagicMock()
        bad_response.content = [MagicMock(text="not json at all")]
        bad_response.usage = MagicMock(input_tokens=100, output_tokens=10)

        with patch.object(extractor, "_call_with_retry", new=AsyncMock(return_value=bad_response)):
            with pytest.raises(CrawlerExtractionError, match="invalid JSON"):
                await extractor.extract(html_v1, "https://example.com")

    @pytest.mark.asyncio
    async def test_strips_markdown_fences(self, extractor, mock_llm_response, html_v1):
        fenced = f"```json\n{json.dumps(mock_llm_response)}\n```"
        fenced_response = MagicMock()
        fenced_response.content = [MagicMock(text=fenced)]
        fenced_response.usage = MagicMock(input_tokens=100, output_tokens=50)

        with patch.object(extractor, "_call_with_retry", new=AsyncMock(return_value=fenced_response)):
            fields, _, _ = await extractor.extract(html_v1, "https://example.com")

        assert fields["title"] == "TechGear Pro X500 Wireless Headphones"


class TestRetryLogic:
    @pytest.mark.asyncio
    async def test_retries_on_rate_limit_error(self, extractor, html_v1, mock_llm_response):
        import anthropic

        call_count = 0
        success_response = _make_response(mock_llm_response)

        async def flaky(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise anthropic.RateLimitError(
                    message="rate limited",
                    response=MagicMock(status_code=429),
                    body={},
                )
            return success_response

        client_mock = AsyncMock()
        client_mock.messages.create = flaky
        extractor._client = client_mock

        # Patch sleep to speed up test
        with patch("asyncio.sleep", new=AsyncMock()):
            fields, _, _ = await extractor.extract(html_v1, "https://example.com")

        assert call_count == 2
        assert fields["title"] == "TechGear Pro X500 Wireless Headphones"

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self, extractor, html_v1):
        import anthropic
        from crawler.config import settings

        async def always_rate_limit(*args, **kwargs):
            raise anthropic.RateLimitError(
                message="rate limited",
                response=MagicMock(status_code=429),
                body={},
            )

        client_mock = AsyncMock()
        client_mock.messages.create = always_rate_limit
        extractor._client = client_mock

        with patch("asyncio.sleep", new=AsyncMock()):
            with pytest.raises(CrawlerLLMError):
                await extractor.extract(html_v1, "https://example.com")

    @pytest.mark.asyncio
    async def test_auth_error_does_not_retry(self, extractor, html_v1):
        import anthropic

        call_count = 0

        async def auth_fail(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise anthropic.AuthenticationError(
                message="invalid key",
                response=MagicMock(status_code=401),
                body={},
            )

        client_mock = AsyncMock()
        client_mock.messages.create = auth_fail
        extractor._client = client_mock

        with pytest.raises(CrawlerLLMError, match="authentication"):
            await extractor.extract(html_v1, "https://example.com")

        # Should NOT retry on auth errors
        assert call_count == 1
