"""Unit tests for the Cloud Run scraper module."""
from unittest.mock import patch, MagicMock

WITH_NEXT_HTML = """
<html><body>
  <div class="quote">
    <span class="text">"The world as we have created it"</span>
    <small class="author">Albert Einstein</small>
    <a href="/author/Albert-Einstein">About</a>
  </div>
  <ul class="pager"><li class="next"><a href="/page/2/">Next</a></li></ul>
</body></html>
"""

NO_NEXT_HTML = """
<html><body>
  <div class="quote">
    <span class="text">"Last page quote"</span>
    <small class="author">Jane Doe</small>
    <a href="/author/Jane-Doe">About</a>
  </div>
</body></html>
"""


@patch("scraper.requests.Session.get")
def test_discover_pages_with_next(mock_get):
    mock_resp = MagicMock()
    mock_resp.text = WITH_NEXT_HTML
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    from scraper import discover_pages
    assert discover_pages("https://quotes.toscrape.com", 5) == 5


@patch("scraper.requests.Session.get")
def test_discover_pages_no_next(mock_get):
    mock_resp = MagicMock()
    mock_resp.text = NO_NEXT_HTML
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    from scraper import discover_pages
    assert discover_pages("https://quotes.toscrape.com", 5) == 1


@patch("scraper.requests.Session.get")
def test_discover_pages_request_error_defaults_to_max(mock_get):
    import requests
    mock_get.side_effect = requests.RequestException("timeout")

    from scraper import discover_pages
    assert discover_pages("https://quotes.toscrape.com", 7) == 7
