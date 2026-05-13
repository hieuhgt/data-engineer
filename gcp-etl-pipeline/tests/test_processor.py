"""Unit tests for the Cloud Function processor (medallion layers)."""
import json
from unittest.mock import patch, MagicMock

SAMPLE_HTML = """
<html><body>
  <div class="quote">
    <span class="text">“Be the change”</span>
    <small class="author">Gandhi</small>
    <div class="tags">
      <a class="tag">change</a>
      <a class="tag">inspirational</a>
    </div>
    <a href="/author/Gandhi">About</a>
  </div>
  <div class="quote">
    <span class="text">“Simplicity”</span>
    <small class="author">Gandhi</small>
    <div class="tags"><a class="tag">simple</a></div>
    <a href="/author/Gandhi">About</a>
  </div>
</body></html>
"""


@patch("main.requests.Session.get")
def test_scrape_returns_raw_records(mock_get):
    mock_resp = MagicMock()
    mock_resp.text = SAMPLE_HTML
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    from main import _scrape
    records = _scrape(1)

    assert len(records) == 2
    assert all(r["page"] == 1 for r in records)
    assert all("scraped_at" in r for r in records)
    assert all("source_url" in r for r in records)


def test_clean_strips_curly_quotes():
    from main import _clean
    raw = {
        "text": "“Hello world”",
        "author": "  Jane  Doe  ",
        "tags": ["Python", "Python", "code"],
        "page": 1,
        "scraped_at": "2024-01-01T00:00:00Z",
    }
    cleaned = _clean(raw)

    assert "“" not in cleaned["text"]
    assert "”" not in cleaned["text"]
    assert cleaned["author"] == "Jane Doe"
    assert cleaned["tags"] == ["code", "python"]  # deduped, sorted, lowercased
    assert "id" in cleaned


def test_aggregate_counts_correctly():
    from main import _aggregate
    records = [
        {"author": "Gandhi", "tags": ["change", "inspirational"]},
        {"author": "Gandhi", "tags": ["simple"]},
        {"author": "Einstein", "tags": ["change", "science"]},
    ]
    result = _aggregate(records, page=1)

    assert result["total_quotes"] == 3
    assert result["unique_authors"] == 2
    top_tags = dict(result["top_tags"])
    assert top_tags["change"] == 2
    assert result["page"] == 1


@patch("main._get_storage")
@patch("main.requests.Session.get")
def test_process_page_full_flow(mock_get, mock_get_storage):
    mock_resp = MagicMock()
    mock_resp.text = SAMPLE_HTML
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    mock_storage = MagicMock()
    mock_get_storage.return_value = mock_storage
    mock_bucket = MagicMock()
    mock_storage.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = MagicMock()

    from main import process_page
    request = MagicMock()
    request.headers = {"X-CloudTasks-TaskName": "test-task"}
    request.get_json.return_value = {"page": 1}

    body, status, _ = process_page(request)
    result = json.loads(body)

    assert status == 200
    assert result["records"] == 2
    assert result["bronze"].startswith("bronze/")
    assert result["silver_json"].startswith("silver/")
    assert result["silver_csv"].startswith("silver/")
    assert result["gold"].startswith("gold/")
