"""
Cloud Function: process_page
Triggered by Cloud Tasks (HTTP POST).
Implements the medallion pattern:
  bronze/ — raw scraped records, one JSON file per page
  silver/ — cleaned + schema-enforced records (CSV + JSON)
  gold/   — aggregated summaries (tag counts, author stats)
"""

import os
import io
import csv
import json
import time
import logging
import collections
import requests
from bs4 import BeautifulSoup
import functions_framework
from flask import Request
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "")
BASE_URL = os.environ.get("SCRAPE_BASE_URL", "https://quotes.toscrape.com")

# Lazy — initialised on first write so tests can import without GCP credentials
_storage: storage.Client | None = None
_session = requests.Session()
_session.headers.update({"User-Agent": "ETL-Pipeline/1.0"})


def _get_storage() -> storage.Client:
    global _storage
    if _storage is None:
        _storage = storage.Client()
    return _storage

# ── Entry point ──────────────────────────────────────────────────────────────

@functions_framework.http
def process_page(request: Request):
    task_name = request.headers.get("X-CloudTasks-TaskName", "local-test")
    logger.info("Task received: %s", task_name)

    payload = request.get_json(silent=True)
    if not payload or "page" not in payload:
        return ("Missing required field: page", 400)

    page = int(payload["page"])
    date_prefix = time.strftime("%Y/%m/%d")

    try:
        raw_records = _scrape(page)
        if not raw_records:
            logger.warning("Page %d returned no records", page)
            return (f"No data on page {page}", 204)

        # Bronze — save raw records as-is
        bronze_path = f"bronze/quotes/{date_prefix}/page_{page:03d}.json"
        _write_json(raw_records, bronze_path)

        # Silver — clean, normalize, enforce schema
        silver_records = [_clean(r) for r in raw_records]
        silver_json = f"silver/quotes/{date_prefix}/page_{page:03d}.json"
        silver_csv = f"silver/quotes/{date_prefix}/page_{page:03d}.csv"
        _write_json(silver_records, silver_json)
        _write_csv(silver_records, silver_csv, fieldnames=["id", "text", "author", "tags", "page", "scraped_at"])

        # Gold — aggregated stats for this page
        gold_path = f"gold/quotes/{date_prefix}/page_{page:03d}_stats.json"
        _write_json(_aggregate(silver_records, page), gold_path)

        logger.info("Page %d: %d records → bronze/silver/gold written", page, len(raw_records))
        return (
            json.dumps({
                "page": page,
                "records": len(raw_records),
                "bronze": bronze_path,
                "silver_json": silver_json,
                "silver_csv": silver_csv,
                "gold": gold_path,
            }),
            200,
            {"Content-Type": "application/json"},
        )

    except requests.HTTPError as e:
        logger.error("HTTP error scraping page %d: %s", page, e)
        return (str(e), 502, {"Content-Type": "text/plain"})
    except Exception:
        logger.exception("Unexpected error on page %d", page)
        return ("Internal error", 500, {"Content-Type": "text/plain"})


# ── Scraping ─────────────────────────────────────────────────────────────────

def _scrape(page: int) -> list[dict]:
    """Fetch raw records from the source. Bronze — no transformation."""
    url = f"{BASE_URL}/page/{page}/"
    resp = _session.get(url, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    records = []
    for div in soup.find_all("div", class_="quote"):
        text_el = div.find("span", class_="text")
        author_el = div.find("small", class_="author")
        bio_link = div.find("a", href=lambda h: h and "/author/" in h)
        if not text_el or not author_el:
            continue
        records.append({
            "text": text_el.get_text(strip=True),
            "author": author_el.get_text(strip=True),
            "author_bio_url": (BASE_URL + bio_link["href"]) if bio_link else None,
            "tags": [t.get_text(strip=True) for t in div.find_all("a", class_="tag")],
            "page": page,
            "source_url": url,
            "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
    return records


# ── Silver — clean & normalize ────────────────────────────────────────────────

_QUOTE_CHARS = {"“", "”", "‘", "’"}

def _clean(raw: dict) -> dict:
    """Silver layer: strip curly quotes, normalize whitespace, add a stable id."""
    text = raw["text"]
    for ch in _QUOTE_CHARS:
        text = text.replace(ch, "")
    text = " ".join(text.split())

    author = " ".join(raw["author"].split())
    tags = sorted({t.lower().strip() for t in raw.get("tags", []) if t.strip()})

    stable_id = f"p{raw['page']:03d}-{abs(hash(text + author)) % 10**8:08d}"

    return {
        "id": stable_id,
        "text": text,
        "author": author,
        "tags": tags,
        "page": raw["page"],
        "scraped_at": raw["scraped_at"],
    }


# ── Gold — aggregate ──────────────────────────────────────────────────────────

def _aggregate(records: list[dict], page: int) -> dict:
    """Gold layer: per-page summary — tag frequencies, author counts."""
    tag_counter: dict[str, int] = collections.Counter()
    author_counter: dict[str, int] = collections.Counter()

    for r in records:
        author_counter[r["author"]] += 1
        for tag in r["tags"]:
            tag_counter[tag] += 1

    return {
        "page": page,
        "total_quotes": len(records),
        "unique_authors": len(author_counter),
        "top_authors": author_counter.most_common(5),
        "top_tags": tag_counter.most_common(10),
        "aggregated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


# ── GCS helpers ───────────────────────────────────────────────────────────────

def _write_json(data: object, gcs_path: str) -> None:
    bucket = _get_storage().bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json",
    )
    logger.debug("Written: gs://%s/%s", BUCKET_NAME, gcs_path)


def _write_csv(records: list[dict], gcs_path: str, fieldnames: list[str]) -> None:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in records:
        writer.writerow({**row, "tags": "|".join(row.get("tags", []))})

    bucket = _get_storage().bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(buf.getvalue(), content_type="text/csv")
    logger.debug("Written: gs://%s/%s", BUCKET_NAME, gcs_path)
