import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "ETL-Pipeline/1.0"})


def discover_pages(base_url: str, max_pages: int) -> int:
    """Return the number of pages available (capped at max_pages)."""
    try:
        resp = _SESSION.get(f"{base_url}/page/1/", timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        if not soup.find("li", class_="next"):
            return 1
    except requests.RequestException as e:
        logger.warning("Could not probe page count: %s — defaulting to max", e)
    return max_pages
