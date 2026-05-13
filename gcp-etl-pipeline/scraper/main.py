import os
import sys
import logging
from flask import Flask, request, jsonify
from scraper import discover_pages
from task_manager import create_scrape_tasks

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
REGION = os.environ.get("GCP_REGION", "us-central1")
QUEUE_NAME = os.environ.get("TASK_QUEUE_NAME", "etl-scrape-queue")
FUNCTION_URL = os.environ["FUNCTION_URL"]          # Cloud Function HTTP URL
SA_EMAIL = os.environ["PIPELINE_SA_EMAIL"]         # Service account for OIDC
BASE_URL = os.environ.get("SCRAPE_BASE_URL", "https://quotes.toscrape.com")
DEFAULT_MAX_PAGES = int(os.environ.get("MAX_PAGES", "10"))


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.post("/start-pipeline")
def start_pipeline():
    body = request.get_json(silent=True) or {}
    max_pages = int(body.get("max_pages", DEFAULT_MAX_PAGES))

    logger.info("Pipeline start requested — max_pages=%d", max_pages)

    total_pages = discover_pages(BASE_URL, max_pages)
    pages = list(range(1, total_pages + 1))

    task_names = create_scrape_tasks(
        project_id=PROJECT_ID,
        region=REGION,
        queue_name=QUEUE_NAME,
        function_url=FUNCTION_URL,
        sa_email=SA_EMAIL,
        pages=pages,
    )

    logger.info("Queued %d tasks", len(task_names))
    return jsonify({
        "status": "started",
        "pages_queued": len(pages),
        "task_count": len(task_names),
    }), 202


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
