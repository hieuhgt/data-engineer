#!/usr/bin/env bash
# teardown.sh — Remove all pipeline resources
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID env var}"
REGION="${REGION:-us-central1}"
BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-etl-pipeline}"

gcloud scheduler jobs delete etl-pipeline-trigger --location="${REGION}" --project="${PROJECT_ID}" --quiet || true
gcloud workflows delete etl-pipeline --location="${REGION}" --project="${PROJECT_ID}" --quiet || true
gcloud run services delete etl-scraper --platform=managed --region="${REGION}" --project="${PROJECT_ID}" --quiet || true
gcloud functions delete etl-process-page --gen2 --region="${REGION}" --project="${PROJECT_ID}" --quiet || true
gcloud tasks queues delete etl-scrape-queue --location="${REGION}" --project="${PROJECT_ID}" --quiet || true

echo "Resources removed. Bucket gs://${BUCKET_NAME} was NOT deleted (contains data)."
echo "To delete: gsutil rm -r gs://${BUCKET_NAME}"
