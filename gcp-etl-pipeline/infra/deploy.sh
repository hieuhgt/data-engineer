#!/usr/bin/env bash
# deploy.sh — Full GCP ETL pipeline deployment
# Usage: PROJECT_ID=my-project ./infra/deploy.sh
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID env var}"
REGION="${REGION:-us-central1}"
BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-etl-pipeline}"
QUEUE_NAME="${QUEUE_NAME:-etl-scrape-queue}"
AR_REPO="${AR_REPO:-etl-pipeline}"                                        # Artifact Registry repo
SCRAPER_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/etl-scraper"
SCRAPER_SERVICE="etl-scraper"
FUNCTION_NAME="etl-process-page"
WORKFLOW_NAME="etl-pipeline"
SCHEDULER_JOB="etl-pipeline-trigger"
SA_NAME="etl-pipeline-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
SCRAPE_BASE_URL="https://quotes.toscrape.com"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log() { echo "[deploy] $*"; }

# ── 1. Enable APIs ─────────────────────────────────────────────────────────────
log "Enabling GCP APIs..."
gcloud services enable \
  run.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudtasks.googleapis.com \
  workflows.googleapis.com \
  cloudscheduler.googleapis.com \
  storage.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  --project="${PROJECT_ID}"

# ── 2. Service account ────────────────────────────────────────────────────────
log "Creating service account ${SA_EMAIL}..."
gcloud iam service-accounts create "${SA_NAME}" \
  --display-name="ETL Pipeline SA" \
  --project="${PROJECT_ID}" 2>/dev/null || log "SA already exists, skipping."

for role in \
  roles/cloudtasks.enqueuer \
  roles/cloudfunctions.invoker \
  roles/run.invoker \
  roles/storage.objectAdmin \
  roles/workflows.invoker \
  roles/artifactregistry.writer; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${role}" \
    --condition=None \
    --quiet
done

# ── 3. GCS bucket with medallion folders ─────────────────────────────────────
log "Creating GCS bucket gs://${BUCKET_NAME}..."
gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "gs://${BUCKET_NAME}" 2>/dev/null || \
  log "Bucket already exists."

# Lifecycle: move bronze→nearline after 30d, delete after 365d
cat > /tmp/lifecycle.json <<'EOF'
{
  "rule": [
    {
      "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
      "condition": {"age": 30, "matchesPrefix": ["bronze/"]}
    },
    {
      "action": {"type": "Delete"},
      "condition": {"age": 365}
    }
  ]
}
EOF
gsutil lifecycle set /tmp/lifecycle.json "gs://${BUCKET_NAME}"

# ── 4. Cloud Tasks queue ──────────────────────────────────────────────────────
log "Creating Cloud Tasks queue ${QUEUE_NAME}..."
gcloud tasks queues create "${QUEUE_NAME}" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" \
  --max-concurrent-dispatches=5 \
  --max-attempts=3 \
  --min-backoff=10s \
  --max-backoff=300s \
  2>/dev/null || log "Queue already exists."

# ── 5. Deploy Cloud Function ──────────────────────────────────────────────────
log "Deploying Cloud Function ${FUNCTION_NAME}..."
gcloud functions deploy "${FUNCTION_NAME}" \
  --gen2 \
  --runtime=python311 \
  --region="${REGION}" \
  --source="${ROOT_DIR}/functions/processor" \
  --entry-point=process_page \
  --trigger-http \
  --service-account="${SA_EMAIL}" \
  --set-env-vars="GCS_BUCKET_NAME=${BUCKET_NAME},SCRAPE_BASE_URL=${SCRAPE_BASE_URL}" \
  --memory=512Mi \
  --timeout=120s \
  --min-instances=0 \
  --max-instances=20 \
  --ingress-settings=internal-and-gclb \
  --no-allow-unauthenticated \
  --project="${PROJECT_ID}"

FUNCTION_URL=$(gcloud functions describe "${FUNCTION_NAME}" \
  --gen2 --region="${REGION}" --project="${PROJECT_ID}" \
  --format="value(serviceConfig.uri)")
log "Function URL: ${FUNCTION_URL}"

# ── 6. Artifact Registry repo + Docker build/push ────────────────────────────
log "Creating Artifact Registry repository '${AR_REPO}'..."
gcloud artifacts repositories create "${AR_REPO}" \
  --repository-format=docker \
  --location="${REGION}" \
  --description="ETL Pipeline Docker images" \
  --project="${PROJECT_ID}" 2>/dev/null || log "Repository already exists."

log "Configuring Docker auth for Artifact Registry..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

log "Building Docker image..."
docker build -t "${SCRAPER_IMAGE}:latest" -t "${SCRAPER_IMAGE}:$(git -C "${ROOT_DIR}" rev-parse --short HEAD 2>/dev/null || echo 'manual')" \
  "${ROOT_DIR}/scraper"

log "Pushing image to Artifact Registry..."
docker push "${SCRAPER_IMAGE}:latest"
docker push "${SCRAPER_IMAGE}:$(git -C "${ROOT_DIR}" rev-parse --short HEAD 2>/dev/null || echo 'manual')"

log "Deploying Cloud Run service ${SCRAPER_SERVICE}..."
gcloud run deploy "${SCRAPER_SERVICE}" \
  --image="${SCRAPER_IMAGE}:latest" \
  --platform=managed \
  --region="${REGION}" \
  --service-account="${SA_EMAIL}" \
  --no-allow-unauthenticated \
  --set-env-vars="\
GCP_PROJECT_ID=${PROJECT_ID},\
GCP_REGION=${REGION},\
TASK_QUEUE_NAME=${QUEUE_NAME},\
FUNCTION_URL=${FUNCTION_URL},\
PIPELINE_SA_EMAIL=${SA_EMAIL},\
SCRAPE_BASE_URL=${SCRAPE_BASE_URL},\
MAX_PAGES=10" \
  --memory=512Mi \
  --cpu=1 \
  --timeout=120 \
  --min-instances=0 \
  --max-instances=3 \
  --project="${PROJECT_ID}"

CLOUD_RUN_URL=$(gcloud run services describe "${SCRAPER_SERVICE}" \
  --platform=managed --region="${REGION}" --project="${PROJECT_ID}" \
  --format="value(status.url)")
log "Cloud Run URL: ${CLOUD_RUN_URL}"

# ── 7. Deploy Cloud Workflow ───────────────────────────────────────────────────
log "Deploying Cloud Workflow ${WORKFLOW_NAME}..."
gcloud workflows deploy "${WORKFLOW_NAME}" \
  --location="${REGION}" \
  --source="${ROOT_DIR}/workflows/etl_pipeline.yaml" \
  --service-account="${SA_EMAIL}" \
  --project="${PROJECT_ID}"

# ── 8. Cloud Scheduler (every 6 hours) ───────────────────────────────────────
log "Creating Cloud Scheduler job ${SCHEDULER_JOB}..."
WORKFLOW_ARGS=$(printf '{"cloud_run_url":"%s","max_pages":10}' "${CLOUD_RUN_URL}")

gcloud scheduler jobs delete "${SCHEDULER_JOB}" \
  --location="${REGION}" --project="${PROJECT_ID}" --quiet 2>/dev/null || true

gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
  --location="${REGION}" \
  --schedule="0 */6 * * *" \
  --time-zone="UTC" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/workflows/${WORKFLOW_NAME}/executions" \
  --message-body="{\"argument\": \"${WORKFLOW_ARGS}\"}" \
  --oauth-service-account-email="${SA_EMAIL}" \
  --project="${PROJECT_ID}"

log ""
log "╔══════════════════════════════════════════════════════════╗"
log "║  Deployment complete!                                    ║"
log "╠══════════════════════════════════════════════════════════╣"
log "║  Cloud Run:      ${CLOUD_RUN_URL}"
log "║  Cloud Function: ${FUNCTION_URL}"
log "║  GCS bucket:     gs://${BUCKET_NAME}"
log "║  Schedule:       every 6 hours (UTC)"
log "╚══════════════════════════════════════════════════════════╝"
log ""
log "To trigger manually:"
log "  gcloud workflows run ${WORKFLOW_NAME} \\"
log "    --location=${REGION} \\"
log "    --data='{\"cloud_run_url\":\"${CLOUD_RUN_URL}\",\"max_pages\":3}' \\"
log "    --project=${PROJECT_ID}"
