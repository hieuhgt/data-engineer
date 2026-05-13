#!/usr/bin/env bash
# setup_wif.sh — One-time setup for Workload Identity Federation + Artifact Registry.
# Run this ONCE before using the GitHub Actions workflow.
# After this runs, add the printed values as GitHub repository variables.
#
# Usage:
#   PROJECT_ID=my-project GITHUB_REPO=owner/repo ./infra/setup_wif.sh
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID}"
GITHUB_REPO="${GITHUB_REPO:?Set GITHUB_REPO as owner/repo}"
REGION="${REGION:-us-central1}"
SA_NAME="${SA_NAME:-etl-pipeline-sa}"
POOL_NAME="github-pool"
PROVIDER_NAME="github-provider"
AR_REPO="etl-pipeline"

SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")

log() { echo ""; echo "▶ $*"; }

# ── Artifact Registry ─────────────────────────────────────────────────────────
log "Creating Artifact Registry repository '${AR_REPO}'..."
gcloud artifacts repositories create "${AR_REPO}" \
  --repository-format=docker \
  --location="${REGION}" \
  --description="ETL Pipeline Docker images" \
  --project="${PROJECT_ID}" 2>/dev/null || echo "  Already exists."

# ── Workload Identity Pool ────────────────────────────────────────────────────
log "Creating Workload Identity Pool '${POOL_NAME}'..."
gcloud iam workload-identity-pools create "${POOL_NAME}" \
  --location=global \
  --display-name="GitHub Actions Pool" \
  --description="Allow GitHub Actions to authenticate via WIF" \
  --project="${PROJECT_ID}" 2>/dev/null || echo "  Already exists."

# ── Workload Identity Provider (GitHub OIDC) ──────────────────────────────────
log "Creating OIDC provider '${PROVIDER_NAME}' for GitHub repo '${GITHUB_REPO}'..."
gcloud iam workload-identity-pools providers create-oidc "${PROVIDER_NAME}" \
  --workload-identity-pool="${POOL_NAME}" \
  --location=global \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="\
google.subject=assertion.sub,\
attribute.actor=assertion.actor,\
attribute.repository=assertion.repository,\
attribute.repository_owner=assertion.repository_owner" \
  --attribute-condition="assertion.repository=='${GITHUB_REPO}'" \
  --display-name="GitHub OIDC" \
  --project="${PROJECT_ID}" 2>/dev/null || echo "  Already exists."

# ── Bind SA to WIF pool ───────────────────────────────────────────────────────
log "Binding service account to Workload Identity Pool..."
gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_NAME}/attribute.repository/${GITHUB_REPO}" \
  --project="${PROJECT_ID}"

# ── Grant SA Artifact Registry write ─────────────────────────────────────────
log "Granting SA write access to Artifact Registry..."
gcloud artifacts repositories add-iam-policy-binding "${AR_REPO}" \
  --location="${REGION}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/artifactregistry.writer" \
  --project="${PROJECT_ID}"

# ── Print GitHub variable values ──────────────────────────────────────────────
WIF_PROVIDER="projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_NAME}/providers/${PROVIDER_NAME}"

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  Setup complete! Add these as GitHub repository variables:"
echo "  (Settings → Secrets and variables → Actions → Variables)"
echo "══════════════════════════════════════════════════════════════"
echo ""
echo "  GCP_PROJECT_ID      = ${PROJECT_ID}"
echo "  GCP_REGION          = ${REGION}"
echo "  WIF_PROVIDER        = ${WIF_PROVIDER}"
echo "  PIPELINE_SA_EMAIL   = ${SA_EMAIL}"
echo "  ARTIFACT_REPO       = ${AR_REPO}"
echo "  TASK_QUEUE_NAME     = etl-scrape-queue"
echo "  GCS_BUCKET_NAME     = ${PROJECT_ID}-etl-pipeline"
echo "  SCRAPE_BASE_URL     = https://quotes.toscrape.com"
echo "  MAX_PAGES           = 10"
echo "  FUNCTION_URL        = (set this after the first Cloud Function deploy)"
echo ""
echo "  Quick copy for gh CLI:"
echo "  gh variable set GCP_PROJECT_ID       --body '${PROJECT_ID}'"
echo "  gh variable set GCP_REGION           --body '${REGION}'"
echo "  gh variable set WIF_PROVIDER         --body '${WIF_PROVIDER}'"
echo "  gh variable set PIPELINE_SA_EMAIL    --body '${SA_EMAIL}'"
echo "  gh variable set ARTIFACT_REPO        --body '${AR_REPO}'"
echo "  gh variable set TASK_QUEUE_NAME      --body 'etl-scrape-queue'"
echo "  gh variable set GCS_BUCKET_NAME      --body '${PROJECT_ID}-etl-pipeline'"
echo "  gh variable set SCRAPE_BASE_URL      --body 'https://quotes.toscrape.com'"
echo "  gh variable set MAX_PAGES            --body '10'"
echo ""
