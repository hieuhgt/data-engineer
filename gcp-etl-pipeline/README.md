# GCP ETL Pipeline

Serverless web-scraping ETL pipeline on GCP. Scrapes [quotes.toscrape.com](https://quotes.toscrape.com), fans out work across Cloud Tasks, and writes output to Cloud Storage using the **Bronze / Silver / Gold** medallion pattern.

---

## Full Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│  TRIGGER                                                            │
│                                                                     │
│  Cloud Scheduler  (cron: 0 */6 * * *  — every 6 hours)            │
│       │                                                             │
│       │  POST /v1/.../workflows/etl-pipeline/executions            │
│       ▼                                                             │
│  Cloud Workflows  (etl_pipeline.yaml)                              │
│       │  1. Validates args (cloud_run_url required)                │
│       │  2. Logs run_id + max_pages                                │
│       │  3. POST /start-pipeline  (OIDC auth)                      │
│       │  4. Checks HTTP 202 — raises on any other status           │
│       │  5. Returns { run_id, pages_queued, task_count }           │
└───────┼─────────────────────────────────────────────────────────────┘
        │
        │  POST /start-pipeline  {"max_pages": 10}
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ORCHESTRATION                                                      │
│                                                                     │
│  Cloud Run  — etl-scraper  (Docker / Flask)                        │
│       │  scraper.py: GET /page/1/ → checks for "next" button       │
│       │              → returns total page count (capped at max)     │
│       │  task_manager.py: creates one Cloud Task per page          │
│       │              → tasks staggered 2 s apart (rate-limit safe) │
│       │              → each task carries OIDC token for auth       │
│       │  Returns 202 { pages_queued, task_count }                  │
└───────┼─────────────────────────────────────────────────────────────┘
        │
        │  Cloud Tasks queue  (etl-scrape-queue)
        │  max 5 concurrent dispatches · 3 retry attempts
        │  min backoff 10 s · max backoff 300 s
        │
        │  POST  {"page": 1}   →  Cloud Function
        │  POST  {"page": 2}   →  Cloud Function   (parallel)
        │  POST  {"page": N}   →  Cloud Function
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PROCESSING  (one Cloud Function invocation per page)               │
│                                                                     │
│  Cloud Function  — etl-process-page  (Gen 2 / Python)             │
│       │                                                             │
│       │  1. Validate X-CloudTasks-TaskName header                  │
│       │  2. Scrape page N from quotes.toscrape.com                 │
│       │     - find all <div class="quote">                         │
│       │     - extract: text, author, author_bio_url, tags          │
│       │                                                             │
│       │  ── BRONZE ──────────────────────────────────────────────  │
│       │  3. Save raw records as-is (no transformation)             │
│       │     gs://BUCKET/bronze/quotes/YYYY/MM/DD/page_NNN.json     │
│       │                                                             │
│       │  ── SILVER ──────────────────────────────────────────────  │
│       │  4. Clean each record:                                      │
│       │     - strip curly quotes (" " ' ')                         │
│       │     - normalise whitespace                                  │
│       │     - deduplicate + sort tags (lowercased)                 │
│       │     - generate stable ID: p001-<hash>                      │
│       │  5. Save cleaned records                                    │
│       │     gs://BUCKET/silver/quotes/YYYY/MM/DD/page_NNN.json     │
│       │     gs://BUCKET/silver/quotes/YYYY/MM/DD/page_NNN.csv      │
│       │                                                             │
│       │  ── GOLD ────────────────────────────────────────────────  │
│       │  6. Aggregate per page:                                     │
│       │     - total_quotes, unique_authors                         │
│       │     - top 5 authors, top 10 tags (with counts)             │
│       │  7. Save stats                                              │
│       │     gs://BUCKET/gold/quotes/YYYY/MM/DD/page_NNN_stats.json │
│       │                                                             │
│       │  Returns 200 { page, records, bronze, silver_*, gold }     │
└───────┼─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STORAGE                                                            │
│                                                                     │
│  Cloud Storage  (gs://<PROJECT>-etl-pipeline)                      │
│                                                                     │
│  bronze/quotes/2024/01/15/                                         │
│    page_001.json   ← raw, exactly as scraped                       │
│    page_002.json                                                    │
│                                                                     │
│  silver/quotes/2024/01/15/                                         │
│    page_001.json   ← cleaned JSON (schema enforced)                │
│    page_001.csv    ← cleaned CSV  (tags joined with |)             │
│    page_002.json                                                    │
│    page_002.csv                                                     │
│                                                                     │
│  gold/quotes/2024/01/15/                                           │
│    page_001_stats.json  ← aggregated: top authors, top tags        │
│    page_002_stats.json                                              │
│                                                                     │
│  Lifecycle rules:                                                   │
│    bronze/ → Nearline storage after 30 days                        │
│    all     → deleted after 365 days                                │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Medallion Layers

| Layer | GCS path | Format | What changed from previous layer |
|-------|----------|--------|----------------------------------|
| **Bronze** | `bronze/quotes/YYYY/MM/DD/page_NNN.json` | JSON | Nothing — raw output of the scraper |
| **Silver** | `silver/quotes/YYYY/MM/DD/page_NNN.json` | JSON | Curly quotes stripped, whitespace normalised, tags deduped/sorted/lowercased, stable `id` added |
| **Silver** | `silver/quotes/YYYY/MM/DD/page_NNN.csv` | CSV | Same as silver JSON; `tags` column is pipe-separated |
| **Gold** | `gold/quotes/YYYY/MM/DD/page_NNN_stats.json` | JSON | Aggregated: `total_quotes`, `unique_authors`, `top_authors[5]`, `top_tags[10]` |

---

## CI/CD Flow (GitHub Actions)

Push to `main` → automatic deploy. The workflow lives at `.github/workflows/deploy-etl.yml`.

```
git push origin main
        │
        ▼
[1] test
    pytest tests/ -v
        │  (fails fast — nothing deploys if tests fail)
        │
        ├─────────────────────────────────┐
        ▼                                 ▼
[2] build-push                     [4] deploy-function
    WIF keyless auth → GCP              WIF keyless auth → GCP
    docker build ./scraper              gcloud functions deploy etl-process-page
    docker push → Artifact Registry         --source=functions/processor
      REGION-docker.pkg.dev/              (source upload — no Docker needed)
        PROJECT/etl-pipeline/
        etl-scraper:<git-sha>
        │
        ▼
[3] deploy-scraper
    gcloud run deploy etl-scraper
      --image=<AR image from step 2>
    (no checkout — image already in AR)
        │
        └─────────────┬───────────────────┘
                      ▼
             [5] deploy-workflow
                 gcloud workflows deploy etl-pipeline
                   --source=workflows/etl_pipeline.yaml
```

**Authentication:** Workload Identity Federation — no JSON service account keys stored in GitHub. GitHub Actions proves identity via OIDC and receives a short-lived GCP token.

---

## Project Structure

```
gcp-etl-pipeline/
│
├── scraper/                        Cloud Run — orchestration
│   ├── main.py                     Flask app: POST /start-pipeline
│   ├── scraper.py                  Discovers total page count
│   ├── task_manager.py             Creates Cloud Tasks (one per page)
│   ├── Dockerfile                  python:3.11-slim + gunicorn
│   ├── .dockerignore
│   └── requirements.txt            Deployment artifact (Docker)
│
├── functions/
│   └── processor/                  Cloud Function — per-page ETL
│       ├── main.py                 Scrape → Bronze → Silver → Gold → GCS
│       └── requirements.txt        Deployment artifact (gcloud functions deploy)
│
├── workflows/
│   └── etl_pipeline.yaml           Cloud Workflows definition
│
├── .github/
│   └── workflows/
│       └── deploy-etl.yml          GitHub Actions CI/CD
│
├── infra/
│   ├── setup_wif.sh                One-time: Workload Identity + Artifact Registry
│   ├── deploy.sh                   One-time: full manual deployment
│   └── teardown.sh                 Removes all GCP resources
│
├── tests/
│   ├── conftest.py                 sys.path setup, functions_framework mock
│   ├── test_scraper.py             Tests for discover_pages
│   └── test_processor.py          Tests for _scrape, _clean, _aggregate, process_page
│
├── requirements.txt                All deps — use this for local dev + venv
├── .env.example                    Environment variable reference
└── .gitignore
```

---

## Setup

### Prerequisites

- GCP project with billing enabled
- `gcloud` CLI: `gcloud auth login && gcloud config set project PROJECT_ID`
- Docker (for manual deploy)
- Python 3.11+

### 1. One-time infrastructure (run once, ever)

```bash
# Creates: Artifact Registry repo, Workload Identity Pool + Provider,
# IAM bindings. Prints GitHub variable values to copy.
PROJECT_ID=your-project PROJECT_ID=your-project-id \
GITHUB_REPO=owner/repo \
./infra/setup_wif.sh
```

Then add the printed values as **GitHub repository variables** (Settings → Secrets and variables → Actions → Variables):

| Variable | Description |
|----------|-------------|
| `GCP_PROJECT_ID` | Your GCP project ID |
| `GCP_REGION` | e.g. `us-central1` |
| `WIF_PROVIDER` | Printed by `setup_wif.sh` |
| `PIPELINE_SA_EMAIL` | `etl-pipeline-sa@PROJECT.iam.gserviceaccount.com` |
| `TASK_QUEUE_NAME` | `etl-scrape-queue` |
| `GCS_BUCKET_NAME` | `PROJECT-etl-pipeline` |
| `SCRAPE_BASE_URL` | `https://quotes.toscrape.com` |
| `MAX_PAGES` | `10` |
| `FUNCTION_URL` | Set this after the first Cloud Function deploy |

### 2. First deploy (manual, via script)

```bash
PROJECT_ID=your-project-id ./infra/deploy.sh
```

This handles everything in order: APIs, service account, GCS bucket + lifecycle rules, Cloud Tasks queue, Cloud Function, Artifact Registry, Docker build + push, Cloud Run, Cloud Workflow, Cloud Scheduler.

### 3. All subsequent deploys

```bash
git push origin main
# GitHub Actions runs automatically
```

---

## Local Development

### Virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run tests

```bash
pytest tests/ -v
```

### Test the Cloud Function locally

```bash
# functions_framework serves the function on localhost:8080
GCS_BUCKET_NAME=local-test \
functions-framework --target=process_page --source=functions/processor --debug

# In another terminal:
curl -X POST localhost:8080 \
  -H "Content-Type: application/json" \
  -d '{"page": 1}'
```

### Test the Cloud Run scraper locally

```bash
GCP_PROJECT_ID=test \
FUNCTION_URL=http://localhost:8081 \
PIPELINE_SA_EMAIL=test@example.com \
python scraper/main.py

curl -X POST localhost:8080/start-pipeline \
  -H "Content-Type: application/json" \
  -d '{"max_pages": 2}'
```

---

## Manual Pipeline Trigger

```bash
# Run the workflow directly, bypassing the scheduler
gcloud workflows run etl-pipeline \
  --location=us-central1 \
  --data='{"cloud_run_url":"https://YOUR-CLOUD-RUN-URL","max_pages":3}' \
  --project=YOUR_PROJECT_ID
```

To find your Cloud Run URL:
```bash
gcloud run services describe etl-scraper \
  --platform=managed --region=us-central1 \
  --format="value(status.url)"
```

---

## Environment Variables

| Variable | Used by | Required | Description |
|----------|---------|----------|-------------|
| `GCP_PROJECT_ID` | Cloud Run | yes | GCP project ID |
| `GCP_REGION` | Cloud Run | no | Default: `us-central1` |
| `TASK_QUEUE_NAME` | Cloud Run | no | Default: `etl-scrape-queue` |
| `FUNCTION_URL` | Cloud Run | yes | Cloud Function HTTP trigger URL |
| `PIPELINE_SA_EMAIL` | Cloud Run | yes | Service account for OIDC tokens |
| `SCRAPE_BASE_URL` | Cloud Run + Function | no | Default: `https://quotes.toscrape.com` |
| `MAX_PAGES` | Cloud Run | no | Default: `10` |
| `GCS_BUCKET_NAME` | Cloud Function | yes | Target GCS bucket |

---

## Cost Estimate

Based on 10 pages per run, 4 runs per day (~1,200 pages/month).

| Service | Usage | Est. cost/month |
|---------|-------|----------------|
| Cloud Scheduler | 1 job | ~$0.10 |
| Cloud Workflows | ~120 executions | free tier |
| Cloud Run | ~120 invocations, 512 MB, <5 s each | ~$0.01 |
| Cloud Tasks | ~1,200 tasks | free tier |
| Cloud Functions | ~1,200 invocations, 512 MB, ~10 s each | ~$0.05 |
| Artifact Registry | ~1 image, <1 GB | ~$0.10 |
| Cloud Storage | <1 GB | ~$0.02 |
| **Total** | | **~$0.28/month** |

---

## Teardown

```bash
# Removes all GCP resources. Does NOT delete the GCS bucket (keeps your data).
PROJECT_ID=your-project-id ./infra/teardown.sh

# To also delete the data:
gsutil rm -r gs://your-project-id-etl-pipeline
```
