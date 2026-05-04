# Production Data Pipeline

A production-grade data pipeline: ingests 500 GB/day from 20 sources, transforms with Spark, orchestrates with Airflow, stores in MinIO (S3-compatible), and exposes metrics via Prometheus + Grafana.

**Stack**: Airflow 3 · Spark 4 · Kafka (KRaft) · PostgreSQL 18 · MinIO · Prometheus · Grafana  
**SLA**: 4-hour batch window  
**Pattern**: Lambda (batch + streaming)

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Docker Desktop | 4.x+ | https://docs.docker.com/desktop/ |
| mise | latest | `brew install mise` |
| Poetry | 1.8+ | `pipx install poetry` |

---

## Running the stack

### 1. Set up Python

```bash
# Install Python 3.12 (required — stack is not compatible with 3.13/3.14)
mise install python@3.12
mise local python 3.12

# Verify
python --version   # Python 3.12.x
```

### 2. Install Python dependencies

```bash
# Core + dev tools
poetry install --with dev

# Optional: Airflow operators/providers (for IDE type checking)
poetry install --with airflow

# Point your IDE to the venv
poetry env info --path   # copy this path → set as Python interpreter
```

### 3. Start all services

```bash
docker compose up -d
```

Wait ~30 seconds for health checks to pass, then verify:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Expected output — all services `healthy` (spark-worker and monitoring show `Up`):

```
pipeline-airflow-webserver   Up ... (healthy)
pipeline-airflow-scheduler   Up ... (healthy)
pipeline-postgres            Up ... (healthy)
pipeline-kafka               Up ... (healthy)
pipeline-minio               Up ... (healthy)
pipeline-spark-master        Up ... (healthy)
pipeline-spark-worker        Up ...
pipeline-prometheus          Up ...
pipeline-grafana             Up ...
```

### 4. Get the Airflow admin password

Airflow 3 auto-generates a password on first start:

```bash
docker logs pipeline-airflow-webserver 2>&1 | grep "Password for user"
```

Output example:
```
Simple auth manager | Password for user 'admin': seNsfpB5eH3aTnTk
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | `admin` / see step 4 above |
| **Spark Master UI** | http://localhost:8088 | — |
| **Spark Worker UI** | http://localhost:8081 | — |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **MinIO API (S3)** | http://localhost:9000 | `minioadmin` / `minioadmin` |
| **Prometheus** | http://localhost:9090 | — |
| **Grafana** | http://localhost:3000 | `admin` / `admin` |
| **Kafka** | `localhost:9092` | — |
| **PostgreSQL** | `localhost:5432` | `airflow` / `airflow` |

---

## Trigger the pipeline

Once Airflow is up, trigger the daily batch DAG:

```bash
# Via CLI inside the scheduler container
docker exec pipeline-airflow-scheduler airflow dags trigger daily_batch_pipeline

# Or open the Airflow UI → DAGs → daily_batch_pipeline → Trigger
```

The DAG runs: **Ingest → Validate → Transform → Load → Monitor**

---

## Development

```bash
# Run tests
poetry run pytest

# Run tests with coverage
poetry run pytest --cov=src --cov-report=term-missing

# Lint
poetry run ruff check src/ tests/

# Format check
poetry run black --check src/ tests/

# Type check
poetry run mypy src/
```

---

## Stopping and resetting

```bash
# Stop all containers (keep data)
docker compose down

# Stop and wipe all volumes (full reset)
docker compose down -v
```

After a full reset, start from step 3 — Airflow will generate a new admin password.

---

## Project structure

```
production-data-pipeline/
├── docker-compose.yml          # All services
├── pyproject.toml              # Python dependencies (Poetry)
├── pyrightconfig.json          # IDE type checking config
│
├── airflow/
│   └── dags/
│       └── daily_batch_pipeline.py   # Main ETL DAG
│
├── src/
│   ├── ingestion/
│   │   └── base_connector.py   # API, File, Database connectors
│   ├── validation/
│   │   └── quality_gates.py    # Null checks, business rules
│   ├── transformation/         # Spark jobs
│   ├── warehouse/
│   │   └── warehouse_loader.py # Idempotent load
│   └── monitoring/             # Prometheus metrics, alerting
│
├── config/
│   └── prometheus.yml          # Prometheus scrape config
│
├── scripts/
│   └── create_airflow_user.py  # Fallback user creation script
│
└── tests/
    ├── test_ingestion.py
    ├── test_validation.py
    └── integration/
```

---

## Architecture

```
20 Data Sources (REST APIs, S3 files, Kafka)
        │
        ▼
  Ingestion Layer          async fetch, retries, schema validation
        │
        ▼
  Validation Layer         null checks, business rules, quality gates (≥95%)
        │
        ▼
  Spark Transform          deduplicate, cleanse, enrich, aggregate
        │
        ▼
  Warehouse Load           idempotent merge (event_id + date)
        │
        ▼
  Monitoring               freshness check, cost tracking, Prometheus metrics
```

---

## Troubleshooting

**Airflow webserver not healthy**
```bash
docker logs pipeline-airflow-webserver --tail 50
# Health endpoint: http://localhost:8080/api/v2/monitor/health
```

**Postgres container exits immediately**
```bash
# If upgrading from a previous version, wipe the volume:
docker compose down -v
docker compose up -d
```

**Kafka unhealthy**
```bash
# Test broker connectivity from inside the container:
docker exec pipeline-kafka kafka-broker-api-versions --bootstrap-server kafka:29092
```

**Poetry using wrong Python version**
```bash
mise local python 3.12
poetry env use $(mise which python)
poetry install --with dev
```
