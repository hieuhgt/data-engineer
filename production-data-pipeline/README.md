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

## How it works

The project has three Airflow DAGs, each with a different job. Here is what each one does and how they fit together.

---

### DAG 1 — `daily_batch_pipeline` (runs every day at midnight)

This is the main pipeline. It moves data from 20 external sources all the way to the warehouse every day.

```
[20 Data Sources]
       │
       ▼
  1. Ingest        Fetch data from all 20 sources in parallel (REST APIs + S3 files).
                   Allowed to have up to 3 source failures before the whole job fails.
                   Saves raw data to XCom for the next step.
       │
       ▼
  2. Validate      Run quality gates on every source:
                   - Null check: required fields (id, user_id) must not be empty
                   - Business rule: amount must be > 0
                   If overall quality score drops below 95%, the pipeline stops here
                   and does not load bad data to the warehouse.
       │
       ▼
  3. Save to MinIO  For each source that passed validation, convert data to Parquet
                   and write to MinIO at:
                     s3://raw-bucket/{source_name}/date=YYYY-MM-DD/data.parquet
                   This is the handoff point from Python-land to Spark-land.
                   Sources that failed validation are skipped — bad data never reaches storage.
       │
       ▼
  4. Transform     Submit a Spark job (spark/transform_daily.py) to the Spark cluster.
                   Spark reads from s3://raw-bucket/*/date=YYYY-MM-DD/ (all sources),
                   applies a three-layer medallion pattern:
                     Bronze → Silver (cleanse, cast, deduplicate)
                     Silver → Gold (enrich, aggregate daily metrics)
                   Outputs written back to MinIO at s3://processed-bucket/silver/ and /gold/
       │
       ▼
  5. Load          Read the Spark output and load it into the warehouse using an
                   idempotent MERGE (safe to retry — running it twice gives the same
                   result). Merge key is event_id + date.
       │
       ▼
  6. Monitor       Post-load checks: data freshness, row counts, cost tracking.
                   Sends a Slack alert if SLA is missed (> 4 hours total).
```

**Why this order is best practice:**

- Validate before storing — bad data is rejected before it ever touches persistent storage
- Small data (XCom) → Parquet on object storage → Spark reads it: each layer uses the right tool for the job size
- Parquet on MinIO is the universal handoff format — Spark, pandas, and DuckDB can all read it
- The medallion pattern (Bronze/Silver/Gold) keeps raw data untouched so bugs can be reprocessed

**What the 20 sources are:**

| Category | Sources |
|---|---|
| Users & social | JSONPlaceholder users, posts, comments, todos, albums |
| Random profiles | RandomUser API |
| Geography | REST Countries |
| Weather | Open-Meteo (New York, London, Tokyo) |
| Crypto | CoinGecko prices + trending |
| Public datasets | Universities, cat facts, dog breeds |
| Space | NASA APOD, ISS live position |
| HTTP testing | httpbin.org JSON + user-agent |
| File source | S3 transactions from MinIO |

---

### DAG 2 — `kafka_streaming_monitor` (runs every 15 minutes)

This DAG does **not** run the Spark streaming job — that runs as a separate long-lived process (`spark/streaming_job.py`). This DAG only watches over it.

```
Every 15 minutes:

  1. Check lag     Connect to Kafka and measure how far behind the consumer group is.
       │
       ▼
  2. Evaluate      If lag > 50,000 messages → fire a warning alert and fail the DAG
                   so the on-call person is notified.
                   If lag is normal → pass silently.
```

If you do not have the streaming job running, this DAG will log a warning and skip (it does not crash the whole setup).

---

### DAG 3 — `pipeline_health_monitor` (runs every hour)

An independent watchdog that checks whether the batch pipeline is staying healthy between runs.

```
Every hour:

  1. Freshness     Look at the warehouse output file (fact_events.parquet).
                   If it is older than 4 hours → SLA miss alert is fired.
       │
       ▼
  2. Row counts    Check that fact_events has at least 1,000 rows.
                   Warns if the table looks suspiciously empty.
```

This DAG is independent from the batch pipeline — it keeps running even if the batch pipeline fails, so you always know the last time data was fresh.

---

### How the three DAGs relate

```
                         ┌─────────────────────────┐
                         │  daily_batch_pipeline    │  runs once a day
                         │  Ingest→Validate→        │  writes fact_events.parquet
                         │  Transform→Load→Monitor  │
                         └────────────┬────────────┘
                                      │ output
                    ┌─────────────────┴──────────────────┐
                    ▼                                     ▼
     ┌──────────────────────────┐        ┌───────────────────────────┐
     │  pipeline_health_monitor │        │  kafka_streaming_monitor  │
     │  (every hour)            │        │  (every 15 min)           │
     │  Checks freshness and    │        │  Watches the Kafka        │
     │  row counts of output    │        │  consumer lag             │
     └──────────────────────────┘        └───────────────────────────┘
```

The batch pipeline **produces** data. The monitoring DAGs **observe** it. They are loosely coupled — each can fail independently without taking down the others.

---

### Where data lives at each stage

| Stage | Location | Format |
|---|---|---|
| After ingest | XCom (Airflow Postgres) | Python list of dicts |
| After validate | XCom | Python dict (quality scores) |
| After save_to_minio | `s3://raw-bucket/{source}/date={ds}/data.parquet` | Parquet (MinIO) |
| After Spark silver | `s3://processed-bucket/silver/date={ds}/` | Parquet (MinIO) |
| After Spark gold | `s3://processed-bucket/gold/date={ds}/` | Parquet (MinIO) |
| After load (demo) | `/tmp/warehouse/fact_events.parquet` | Parquet (local) |
| After load (production) | Snowflake / BigQuery table | SQL table |
| Pipeline metadata | PostgreSQL | Airflow internal DB |

---

## Kafka in detail

Kafka handles the **streaming side** of the Lambda architecture — events that cannot wait for the nightly batch.

### What Kafka does here

```
Event Producers (apps, IoT, clickstreams)
        │
        │  publish messages
        ▼
   Kafka Topic (raw_events, raw_transactions)
        │
        │  consume messages
        ▼
  Spark Streaming Job (spark/streaming_job.py)
        │  runs 24/7 as a long-lived process
        │  micro-batches every 30 seconds
        ▼
   MinIO (s3://raw-bucket/streaming/...)
        │
        ▼
  Airflow monitors lag every 15 minutes (kafka_streaming_monitor DAG)
```

### How Kafka is configured

This project runs Kafka in **KRaft mode** — no ZooKeeper required (available from Confluent 8.x). KRaft means Kafka manages its own metadata internally using a Raft consensus log.

```yaml
KAFKA_PROCESS_ROLES: broker,controller       # single node acts as both
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093 # controller port (internal)
KAFKA_LISTENERS:
  PLAINTEXT://kafka:29092                    # internal Docker traffic
  PLAINTEXT_HOST://0.0.0.0:9092             # host machine access
  CONTROLLER://kafka:9093                    # Raft consensus
```

**Two listeners** because Kafka needs to tell clients where to reconnect after the initial connection:
- Inside Docker, other containers use `kafka:29092`
- From your Mac, you use `localhost:9092`

### Topics used

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `raw_events` | Application events | Spark Streaming | User activity, clicks |
| `raw_transactions` | Payment system | Spark Streaming | Financial transactions |

### Consumer lag monitoring

The `kafka_streaming_monitor` DAG connects to Kafka every 15 minutes and measures **consumer lag** — how many messages are in the topic that the Spark consumer has not yet read.

- Lag = 0: Spark is keeping up in real time
- Lag growing: Spark is falling behind, data is accumulating
- Lag > 50,000: alert fires, DAG fails to notify on-call

### Connect to Kafka locally

```bash
# List topics
docker exec pipeline-kafka kafka-topics --bootstrap-server kafka:29092 --list

# Produce a test message
docker exec -it pipeline-kafka kafka-console-producer \
  --bootstrap-server kafka:29092 --topic raw_events

# Consume messages
docker exec -it pipeline-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic raw_events --from-beginning
```

---

## Spark in detail

Spark handles the **heavy transformation** — processing that would be too slow or too memory-intensive for a single Python process.

### What Spark does here

The Spark job (`spark/transform_daily.py`) implements the **medallion architecture** — three layers of increasingly clean data:

```
s3://raw-bucket/*/date=YYYY-MM-DD/data.parquet
  (Bronze — raw, exactly as ingested)
        │
        │  cleanse: drop nulls in required columns
        │  cast: amount → double, user_id → int
        │  deduplicate: remove exact duplicate rows
        │  audit: add _ingested_at, _source columns
        ▼
s3://processed-bucket/silver/date=YYYY-MM-DD/
  (Silver — clean, typed, deduplicated)
        │
        │  enrich: join with user_segments dimension table
        │  aggregate: group by user_segment + date
        │             sum(amount), count(events), avg(session_length)
        ▼
s3://processed-bucket/gold/date=YYYY-MM-DD/
  (Gold — business-ready aggregates)
```

### Why three layers?

| Layer | What it is | Who uses it |
|---|---|---|
| Bronze | Raw data, never modified | Debugging, reprocessing from scratch |
| Silver | Clean + typed | Data scientists, ad-hoc analysis |
| Gold | Aggregated business metrics | BI dashboards, analyst reports |

If a bug is found in the Silver logic, you reprocess from Bronze — the raw data was never touched.

### How Spark connects to MinIO

MinIO is S3-compatible, so Spark reads/writes it using the `s3a://` protocol (Hadoop's S3A connector):

```python
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
```

`path.style.access = true` is required for MinIO because it does not support the AWS virtual-hosted URL style (`bucket.s3.amazonaws.com`).

### How Airflow submits the Spark job

The `transform` task uses `SparkSubmitOperator`, which runs `spark-submit` to send the job to the Spark master:

```python
SparkSubmitOperator(
    task_id='transform',
    application='/spark/transform_daily.py',
    application_args=['--date', '{{ ds }}'],   # Jinja template — Airflow fills in the date
    conf={
        'spark.executor.memory': '4g',
        'spark.executor.cores': 4,
        'spark.driver.memory': '2g',
    },
    num_executors=4,   # 4 workers × 4 cores × 4 GB = 64 GB total capacity
)
```

`{{ ds }}` is an Airflow template variable that resolves to the run date (e.g. `2026-05-05`). This is how the Spark job knows which date partition to process.

### Spark cluster layout

```
Airflow Scheduler
    │  spark-submit (via SparkSubmitOperator)
    ▼
Spark Master (spark://spark-master:7077)
    │  assigns work
    ├──▶ Spark Worker 1 (2 cores, 2 GB)
    └──▶ Spark Worker 2 (2 cores, 2 GB)  ← add more workers to scale
```

In this local setup there is one worker with 2 cores and 2 GB. In production you would run multiple workers (or use a managed cluster like EMR or Databricks).

### View Spark jobs

Open the Spark Master UI at http://localhost:8088 — you can see running and completed jobs, executor memory usage, and job duration.

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

The DAG runs: **Ingest → Validate → Save to MinIO → Spark Transform → Load → Monitor**

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
        │  XCom (Postgres)
        ▼
  Validation Layer         null checks, business rules, quality gates (≥95%)
        │  XCom (Postgres)
        ▼
  Save to MinIO            Parquet → s3://raw-bucket/{source}/date={ds}/
        │  s3a:// (MinIO)
        ▼
  Spark Transform          Bronze → Silver (cleanse) → Gold (aggregate)
        │  s3a:// (MinIO)
        ▼
  Warehouse Load           idempotent merge, s3://processed-bucket/gold/
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
