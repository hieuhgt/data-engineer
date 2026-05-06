# Production Data Pipeline

A production-grade data pipeline built to scale: ingests data from multiple sources, streams events via Kafka, transforms with Spark, orchestrates with Airflow, stores in MinIO (S3-compatible), and exposes metrics via Prometheus + Grafana.

**Stack**: Airflow 3 · Spark 4 · Kafka (KRaft) · PostgreSQL 18 · MinIO · Prometheus · Grafana  
**Pattern**: Lambda (batch + streaming)  
**Scale**: Designed to handle millions of records per day

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Docker Desktop | 4.x+ | https://docs.docker.com/desktop/ |
| mise | latest | `brew install mise` |
| Poetry | 1.8+ | `pipx install poetry` |

---

## How it works

The project has three Airflow DAGs, each with a different job.

---

### DAG 1 — `daily_batch_pipeline` (runs every day at midnight)

This is the main pipeline. It moves data from external sources all the way to the warehouse every day, designed to scale to millions of records.

```
[Data Sources (REST APIs)]
  Source: https://dummyjson.com/users — 208 users total
        │
        ▼
  1. Ingest        Fetch data page by page (100 rows/page, offset-based: limit/skip).
                   208 users → 3 pages: skip=0 (100), skip=100 (100), skip=200 (8).
                   Each page is written directly to MinIO as Parquet — nothing
                   is held in memory. Supports up to 3 source failures before
                   the whole job fails.
                   XCom carries only MinIO paths (a few bytes), not the data.
        │
        ▼  XCom: { "users": { "prefix": "users/date=.../", "rows": 208 } }
        ▼
  2. Validate      Read Parquet files from MinIO and run vectorized null checks
                   using pandas (no Python loops over rows — scales to 1M+).
                   Required fields: id, firstName, email.
                   If overall quality drops below 95%, the pipeline stops here.
                   XCom carries only small metadata (quality scores, row counts).
        │
        ▼  XCom: { "users": { "passed": true, "quality_score": 1.0, "rows": 208 } }
        ▼
  3. Confirm       Data is already in MinIO from step 1. This step checks which
                   sources passed validation and marks them ready for Spark.
                   Sources that failed validation are excluded from transform.
        │
        ▼  s3a://raw-bucket/{source}/date=YYYY-MM-DD/part-00000.parquet
        ▼
  4. Transform     Submit a Spark job (spark/transform_daily.py) to the cluster.
                   Spark reads all validated sources and applies the medallion pattern:
                     Bronze → Silver (cleanse, cast, flatten nested structs, deduplicate)
                     Silver → Gold  (aggregate by company, date)
                   Outputs written to MinIO at s3://processed-bucket/silver/ and /gold/
        │
        ▼  s3a://processed-bucket/gold/date=YYYY-MM-DD/part-*.parquet
        ▼
  5. Load          Read gold Parquet from MinIO (via boto3), convert to records,
                   and write to the warehouse using an idempotent MERGE.
                   Safe to retry — running it twice gives the same result.
        │
        ▼
  6. Monitor       Post-load checks: data freshness, row counts, cost tracking.
```

**Why this architecture scales:**

| Bottleneck | Old approach | New approach |
|-----------|-------------|-------------|
| XCom size | Push all data rows into PostgreSQL | Push only MinIO paths (bytes) |
| Validation speed | Python `for` loop over every row | `df.isnull().any(axis=1).sum()` — one pandas op |
| Memory in ingest | Load full dataset into RAM | Stream 100 rows at a time → write → free |
| Save to MinIO | Re-read from XCom then write | Data already in MinIO from ingest |
| Pagination style | `_page=1&_limit=N` (JSONPlaceholder) | `limit=N&skip=offset` (standard offset-based) |

**What data looks like at each stage:**

| Stage | Location | Format | Size (1M records) |
|---|---|---|---|
| During ingest | MinIO (`raw-bucket`) | Parquet chunks (100 rows each) | ~50 MB |
| After validate | XCom (Airflow Postgres) | Small metadata dict | < 1 KB |
| After confirm | `s3://raw-bucket/{source}/date={ds}/` | Parquet | ~50 MB |
| After Spark silver | `s3://processed-bucket/silver/date={ds}/` | Parquet | ~30 MB |
| After Spark gold | `s3://processed-bucket/gold/date={ds}/` | Parquet (aggregated) | < 1 MB |
| After load | `s3://warehouse/dim_users/latest.parquet` | Parquet (MinIO) | < 1 MB |

---

### DAG 2 — `kafka_streaming_monitor` (runs every 15 minutes)

This DAG monitors the long-lived Spark Streaming job (`spark/streaming_job.py`) and restarts it automatically if it crashes.

```
Every 15 minutes:

  1. Check job     Query Spark Master REST API: is StreamingIngest running?
        │
        ▼
  2. Restart       If the job is down → submit it via spark-submit in background.
        │          If it's already running → skip.
        ▼
  3. Check lag     Connect to Kafka (kafka:29092) and calculate real consumer lag:
                     lag = latest_offset - consumer_group_committed_offset
        │
        ▼
  4. Evaluate      lag < 50,000 → OK, log and pass
                   lag > 50,000 → fire alert, fail the DAG to notify on-call
```

The Spark Streaming job (`spark/streaming_job.py`) runs continuously: reads from `raw-events` Kafka topic → writes Parquet to `s3a://raw-bucket/streaming/events/` every 60 seconds.

**First-time start (run once manually):**
```bash
docker exec -d pipeline-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --name StreamingIngest \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.apache.hadoop:hadoop-aws:3.4.1" \
  /opt/spark-jobs/streaming_job.py
```

After the first start, the DAG handles restarts automatically.

---

### DAG 3 — `pipeline_health_monitor` (runs every hour)

An independent watchdog that checks whether the batch pipeline is staying healthy between runs.

```
Every hour:

  1. Freshness     Check warehouse output. If older than 4 hours → SLA miss alert.
        │
        ▼
  2. Row counts    Check that warehouse table has at least 1,000 rows.
```

---

### How the three DAGs relate

```
                      ┌──────────────────────────────┐
                      │     daily_batch_pipeline      │  midnight daily
                      │  Ingest → Validate →          │
                      │  Confirm → Transform →        │
                      │  Load → Monitor               │
                      └──────────────┬───────────────┘
                                     │ writes to MinIO / warehouse
               ┌─────────────────────┴──────────────────────┐
               ▼                                             ▼
┌──────────────────────────┐              ┌──────────────────────────────┐
│  pipeline_health_monitor │              │  kafka_streaming_monitor     │
│  every hour              │              │  every 15 min                │
│  Checks freshness and    │              │  Watches Spark Streaming job │
│  row counts of output    │              │  and Kafka consumer lag      │
└──────────────────────────┘              └──────────────────────────────┘
```

---

## Kafka in detail

Kafka handles the **streaming side** of the Lambda architecture — events that cannot wait for the nightly batch.

### What Kafka does here

```
Event Producers (apps, IoT, clickstreams)
        │  publish messages
        ▼
   Kafka Topic "raw-events"
        │  consume messages
        ▼
  Spark Streaming Job (spark/streaming_job.py)
        │  runs 24/7, micro-batches every 60 seconds
        ▼
   MinIO (s3a://raw-bucket/streaming/events/event_date=YYYY-MM-DD/)
        │
        ▼
  Airflow monitors lag every 15 min (kafka_streaming_monitor DAG)
```

### How Kafka is configured

This project runs Kafka in **KRaft mode** — no ZooKeeper required (Confluent 8.x+). KRaft means Kafka manages its own metadata internally using a Raft consensus log.

```yaml
KAFKA_PROCESS_ROLES: broker,controller       # single node acts as both
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093 # controller port (internal)
KAFKA_LISTENERS:
  PLAINTEXT://kafka:29092                    # internal Docker traffic
  PLAINTEXT_HOST://0.0.0.0:9092             # host machine access
  CONTROLLER://kafka:9093                    # Raft consensus
```

**Two listeners** because Kafka tells clients where to reconnect after the initial handshake:
- Inside Docker → containers use `kafka:29092`
- From your Mac → use `localhost:9092`

### Consumer lag monitoring

The `kafka_streaming_monitor` DAG calculates real lag every 15 minutes:

```
lag = end_offset (latest message on partition)
    - committed_offset (last message Spark confirmed reading)
```

- Lag = 0: Spark is keeping up
- Lag growing: Spark is falling behind
- Lag > 50,000: alert fires, DAG fails to notify on-call

### Connect to Kafka locally

```bash
# List topics
docker exec pipeline-kafka kafka-topics --bootstrap-server kafka:29092 --list

# Produce a test message
docker exec -it pipeline-kafka kafka-console-producer \
  --bootstrap-server kafka:29092 --topic raw-events

# Consume messages
docker exec -it pipeline-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic raw-events --from-beginning
```

---

## Spark in detail

Spark handles heavy transformation — the medallion architecture with three layers of increasingly clean data.

### Medallion architecture

```
s3a://raw-bucket/*/date=YYYY-MM-DD/part-*.parquet
  (Bronze — raw Parquet chunks, exactly as ingested from dummyjson.com)
        │
        │  combine: firstName + lastName → name
        │  cleanse: drop nulls in id, firstName, email
        │  cast: id → int
        │  flatten address: city, state, country  (drop nested coordinates)
        │  flatten company: company_name, company_department  (drop nested address)
        │  drop: hair, bank, crypto  (not needed for analytics)
        │  deduplicate: remove exact duplicate rows
        │  audit: add _source, _loaded_at columns
        ▼
s3a://processed-bucket/silver/date=YYYY-MM-DD/
  (Silver — clean, typed, flat schema)
        │
        │  aggregate: group by event_date + company_name + company_department
        │             count(users), collect_list(names), collect_list(emails)
        ▼
s3a://processed-bucket/gold/date=YYYY-MM-DD/
  (Gold — business-ready aggregates)
```

### Why three layers?

| Layer | What it is | Who uses it |
|---|---|---|
| Bronze | Raw data, never modified | Debugging, reprocessing from scratch |
| Silver | Clean, typed, flat | Data scientists, ad-hoc queries |
| Gold | Aggregated business metrics | Dashboards, analyst reports |

If a bug is found in Silver logic, reprocess from Bronze — the raw data was never touched.

### How Spark connects to MinIO

MinIO is S3-compatible. Spark uses the `s3a://` protocol with Hadoop's S3A connector:

```python
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.path.style.access", "true")   # required for MinIO
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

`path.style.access = true` is required because MinIO does not support AWS virtual-hosted URL style (`bucket.s3.amazonaws.com`).

### How Airflow submits the Spark job

`SparkSubmitOperator` runs `spark-submit` using the Spark binary shared from the spark-master container:

```python
SparkSubmitOperator(
    task_id='transform',
    conn_id='spark_default',                              # → spark://spark-master:7077
    application='/opt/airflow/spark/transform_daily.py',
    packages='org.apache.hadoop:hadoop-aws:3.4.1,'        # S3A JARs for MinIO
             'com.amazonaws:aws-java-sdk-bundle:1.12.262',
    conf={
        'spark.executor.memory': '512m',
        'spark.driver.memory': '512m',
        'spark.sql.shuffle.partitions': '4',
        'spark.eventLog.enabled': 'true',
        'spark.eventLog.dir': '/tmp/spark-events',
    },
)
```

### Resource allocation — why 512m and 1 core?

The local Docker environment has **1 worker with 2 cores and 2 GB RAM**. Two Spark jobs run on this single worker at the same time:

```
Worker (2 cores, 2 GB RAM)
├── StreamingIngest (Kafka → MinIO, runs 24/7)
│     executor: 512m, 1 core
│     actual RAM used: 512m + 384m overhead = ~896m
│
└── DailyTransform (batch ETL, runs when triggered)
      executor: 512m, 1 core
      actual RAM used: 512m + 384m overhead = ~896m

Total: ~1.8 GB RAM, 2 cores → fits inside 2 GB worker ✓
```

**Why not use more memory?**
Spark always adds a memory overhead on top of `spark.executor.memory`:
```
actual memory = executor.memory + max(executor.memory × 10%, 384MB)
```
So `512m` executor → `512 + 384 = 896MB` actual.
Two jobs × 896MB = 1.79 GB — just fits in 2 GB.

If you set `executor.memory=2g`, one job alone would need `2048 + 384 = 2.4 GB` — more than the entire worker has. Spark would refuse to start the executor and the job would hang with:
```
WARN TaskSchedulerImpl: Initial job has not accepted any resources
```

**Why `spark.cores.max=1` for StreamingIngest?**
Without this, Spark greedily takes all available cores (2). That leaves 0 cores for DailyTransform, which also causes the same "no resources" hang. Setting `cores.max=1` for the streaming job reserves 1 core for the batch job to use.

**In production** (EMR, Databricks, Dataproc), you'd have many workers with many cores and GB of RAM each — these settings would be much larger and tuned per job. The 512m/1-core config is specific to this local single-worker setup.

### Spark History Server

After a job finishes, its Jobs/Stages/Tasks detail is visible at **http://localhost:18080** (Spark History Server). The History Server reads event logs written to a shared Docker volume (`spark_events`) that both the Airflow scheduler and the History Server mount.

### How spark-submit is available in Airflow

The Airflow container does not bundle Spark. Instead, the Spark binary is shared via a Docker named volume:

```
spark-master container
  /opt/spark/  ← Spark 4.0.2 installation
      └── mounted into spark_home volume

airflow-scheduler container
  /opt/spark/ (read-only) ← same volume
      └── spark-submit ✓
```

Java 17 is installed in the Airflow image (`openjdk-17-jre-headless`) and `SPARK_HOME=/opt/spark` is set so `spark-submit` is found automatically.

### Spark cluster layout

```
Airflow Scheduler
    │  spark-submit (SparkSubmitOperator, client mode)
    ▼
Spark Master (spark://spark-master:7077)
    │  assigns tasks
    └──▶ Spark Worker (2 cores, 2 GB RAM)
              ├── StreamingIngest: 1 core, 512m  (24/7)
              └── DailyTransform:  1 core, 512m  (on demand)
```

In production, add more workers to scale horizontally — or use a managed cluster (EMR, Databricks, Dataproc).

### View Spark jobs

Open the Spark Master UI at http://localhost:8088 — running and completed jobs, executor memory usage, and job duration.

---

## Running the stack

### 1. Set up Python

```bash
mise install python@3.12
mise local python 3.12
python --version   # Python 3.12.x
```

### 2. Install Python dependencies

```bash
poetry install --with dev
poetry env info --path   # copy → set as IDE interpreter
```

### 3. Start all services

```bash
docker compose up -d
```

Wait ~60 seconds for health checks to pass:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Expected output:

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

```bash
docker logs pipeline-airflow-webserver 2>&1 | grep "Password for user"
# Simple auth manager | Password for user 'admin': seNsfpB5eH3aTnTk
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | `admin` / see step 4 |
| **Spark Master UI** | http://localhost:8088 | — |
| **Spark Worker UI** | http://localhost:8081 | — |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **MinIO API** | http://localhost:9000 | `minioadmin` / `minioadmin` |
| **Prometheus** | http://localhost:9090 | — |
| **Grafana** | http://localhost:3000 | `admin` / `admin` |
| **Kafka** | `localhost:9092` | — |
| **PostgreSQL** | `localhost:5432` | `airflow` / `airflow` |

---

## Trigger the pipeline

```bash
# Via CLI
docker exec pipeline-airflow-scheduler airflow dags trigger daily_batch_pipeline

# Or: Airflow UI → DAGs → daily_batch_pipeline → Trigger ▶
```

Flow: **Ingest → Validate → Confirm → Transform → Load → Monitor**

---

## Development

```bash
poetry run pytest                                   # run tests
poetry run pytest --cov=src --cov-report=term-missing  # with coverage
poetry run ruff check src/ tests/                   # lint
poetry run black --check src/ tests/               # format check
poetry run mypy src/                               # type check
```

---

## Stopping and resetting

```bash
docker compose down          # stop (keep volumes)
docker compose down -v       # stop + wipe all data
```

---

## Project structure

```
production-data-pipeline/
├── docker-compose.yml          # All services + volumes
├── Dockerfile.airflow          # Airflow image (Java 17 + providers)
├── pyproject.toml              # Python dependencies (Poetry)
│
├── airflow/
│   └── dags/
│       ├── daily_batch_pipeline.py     # Main ETL DAG
│       ├── kafka_streaming_dag.py      # Streaming monitor DAG
│       └── monitoring_dag.py           # Health monitor DAG
│
├── spark/
│   ├── transform_daily.py      # Batch transform (Bronze→Silver→Gold)
│   └── streaming_job.py        # Kafka→MinIO streaming (runs 24/7)
│
├── src/
│   ├── ingestion/
│   │   └── base_connector.py   # APIConnector with fetch_pages() pagination
│   ├── validation/
│   │   └── quality_gates.py    # Null checks, business rules (pandas vectorized)
│   ├── transformation/         # SparkTransformer, aggregations, enrichment
│   ├── warehouse/
│   │   └── warehouse_loader.py # Idempotent load → MinIO warehouse bucket
│   └── monitoring/             # Prometheus metrics, alerting
│
└── config/
    └── prometheus.yml          # Prometheus scrape config
```

---

## Architecture

```
REST APIs (dummyjson.com/users — 208 users)
  │  page-by-page (100 rows/page, limit/skip offset pagination)
  │  3 pages: skip=0 (100), skip=100 (100), skip=200 (8)
  │  write directly to MinIO as Parquet chunks
  ▼
MinIO raw-bucket/{source}/date={ds}/part-*.parquet
  │  read with pandas (vectorized null check)
  ▼
Validate (pandas)
  │  XCom: small metadata only (paths + quality scores)
  ▼
Spark Transform
  │  Bronze → Silver (cleanse, flatten) → Gold (aggregate)
  │  spark-submit from Airflow via shared volume
  ▼
MinIO processed-bucket/gold/date={ds}/
  │  read with boto3 → warehouse load
  ▼
MinIO warehouse/dim_users/latest.parquet

─── Streaming path (parallel) ────────────────────────────
Kafka "raw-events"
  │  Spark Structured Streaming (60s micro-batch)
  ▼
MinIO raw-bucket/streaming/events/event_date={ds}/
  │  lag monitored every 15 min (kafka_streaming_monitor DAG)
  │  auto-restart on crash
```

---

## Troubleshooting

**Airflow webserver not healthy**
```bash
docker logs pipeline-airflow-webserver --tail 50
```

**DAG not reloading after code change**
```bash
# Changes to src/ files require a forced reparse:
touch airflow/dags/daily_batch_pipeline.py
# Or:
docker exec pipeline-airflow-scheduler airflow dags reserialize
```

**Spark transform fails with S3A error**
```bash
# Verify MinIO is healthy and the raw-bucket exists:
docker exec pipeline-airflow-scheduler \
  python -c "import boto3; s3=boto3.client('s3',endpoint_url='http://minio:9000',aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin'); print(s3.list_buckets())"
```

**Kafka unhealthy**
```bash
docker exec pipeline-kafka kafka-broker-api-versions --bootstrap-server kafka:29092
```

**spark-submit not found in Airflow**
```bash
# The spark_home volume must be populated by spark-master first:
docker compose restart airflow-scheduler
```

**Poetry using wrong Python version**
```bash
mise local python 3.12
poetry env use $(mise which python)
poetry install --with dev
```
