# Architecture

## Overview

This pipeline processes **500 GB/day from 20 heterogeneous sources** with a 4-hour end-to-end SLA and 99.9 % uptime target. It uses a **Lambda architecture** — a Spark batch path for history and a Kafka streaming path for near-real-time — unified into a single Gold layer consumed by the warehouse.

```
Sources (20x)
  │ REST APIs, S3 files, PostgreSQL CDC, Kafka topics
  ▼
Bronze Layer  (raw, immutable Parquet)
  │ Schema validation · Deduplication · Quality gates
  ▼
Silver Layer  (cleansed, typed, audited Parquet)
  │ Spark transforms · Enrichment (broadcast joins)
  ▼
Gold Layer    (aggregated, business-ready Parquet)
  │ Idempotent MERGE/UPSERT
  ▼
Warehouse     (Snowflake fact/dim/agg tables)
```

---

## Data Flow

### Batch path (Airflow → Spark)

```
daily_batch_pipeline DAG  (runs at 02:00 UTC)
  ├─ ingest_*  (20 parallel tasks, partial failure OK)
  ├─ validate  (quality gates: null check, business rules, schema)
  ├─ transform (SparkSubmit: Bronze → Silver → Gold)
  ├─ load      (idempotent_load → Snowflake MERGE INTO)
  └─ monitor   (freshness checks, Slack alert on SLA miss)
```

### Streaming path (Kafka → Spark Structured Streaming)

```
Kafka topics  →  Spark Structured Streaming  →  Bronze Parquet
                  60-second micro-batch           (append-only)
                  checkpoint in HDFS/S3
```

Every 15 minutes the `kafka_streaming_dag` checks consumer lag; if lag > threshold it fires a Slack alert.

---

## Design Decisions

### Idempotency everywhere

Every write uses a MERGE/UPSERT pattern keyed on a natural business key (`event_id`, `transaction_id`). Re-running the same DAG for the same date produces identical output — safe for backfills and retries.

### Partial source failure is non-fatal

The ingest layer wraps each source in an independent try/except. A single unavailable API logs to `failed_sources` and continues; the downstream validation step receives however many sources succeeded. A Slack alert fires for any failed source but does not block the run.

### Quality gates are blocking for critical rules

`NullCheckGate` with `alert_level="critical"` raises `ValueError` and halts the pipeline when data quality drops below threshold. Non-critical gates log warnings and increment a Prometheus counter without stopping the run.

### Bronze layer is append-only

Raw data is never mutated after landing in Bronze. This makes debugging straightforward — any anomaly can be reprocessed from the immutable Bronze snapshot.

### Broadcast joins for dimension tables

User and product dimensions are small enough to fit in Spark executor memory. Using `F.broadcast()` eliminates the shuffle that would otherwise dominate runtime for fact-dimension joins.

---

## Component Responsibilities

| Component | Responsibility |
|---|---|
| `src/ingestion/` | Pull data from each source type; rate-limit, retry, normalise to dict list |
| `src/validation/` | Schema check, null check, business rules, deduplication |
| `src/transformation/` | Spark cleanse → cast → enrich → aggregate |
| `src/warehouse/` | Idempotent write to local Parquet (dev) or Snowflake MERGE (prod) |
| `src/monitoring/` | Prometheus metrics, Slack alerts, lineage records |
| `airflow/dags/` | Orchestration: schedule, task order, retries, SLA callbacks |
| `spark/` | Standalone Spark jobs submitted by Airflow SparkSubmitOperator |
| `config/` | Declarative source/gate/warehouse definitions (no hardcoded values in code) |

---

## Scalability Trade-offs

| Decision | Benefit | Cost |
|---|---|---|
| Lambda architecture | Streaming + batch independently scalable | Two code paths to maintain |
| Parquet + Bronze/Silver/Gold | Schema evolution, time-travel, partition pruning | More storage than a single table |
| Async ingestion with Semaphore | High source parallelism without overwhelming APIs | Harder to debug than sequential |
| Snowflake MERGE INTO | True idempotency | Requires Snowflake; swap `idempotent_load` for other warehouses |
| Config-driven gates | Add/change rules without code changes | YAML validation needed to catch typos |

---

## Security

- Credentials in `.env` (never committed); use Airflow Connections / AWS Secrets Manager in production.
- MinIO uses bucket policies; S3 uses IAM roles with least-privilege.
- Snowflake uses a dedicated `PIPELINE_ROLE` with write access only to pipeline schemas.
- Docker containers run as non-root users.
