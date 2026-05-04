# Module Reference

## src.ingestion

### `base_connector.DataConnector` (abstract)

Base class for all source connectors.

```python
class DataConnector(ABC):
    def execute(self) -> list[dict]
    # Wraps fetch(), records metrics, logs lineage.

    @abstractmethod
    async def fetch(self) -> list[dict]
    # Implement in subclasses.
```

---

### `api_connector.APIConnector`

Fetches JSON from a REST endpoint with async rate-limiting and retry.

```python
connector = APIConnector(
    name="events_api",
    url="https://api.example.com/events",
    headers={"Authorization": "Bearer token"},
    params={"limit": 1000},
    max_concurrent=5,      # asyncio.Semaphore
    data_key="data",       # unwrap {"data": [...]}
)
records = connector.execute()   # → list[dict]
```

---

### `file_connector.FileConnector`

Reads Parquet, CSV, or JSON from a local path or `s3://` URI.

```python
connector = FileConnector(
    name="daily_export",
    path="s3://bucket/prefix/2024-01-15/",
    format="parquet",         # "parquet" | "csv" | "json"
)
records = connector.execute()
```

---

### `kafka_connector.KafkaConnector`

Batch-consumes messages from a Kafka topic (manual commit).

```python
connector = KafkaConnector(
    name="raw_events",
    bootstrap_servers="kafka:9092",
    topic="raw_events",
    group_id="pipeline_consumer",
    max_records=10_000,
    timeout_ms=5_000,
)
records = connector.execute()
```

---

### `decorators`

```python
@retry_with_backoff(max_retries=3, base_delay=1.0)
async def fetch_data(): ...

@rate_limit(calls_per_second=10)
async def call_api(): ...
```

---

## src.validation

### `quality_gates.NullCheckGate`

Fails when required fields contain nulls above `threshold`.

```python
gate = NullCheckGate(
    required_fields=["event_id", "user_id"],
    threshold=0.95,           # 95 % must be non-null to pass
    alert_level="critical",   # "warning" | "critical"
)
result: ValidationResult = gate.validate(records)
result.passed       # bool
result.pass_rate    # float  0.0–1.0
result.fail_count   # int
```

---

### `quality_gates.BusinessRuleGate`

Evaluates Python expressions against each record.

```python
gate = BusinessRuleGate(
    rules={"positive_amount": "amount >= 0", "valid_type": "event_type in ['purchase','view']"},
    threshold=0.99,
)
```

---

### `quality_gates.QualityGateChecker`

Runs multiple gates; raises `ValueError` if any critical gate fails.

```python
checker = QualityGateChecker(gates=[gate1, gate2])
clean_data, results = checker.validate_all(records)
score = checker.get_quality_score()   # 0.0–1.0 mean of pass_rates
```

---

### `deduplication.Deduplicator`

SHA-256 hash of key fields for exact deduplication.

```python
dedup = Deduplicator(key_fields=["event_id", "user_id"])
clean = dedup.deduplicate(records)   # first occurrence wins
```

---

### `schema_validator.SchemaValidator`

Validates records against a type map; returns valid and invalid lists.

```python
validator = SchemaValidator({"id": "integer", "name": "string", "amount": "number"})
valid, invalid = validator.validate_batch(records)
```

Type strings: `"integer"`, `"string"`, `"number"`, `"boolean"`, `"array"`, `"object"`.

---

### `data_profiler.DataProfiler`

Returns null counts, unique counts, and numeric statistics per column.

```python
profiler = DataProfiler()
profile = profiler.profile(records)
# {"event_id": {"null_count": 0, "null_rate": 0.0, "unique_count": 1000}, ...}
```

---

## src.transformation

### `spark_transformer.SparkTransformer`

```python
transformer = SparkTransformer(spark)

df = transformer.cleanse(df, required_cols=["user_id"])
# Drops rows where required_cols are null, then drops duplicates.

df = transformer.cast_columns(df, {"amount": "double", "user_id": "integer"})

df = transformer.add_audit_columns(df, source="api_events")
# Adds _source (string) and _loaded_at (timestamp).
```

---

### `aggregations`

```python
from src.transformation.aggregations import daily_user_metrics, top_n_per_group, running_total

# Per-user per-day: event_count, total_amount, unique_event_types
result = daily_user_metrics(df, date_col="event_date")

# Top N products per category by sales rank (window function)
result = top_n_per_group(df, group_col="category", rank_col="sales", n=5)

# Cumulative sum partitioned by user
result = running_total(df, partition_col="user_id", order_col="timestamp", value_col="amount")
```

---

### `enrichment.Enricher`

Broadcast-joins dimension tables onto fact data.

```python
enricher = Enricher(spark)
df = enricher.enrich_with_user_segments(events_df, users_df)
df = enricher.enrich_with_product_info(events_df, products_df)
```

---

## src.warehouse

### `warehouse_loader.idempotent_load`

Dev/test: writes Parquet to `/tmp/warehouse/`. Swap with `SnowflakeLoader` in production.

```python
result = idempotent_load(
    records,
    table_name="fact_events",
    merge_key=["event_id"],
)
# result: {"rows_loaded": int, "table": str}
```

---

### `snowflake_loader.SnowflakeLoader`

```python
loader = SnowflakeLoader(connection_params={
    "account": "...", "user": "...", "password": "...",
    "database": "DW", "schema": "PUBLIC", "warehouse": "WH",
})
loader.merge(records, table="fact_events", merge_key=["event_id"])
```

Internally: `PUT` records to internal stage → `MERGE INTO` on merge key.

---

## src.monitoring

### `metrics.PipelineMetrics`

```python
metrics = PipelineMetrics()
metrics.record_rows_processed("ingest", 10_000)
metrics.record_quality_score("validate", 0.99)

with metrics.time_stage("transform"):
    run_spark_job()
```

Prometheus counters/gauges are registered automatically when `prometheus_client` is installed.

---

### `alerting.AlertManager`

```python
alerts = AlertManager(slack_webhook_url="https://hooks.slack.com/...")
alerts.sla_miss(dag_id="daily_batch", expected_minutes=240, actual_minutes=275)
alerts.quality_gate_failed(gate_name="null_check", pass_rate=0.82, threshold=0.95)
alerts.source_failure(source_name="events_api", error="Connection refused")
```

---

### `lineage.LineageTracker`

```python
tracker = LineageTracker()
tracker.record("ingest", source="api_events", target="bronze", rows=10_000, status="success")
tracker.record("transform", source="bronze", target="silver", rows=9_800, status="success")
tracker.save("/tmp/lineage.json")   # written by monitor_pipeline.py --timeline

records = tracker.all_records()    # list[dict]
```
