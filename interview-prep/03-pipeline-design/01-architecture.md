# Production Data Pipeline Architecture

## Real-World Scenario

You need to ingest 500GB of data daily from 20 sources, transform it, and serve it to 50 analysts. Requirements:
- **Latency:** Results available within 4 hours
- **Reliability:** 99.9% uptime (8 hours downtime allowed per year)
- **Cost:** Minimize cloud spend
- **Maintainability:** New analyst on team can debug issues

**Key Decision:** What's the architecture?

---

## 1. Pipeline Components & Architecture Patterns

### Batch vs. Streaming

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| Latency | Hours | Seconds |
| Throughput | Very high (TB) | Medium (MB/s) |
| Complexity | Lower | Higher |
| Cost | Lower | Higher |
| Example | Daily sales ETL | Real-time fraud detection |

**For the scenario above:** Batch (4-hour latency is acceptable, cost-sensitive)

### Architecture Pattern: Lambda (Batch + Real-time)

```
Data Sources
    ↓
    ├→ Kafka (Real-time stream) → Spark Streaming → Hot table (minutes latency)
    │
    └→ S3 (Raw data) → Daily batch (Spark/Airflow) → Warehouse → Analyst table (hours)

Result: Analysts get daily batch for reports, ops get real-time for alerts
```

### Recommended Architecture for Scenario

```
Data Sources
    ↓
Cloud Storage (S3/GCS)
    ↓ (Airflow orchestrates)
Ingestion Layer
    ├ Validate schema
    └ Check for duplicates
    ↓
Transform Layer (Spark)
    ├ Cleanse (nulls, typos, ranges)
    ├ Enrich (add external data)
    └ Aggregate (daily rollups)
    ↓
Data Warehouse (Snowflake/BigQuery)
    ├ Fact tables (events, transactions)
    └ Dimension tables (users, products)
    ↓
Analyst Tools (Tableau, dbt)
```

---

## 2. Data Pipeline Best Practices

### Idempotency (Run Pipeline Safely Multiple Times)

**Problem:** Pipeline fails halfway. Retry causes duplicates.

```python
# ❌ BAD: Not idempotent (INSERT creates duplicates)
def load_data(source, warehouse):
    data = source.read()
    warehouse.insert(data)  # Run twice = 2x rows

# ✅ GOOD: Idempotent (can run safely N times)
def load_data_idempotent(source, warehouse):
    data = source.read()
    warehouse.truncate(table_name)  # Clear before loading
    warehouse.insert(data)

# ✅ BETTER: Idempotent with deduplication
def load_data_dedup(source, warehouse):
    data = source.read()
    # Merge: update if exists, insert if not
    warehouse.merge(
        target_table='fact_orders',
        source_data=data,
        on=['order_id', 'source_id'],  # Unique key
        when_matched='update',
        when_not_matched='insert'
    )
```

### Data Lineage (Track Where Data Comes From)

```python
import json
from datetime import datetime

class PipelineStep:
    """Track data lineage through pipeline."""

    def __init__(self, step_name, source_table, target_table):
        self.step_name = step_name
        self.source_table = source_table
        self.target_table = target_table
        self.execution_time = datetime.now()
        self.status = None
        self.row_count = 0
        self.errors = []

    def to_lineage_record(self):
        """Log for audit trail."""
        return {
            'step': self.step_name,
            'source': self.source_table,
            'target': self.target_table,
            'timestamp': self.execution_time.isoformat(),
            'status': self.status,
            'rows_processed': self.row_count,
            'errors': self.errors
        }

# Usage: Auditing
pipeline = PipelineStep('transform_orders', 'raw_orders', 'fact_orders')
pipeline.row_count = 1000000
pipeline.status = 'success'

audit_log = pipeline.to_lineage_record()
# Can trace exactly which data came from which step
```

### Data Quality Gates (Fail Fast)

```python
class QualityGate:
    """Prevent bad data from reaching analytics."""

    def __init__(self, name, threshold=0.95):
        self.name = name
        self.threshold = threshold  # 95% of records must pass

    def validate(self, records):
        """Check quality before loading to warehouse."""
        valid = 0
        total = len(records)

        for record in records:
            if self._is_valid(record):
                valid += 1

        pass_rate = valid / total if total > 0 else 0

        if pass_rate < self.threshold:
            raise ValueError(
                f"Quality gate {self.name} failed: "
                f"{pass_rate:.1%} pass rate < {self.threshold:.1%} threshold"
            )

        return records

    def _is_valid(self, record):
        # Define validation logic
        required_fields = ['user_id', 'order_id', 'amount']
        return all(record.get(field) is not None for field in required_fields)

# Usage
quality = QualityGate('order_validation', threshold=0.95)
try:
    valid_records = quality.validate(raw_data)
    warehouse.load(valid_records)
except ValueError as e:
    logger.error(f"Pipeline stopped: {e}")
    # Alert engineers, don't load bad data
    raise
```

---

## 3. Failure Modes & Recovery

### Handling Common Failures

#### 1. Partial Failure (Some Sources Fail)

```python
async def ingest_all_sources(sources):
    """
    Scenario: 20 data sources, 1 is down.
    Goal: Load 19, alert on 1, don't block entire pipeline.
    """
    results = {}
    failed = []

    for source in sources:
        try:
            data = await source.fetch()
            results[source.name] = data
        except Exception as e:
            failed.append({'source': source.name, 'error': str(e)})
            logger.error(f"Failed to ingest {source.name}: {e}")

    # Load successful sources
    if results:
        warehouse.load(results)
        logger.info(f"Loaded {len(results)} sources")

    # Alert on failures
    if failed:
        send_alert(f"Pipeline partial failure: {failed}")
        # Don't raise here—let pipeline continue

    return results, failed
```

#### 2. Duplicate Detection & Handling

```python
import hashlib

class DuplicateHandler:
    """Detect and handle duplicate records."""

    @staticmethod
    def get_record_hash(record):
        """Hash record for duplicate detection."""
        key_fields = ['order_id', 'timestamp', 'source_id']
        key = '|'.join(str(record.get(f)) for f in key_fields)
        return hashlib.sha256(key.encode()).hexdigest()

    @staticmethod
    def deduplicate_by_hash(records):
        """Remove duplicates based on hash."""
        seen = set()
        unique = []

        for record in records:
            record_hash = DuplicateHandler.get_record_hash(record)
            if record_hash not in seen:
                unique.append(record)
                seen.add(record_hash)

        logger.info(f"Deduplicated: {len(records)} → {len(unique)} records")
        return unique

# Usage
raw_data = [
    {'order_id': 1, 'timestamp': '2024-01-01', 'source_id': 'API'},
    {'order_id': 1, 'timestamp': '2024-01-01', 'source_id': 'API'},  # Duplicate
    {'order_id': 2, 'timestamp': '2024-01-01', 'source_id': 'API'},
]

clean_data = DuplicateHandler.deduplicate_by_hash(raw_data)
# Result: 3 → 2 records
```

#### 3. Backoff & Retry Strategy

```python
import asyncio
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(ConnectionError)
)
async def load_to_warehouse(data, warehouse):
    """Retry on connection error, with exponential backoff."""
    return await warehouse.insert(data)

# Retries: waits 2s, 4s, 8s before giving up
```

---

## 4. Orchestration (Airflow)

### Basic DAG Structure

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define DAG (Directed Acyclic Graph)
default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('daily_analytics_pipeline', default_args=default_args, schedule_interval='@daily')

# Tasks
def extract_task():
    """Extract from 20 sources."""
    pass

def validate_task():
    """Validate data quality."""
    pass

def transform_task():
    """Aggregate & enrich."""
    pass

def load_task():
    """Load to warehouse."""
    pass

# Define dependencies (tasks run in order)
extract = PythonOperator(task_id='extract', python_callable=extract_task, dag=dag)
validate = PythonOperator(task_id='validate', python_callable=validate_task, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=transform_task, dag=dag)
load = PythonOperator(task_id='load', python_callable=load_task, dag=dag)

# DAG structure:
#   extract → validate → transform → load
extract >> validate >> transform >> load
```

### Scheduling & Backfills

```python
# Run daily at 2 AM UTC
schedule_interval = '0 2 * * *'

# Backfill: re-run for past dates (e.g., after fixing bug)
# airflow dags backfill daily_analytics_pipeline --start-date 2024-01-01 --end-date 2024-01-31
```

---

## 5. Monitoring & Observability

### Key Metrics

```python
class PipelineMetrics:
    """Track pipeline health."""

    def __init__(self):
        self.metrics = {}

    def record_metric(self, name, value, tags=None):
        """Record metric for monitoring."""
        self.metrics[name] = {
            'value': value,
            'timestamp': datetime.now(),
            'tags': tags or {}
        }

    def to_datadog(self):
        """Send to monitoring (Datadog, Prometheus, etc)."""
        return self.metrics

# Key metrics to track:
metrics = PipelineMetrics()
metrics.record_metric('ingest.rows_processed', 1000000, {'source': 'api'})
metrics.record_metric('ingest.duration_sec', 125, {'source': 'api'})
metrics.record_metric('ingest.error_rate', 0.002, {'source': 'api'})  # 0.2%
metrics.record_metric('quality.pass_rate', 0.998, {'gate': 'orders'})
metrics.record_metric('warehouse.freshness_min', 45, {'table': 'fact_orders'})
```

### Alert Conditions

```python
# Alert if:
- Pipeline takes > 5 hours (SLA violation)
- Data quality < 95% (bad data entering warehouse)
- > 5% duplicate detection (data source issue)
- Warehouse not refreshed in 6 hours (pipeline hung)
- > 10 consecutive failures (systemic issue)
```

---

## Interview Question

**Q: Design a data pipeline ingesting 500GB daily from 20 sources for 50 analysts with 4-hour latency.**

**Good Answer:**

1. **Architecture:** Batch pipeline (Airflow orchestration)
   - Ingestion layer: API/file connectors with async/parallel fetching
   - Validation: Schema checks, deduplication, quality gates
   - Transform: Spark for distributed processing
   - Load: Idempotent merge to warehouse

2. **Reliability:**
   - Partial failure handling (continue if 1-2 sources fail)
   - Retry with backoff on transient failures
   - Data quality gates prevent bad data

3. **Monitoring:**
   - Track: rows processed, duration, error rate, quality score
   - Alert on: SLA miss, data quality drop, pipeline hang

4. **Idempotency:**
   - Use merge/upsert instead of insert
   - Deduplication by unique key
   - Can safely re-run failed tasks

**Metrics:**
- Ingest time: 2-3 hours (leaves buffer for 4-hour SLA)
- Success rate: 99.5%
- Data quality: 98%+
- Cost: ~$2-3k/month (RI on compute, S3 storage)
