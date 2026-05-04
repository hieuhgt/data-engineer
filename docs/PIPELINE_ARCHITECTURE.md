# Data Pipeline Architecture & Best Practices

## 1. Pipeline Architecture Overview

### Typical Modern Data Pipeline Flow

```
Data Source (Events/APIs/Databases)
    ↓
Kafka (Message Queue / Event Streaming)
    ↓
Spark (Processing / Transformations)
    ↓
Data Lake (Parquet/Delta Lake)
    ↓
Data Warehouse (Analytics / BI)
    ↓
Dashboards / ML Models
```

### Detailed Flow with Airflow Orchestration

```
┌─────────────────────────────────────────────────────────────────┐
│                    AIRFLOW ORCHESTRATION                        │
│                  (Scheduling & Monitoring)                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│ START DAG                                                        │
├──────────────────────────────────────────────────────────────────┤
│ 1. Validate Input Data                                          │
│    - Check data quality                                         │
│    - Validate schemas                                           │
│    - Set SLAs                                                   │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│ INGEST LAYER                                                     │
├──────────────────────────────────────────────────────────────────┤
│ 2a. Kafka Consumer           │ 2b. API/Database Reader         │
│    - Read events             │    - Batch read                 │
│    - Deserialize JSON/Avro   │    - Handle pagination          │
│    - Validate records        │    - Incremental load (CDC)     │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│ PROCESSING LAYER (Spark Jobs)                                   │
├──────────────────────────────────────────────────────────────────┤
│ 3. Spark ETL Stage 1         │ 4. Spark ETL Stage 2           │
│    - Parse & Clean           │    - Enrich data               │
│    - Deduplication           │    - Join with dimensions      │
│    - Type casting            │    - Calculate metrics         │
│    - Remove nulls            │                                │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│ STORAGE LAYER                                                    │
├──────────────────────────────────────────────────────────────────┤
│ 5. Data Lake (Raw)           │ 6. Data Lake (Processed)       │
│    - Bronze Layer            │    - Silver Layer              │
│    - s3://bucket/raw/        │    - s3://bucket/processed/    │
│    - Immutable              │    - Deduplicated              │
│                             │                                │
│ 7. Data Warehouse            │ 8. Cache Layer                │
│    - Gold Layer              │    - Redis/Memcached          │
│    - s3://bucket/gold/       │    - For hot queries          │
│    - Aggregated             │                                │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│ QUALITY & VALIDATION                                            │
├──────────────────────────────────────────────────────────────────┤
│ 9. Data Quality Checks                                         │
│    - Row counts match        - No unexpected nulls            │
│    - Schema validation       - Statistical anomalies         │
│    - SLA monitoring         - Freshness checks               │
└──────────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────────┐
│ ANALYTICS & CONSUMPTION                                         │
├──────────────────────────────────────────────────────────────────┤
│ 10. BI / ML / Dashboards                                        │
│     - Looker / Tableau       - ML Training Pipelines           │
│     - Jupyter Notebooks      - Real-time Analytics             │
└──────────────────────────────────────────────────────────────────┘
                              ↓
                     Alert if failures
```

## 2. Data Pipeline Layers Explained

### Bronze Layer (Raw Data)
- **Purpose**: Immutable source of truth
- **Format**: Parquet (preserves original schema)
- **Retention**: Long-term (cold storage after 1 year)
- **SLA**: Best effort, failures don't block downstream
- **Example**: `s3://company-datalake/bronze/events/2024-04-22/`

### Silver Layer (Cleaned & Processed)
- **Purpose**: Business-ready data, lightly transformed
- **Format**: Parquet (compressed, partitioned)
- **Retention**: 2-3 years active, then archive
- **SLA**: 4-hour freshness SLA
- **Transformations**:
  - Deduplication
  - Type casting
  - Null handling
  - Validation
- **Example**: `s3://company-datalake/silver/user_events/2024-04-22/`

### Gold Layer (Aggregated & Optimized)
- **Purpose**: Ready for analytics/BI/ML
- **Format**: Parquet (highly optimized for queries)
- **Retention**: 1 year active
- **SLA**: 2-hour freshness SLA
- **Aggregations**:
  - Daily summaries
  - User segments
  - Metrics
- **Example**: `s3://company-datalake/gold/daily_metrics/date=2024-04-22/`

## 3. Technology Stack Breakdown

### Data Ingestion
```python
# Option 1: Kafka (Real-time streams)
kafka_cluster → topics → consumer group → Spark Streaming

# Option 2: Batch (APIs, Databases)
jdbc_driver → spark.read.jdbc() → DataFrame

# Option 3: File-based
s3/gcs → spark.read.parquet/csv() → DataFrame
```

### Processing (Spark)
```python
# Key operations
df.read → filter → join → agg → window → write

# Optimization
- Repartition key columns
- Cache intermediate results
- Enable adaptive execution
- Broadcast small tables
```

### Orchestration (Airflow)
```
Airflow DAG
├── Task 1: Validate input
├── Task 2: Spark submit job
├── Task 3: Quality checks
└── Task 4: Send notifications
```

### Storage
```
S3 (Data Lake)
├── /bronze/  - Raw data
├── /silver/  - Processed
└── /gold/    - Analytics-ready

Warehouse (BigQuery/Redshift/Snowflake)
├── Raw tables (from gold layer)
├── Aggregated views
└── ML feature tables
```

## 4. Best Practice Patterns

### Pattern 1: Medallion Architecture (Bronze-Silver-Gold)

**When to use**: Large-scale data lakes with multiple downstream consumers

```
Raw Data → Bronze (as-is)
         → Silver (cleaned)
         → Gold (optimized)
         → BI/ML/Dashboards
```

**Advantages**:
- Data lineage is clear
- Easy to reprocess from bronze
- Incremental improvements possible
- Supports multiple use cases from same data

### Pattern 2: Lambda Architecture (Batch + Real-time)

**When to use**: Need both historical accuracy and real-time insights

```
Events → Kafka → Spark Streaming → Real-time views
      ↓
      → Spark Batch Jobs → Historical views
      ↓
      → Merge views for complete picture
```

**Advantages**:
- Real-time insights with batch accuracy
- Can correct real-time data in batch layer

### Pattern 3: Kappa Architecture (Streaming Only)

**When to use**: Real-time data, can reprocess from Kafka

```
Events → Kafka → Spark Streaming → Gold Layer
      (replay from beginning if needed)
```

**Advantages**:
- Simpler (no dual systems)
- True real-time processing
- Requires Kafka retention policy

## 5. Orchestration with Airflow

### DAG Structure

```python
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=4),  # Must complete within 4 hours
}

with DAG('daily_data_pipeline', default_args=default_args) as dag:
    # Tasks defined here
    pass
```

### Task Dependencies

```python
validate_data >> ingest_data >> process_data >> quality_checks >> notify
```

### Key Airflow Features for Data Pipelines
- **SLA Monitoring**: Alert if task exceeds time threshold
- **Backfill**: Reprocess historical data
- **Branching**: Conditional task execution
- **Sensor**: Wait for external events (file arrival, data quality)
- **XCom**: Pass data between tasks

## 6. Deployment Architecture

### Local Development
```
Your Machine
├── Docker Compose (Kafka + Zookeeper + Spark + Airflow)
├── Python venv
└── Testing
```

### Staging/Production on EKS
```
AWS EKS Cluster
├── Kafka Cluster (3+ nodes, StatefulSet)
├── Airflow (Master + Workers)
│   ├── Webserver
│   ├── Scheduler
│   ├── Worker pods (KubernetesExecutor)
│   └── PostgreSQL (metadata DB)
├── Spark (on-demand jobs via spark-submit)
├── Monitoring (Prometheus + Grafana)
└── Logging (CloudWatch / ELK)
```

## 7. Error Handling & Recovery

### Idempotency (Critical!)
Every task should be safe to re-run:

```python
# Good - Spark write with idempotent mode
df.write.mode("overwrite").parquet("s3://path/date=2024-04-22/")

# Bad - Spark write append (creates duplicates on retry)
df.write.mode("append").parquet("s3://path/")
```

### Fault Tolerance
```
If Kafka consumer fails:
  - Offset stored in Kafka cluster
  - Restart consumer from last offset

If Spark job fails:
  - Retry via Airflow
  - Idempotent writes prevent duplicates
  - DLQ (Dead Letter Queue) for unprocessable records

If Airflow task fails:
  - Retry policy (retry N times)
  - Notify on failure (email, Slack)
  - Manual rerun possible via backfill
```

### Dead Letter Queue (DLQ) Pattern

```python
# Process events, catch errors
try:
    process_event(event)
except Exception as e:
    logger.error(f"Failed to process: {e}")
    # Send to DLQ for manual inspection
    send_to_dlq(event, error=str(e))
```

## 8. Performance Optimization

### Spark Optimization
```python
# 1. Right-size partitions
df.repartition(500)  # 128-256MB per partition

# 2. Cache intermediate results
df_cached = df.cache()
result1 = df_cached.filter(...).agg(...)
result2 = df_cached.filter(...).join(...)

# 3. Broadcast small tables
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "id")

# 4. Enable AQE
.config("spark.sql.adaptive.enabled", "true")

# 5. Parallelize I/O
df.write.option("maxRecordsPerFile", 100000).parquet("path")
```

### Airflow Optimization
```python
# 1. Pool limits (prevent resource exhaustion)
my_pool = Pool(pool_name='spark_jobs', slots=5)

# 2. Task concurrency
dag = DAG(..., concurrency=10)  # Max 10 tasks in parallel

# 3. Sensor poke strategy
BaseSensorOperator(poke_interval=60, timeout=3600)

# 4. Use SubDAGs for reuse
sub_dag = SubDagOperator(task_id='process', subdag=process_subdag(...))
```

## 9. Monitoring & Alerting

### Key Metrics to Monitor
```
1. Pipeline Health
   - SLA success rate
   - Retry frequency
   - Task duration trends

2. Data Quality
   - Row counts (expected vs actual)
   - Null percentages
   - Data freshness

3. Resource Usage
   - Spark executor memory/CPU
   - Airflow task queue depth
   - Storage growth rate

4. Business KPIs
   - Data latency
   - Processing cost per GB
   - Pipeline run duration
```

### Alerting
```python
from airflow.operators.slack_operator import SlackAPIPostOperator

notify_on_failure = SlackAPIPostOperator(
    task_id='notify_failure',
    text='Pipeline failed: {{ dag.dag_id }} - {{ task.task_id }}',
    trigger_rule='one_failed',
)
```

## 10. Testing Strategy

### Unit Tests
- Test Spark transformations with sample data
- Mock external dependencies
- Use pytest with SparkSession fixtures

### Integration Tests
- Test with real Kafka topic (test environment)
- End-to-end pipeline with sample data
- Validate schemas and transformations

### Data Quality Tests
- Great Expectations framework
- Validate row counts, nulls, data types
- Run post-transformation

```python
# Example: Great Expectations
from great_expectations.dataset import SparkDataset

gx_df = SparkDataset(spark_df)
assert gx_df.expect_column_to_exist("user_id")
assert gx_df.expect_column_values_to_not_be_null("user_id")
assert gx_df.expect_column_values_to_be_in_set("status", ["active", "inactive"])
```

---

**Next**: See `DEPLOYMENT_GUIDE.md` for EKS deployment instructions.
