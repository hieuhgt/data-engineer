# Data Pipeline Project - Complete Walkthrough Guide

This guide walks you through every part of the project step-by-step, explaining what each component does and how to interact with it.

---

## 📖 Table of Contents

1. [Project Overview](#project-overview)
2. [Local Development Setup](#local-development-setup)
3. [Component Walkthrough](#component-walkthrough)
4. [Running Your First Pipeline](#running-your-first-pipeline)
5. [Understanding the Code](#understanding-the-code)
6. [Scaling to Production](#scaling-to-production)
7. [Common Tasks](#common-tasks)

---

## 🎯 Project Overview

This project demonstrates a **production-ready data engineering pipeline** with:

- **Data Ingestion**: Kafka for real-time event streaming
- **Processing**: PySpark for ETL transformations
- **Orchestration**: Apache Airflow to schedule and monitor jobs
- **Storage**: S3 data lake with medallion architecture (bronze/silver/gold)
- **Deployment**: Kubernetes (EKS) for scalable production infrastructure

### High-Level Flow

```
1. Events arrive in Kafka
2. Airflow scheduler triggers pipeline daily
3. Spark job reads from Kafka → Bronze layer (raw)
4. Spark job cleans data → Silver layer (processed)
5. Spark job aggregates data → Gold layer (analytics-ready)
6. Data quality checks validate results
7. Alerts sent on success/failure
```

---

## 🚀 Local Development Setup

### Step 1: Clone and Prepare

```bash
cd /Users/hieuht/workspace/personal/data-engineer

# Copy environment template
cp .env.example .env

# Verify Python version
python --version  # Should be 3.9+

# Create virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate
```

### Step 2: Install Dependencies (Monorepo)

This project uses a **monorepo structure** with separate requirements for each component.

```bash
# Option A: Install only specific components
./scripts/install.sh --spark          # Just Spark
./scripts/install.sh --airflow        # Just Airflow
./scripts/install.sh --kafka          # Just Kafka

# Option B: Install all components (recommended for first time)
./scripts/install.sh --all

# Option C: Install all + development tools
./scripts/install.sh --dev

# Verify installation
./scripts/verify-deps.sh

# Output should show:
# Core Dependencies:
#   dotenv... ✓
#   loguru... ✓
# Spark Dependencies:
#   pyspark... ✓
# ... (etc)
```

**What is Monorepo?**
- Each component has its own `requirements.txt`
- Base dependencies shared in `requirements/base.txt`
- See: `MONOREPO_STRUCTURE.md` for details
- Benefits: Install only what you need, cleaner dependencies, better CI/CD

### Step 3: Start Services with Docker Compose

```bash
# Start all services (each uses its monorepo requirements)
docker-compose -f docker-compose.full.yml up -d

# Monitor startup (wait ~30 seconds for all services to be healthy)
docker-compose -f docker-compose.full.yml ps

# Expected services:
# zookeeper           ✓ healthy
# kafka               ✓ healthy
# postgres            ✓ healthy
# airflow-webserver   ✓ healthy
# airflow-scheduler   ✓ healthy
# spark               ✓ running
# prometheus          ✓ running
# grafana             ✓ running

# Watch logs if needed
docker-compose -f docker-compose.full.yml logs -f
```

**Docker Compose also uses Monorepo**:
- Each service Dockerfile specifies component requirements
- Spark uses `spark/requirements.txt`
- Airflow uses `airflow/requirements.txt`
- etc.

### Step 4: Access Web Interfaces

Open these in your browser:

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | airflow / airflow |
| **Spark Master** | http://localhost:8081 | (no auth) |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | (no auth) |

---

## 🔧 Component Walkthrough

### 1. Configuration Management (`config.py`)

**Purpose**: Centralize all configuration in one place

```python
# What it does:
from config import get_config

config = get_config()
print(config.spark.master)        # "local[4]"
print(config.kafka.bootstrap_servers)  # ["localhost:9092"]
print(config.debug)               # False

# Why it matters:
# - No hardcoded values scattered in code
# - Same code works in dev/staging/prod with different .env files
# - Easy to override for testing
```

**How to use**:
```bash
# Modify .env file for different configurations
SPARK_MASTER=local[4]          # Local development
# or
SPARK_MASTER=spark://master:7077  # Spark cluster
```

### 2. Logging Setup (`logger_setup.py`)

**Purpose**: Structured logging with rotation and formatting

```python
from loguru import logger

logger.info("Processing started")         # Console + file
logger.error("Failed to connect to DB")   # Errors logged and visible
```

**Why it matters**:
- Logs saved to `logs/pipeline.log` with rotation
- Structured format includes timestamp, level, function, line number
- No lost logs due to file size (automatic rotation)

### 3. Kafka Components

#### Producer (`kafka/producers/event_producer.py`)

**Purpose**: Send test events into Kafka for processing

```bash
# Run it
python kafka/producers/event_producer.py

# What it does:
# - Creates sample user events with user_id, event_type, value
# - Sends to 'events' topic in Kafka
# - Demonstrates error handling and retries

# Example output:
# ✓ Message sent to events partition 0
# Sent 100/100 events
```

#### Consumer (`kafka/consumers/event_consumer.py`)

**Purpose**: Read events from Kafka (useful for testing)

```bash
# Consume 10 messages
python kafka/consumers/event_consumer.py

# What it does:
# - Connects to 'events' topic
# - Prints incoming messages
# - Handles disconnections gracefully
```

### 4. Spark Jobs

All jobs follow same pattern:
1. Read data (Kafka/S3)
2. Transform
3. Write output
4. Log results

#### Job 1: Ingest Kafka to Bronze (`spark/jobs/ingest_kafka.py`)

```
Kafka → Bronze Layer (s3://bucket/bronze/date/)
```

**What it does**:
```python
# 1. Connect to Kafka
df = spark.read.format("kafka").load()

# 2. Parse JSON events
df = parse_kafka_events(df)  # Extract user_id, event_type, etc.

# 3. Deduplicate
df = df.dropDuplicates(["user_id", "event_type", "timestamp"])

# 4. Add metadata and write
df.write.parquet(bronze_path)
```

**Example output**:
```
Reading Kafka topic: events
Read 1000 events
Writing 950 records to s3://datalake/bronze/events/2024-04-22/
✓ Ingestion completed successfully
```

#### Job 2: Transform Bronze to Silver (`spark/jobs/bronze_to_silver.py`)

```
Bronze Layer → Silver Layer (Cleaned & Deduplicated)
```

**Transformations applied**:
```python
# 1. Clean data
.withColumn("user_id", trim(col("user_id")))
.withColumn("event_type", lower(col("event_type")))

# 2. Handle nulls
.withColumn("value", coalesce(col("value"), 0.0))

# 3. Validate
.filter(col("user_id").isNotNull())
.filter(col("user_id") != "")

# 4. Deduplicate
.dropDuplicates(["user_id", "event_type", "timestamp"])
```

**Example output**:
```
Read 950 records from Bronze
After cleaning: 945 records
After deduplication: 930 records
Writing Silver data to s3://datalake/silver/events/2024-04-22/
✓ Transformation completed
```

#### Job 3: Aggregate Silver to Gold (`spark/jobs/silver_to_gold.py`)

```
Silver Layer → Gold Layer (Aggregated & Optimized)
```

**Aggregations created**:
```python
# 1. Daily metrics by event type
daily_metrics = df.groupBy("date", "event_type").agg(
    count("*").alias("event_count"),
    sum("value").alias("total_value"),
    avg("value").alias("avg_value")
)

# 2. Per-user metrics
user_metrics = df.groupBy("date", "user_id").agg(
    count("*").alias("user_event_count"),
    sum("value").alias("user_total_value")
).join(user_segments, on="user_id")

# 3. Trend analysis
event_trends = df.groupBy("event_type", "date").agg(...)
```

**Example output**:
```
Read 930 records from Silver
Creating daily metrics
Creating user metrics
Writing to:
  s3://datalake/gold/daily_metrics/2024-04-22/
  s3://datalake/gold/user_metrics/2024-04-22/
  s3://datalake/gold/event_trends/2024-04-22/
✓ Silver→Gold transformation completed
```

### 5. Airflow DAGs (Orchestration)

#### Main DAG: `daily_data_pipeline_dag.py`

**Purpose**: Schedule and monitor daily pipeline execution

```python
# DAG Definition
DAG(
    'daily_data_pipeline',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    sla=timedelta(hours=4),         # Must finish within 4 hours
)
```

**Task Flow**:
```
Validate Inputs
    ↓
Ingest Kafka → Bronze
    ↓
Transform Bronze → Silver
    ↓
Aggregate Silver → Gold
    ↓
Quality Checks
    ↓
Success/Failure Notification
```

### 6. Kubernetes Manifests

**Purpose**: Deploy to EKS (AWS Kubernetes)

Key files:
- `kubernetes/namespace.yaml` - Create data-pipeline namespace
- `kubernetes/airflow/` - Airflow webserver, scheduler, PostgreSQL
- `kubernetes/spark/` - Spark RBAC for job execution
- `kubernetes/configmaps/` - Configuration & secrets

---

## ▶️ Running Your First Pipeline

### Scenario: Process Events and Check Results

#### Step 1: Produce Test Events

```bash
# In one terminal, keep running producer to generate events
python kafka/producers/event_producer.py

# Output:
# Message sent to events partition 0 (offset 0)
# Message sent to events partition 0 (offset 1)
# ... (100 events total)
# Sent 100/100 events
# Producer closed
```

#### Step 2: Trigger Airflow DAG

```bash
# Option A: Via Web UI
# 1. Go to http://localhost:8080
# 2. Find "daily_data_pipeline" DAG
# 3. Toggle it ON (switch appears)
# 4. Click "Trigger DAG" button
# 5. Select today's date as execution date

# Option B: Via CLI
airflow dags trigger daily_data_pipeline --exec-date 2024-04-22
```

#### Step 3: Monitor Execution

```bash
# Watch DAG runs in Airflow UI
# http://localhost:8080/dags/daily_data_pipeline

# Expected progress:
# 1. validate_inputs - RUNNING → SUCCESS ✓
# 2. ingest_kafka_to_bronze - RUNNING (5-10 mins)
# 3. bronze_to_silver - RUNNING (3-5 mins)
# 4. silver_to_gold - RUNNING (2-3 mins)
# 5. quality_checks - RUNNING (1 min)
# 6. success_notification - SUCCESS ✓

# Check task logs
# Click on task → Logs tab
```

#### Step 4: Verify Output

```bash
# Check what was created
# Local development: check logs/pipeline.log
tail -f logs/pipeline.log

# Expected messages:
# 1 [INFO] Reading Kafka topic: events
# 2 [INFO] Read 100 records from Kafka
# 3 [INFO] After cleaning: 98 records
# 4 [INFO] Writing to s3://datalake/silver/events/2024-04-22/
# 5 [INFO] Data quality checks passed
# 6 [INFO] ✓ Pipeline completed successfully
```

---

## 💡 Understanding the Code

### Key Python Concepts Used

#### 1. Configuration with Dataclasses

```python
# config.py uses dataclasses for type-safe config
from dataclasses import dataclass

@dataclass
class SparkConfig:
    app_name: str = "data_pipeline"
    master: str = "local[4]"
    memory: str = "2g"

# Benefits:
# - Type hints catch errors early
# - Easy to read and understand
# - Can be serialized/validated
```

#### 2. PySpark DataFrame Transformations

```python
# Transform data through a pipeline
df = (
    spark.read.parquet("bronze/")
    .filter(col("value") > 0)                      # Filter rows
    .withColumn("user_id", trim(col("user_id")))  # Transform column
    .dropDuplicates(["user_id", "event_id"])       # Deduplicate
    .groupBy("event_type")                         # Group
    .agg({"value": "sum"})                         # Aggregate
    .write.parquet("silver/")                      # Write output
)
```

**Why this pattern**:
- Lazy evaluation (doesn't execute until `.write()`)
- Optimized by Spark's Catalyst optimizer
- Works on distributed data

#### 3. Kafka Producer/Consumer Pattern

```python
# Producer (send events)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('events', value={"user_id": "123", "event": "click"})

# Consumer (read events)
consumer = KafkaConsumer('events', bootstrap_servers=['localhost:9092'])
for msg in consumer:
    print(msg.value)  # Process message
```

**Why this pattern**:
- Decouples data source from processing
- Scalable (multiple consumers can process same events)
- Reliable (Kafka stores messages)

#### 4. Airflow DAG (Directed Acyclic Graph)

```python
# Define pipeline as code
with DAG('daily_data_pipeline') as dag:
    validate = PythonOperator(task_id='validate_inputs', ...)
    ingest = SparkSubmitOperator(task_id='ingest_kafka', ...)
    transform = SparkSubmitOperator(task_id='bronze_to_silver', ...)

    # Define dependencies
    validate >> ingest >> transform
```

**Why this pattern**:
- Pipeline defined as code (version controllable)
- Dependencies explicit
- Easy to test and debug
- Scheduling built-in

### Common Tasks & Code Locations

| Task | File | Key Function |
|------|------|--------------|
| Change Spark config | `config.py` | `SparkConfig` class |
| Add Spark transformation | `spark/jobs/bronze_to_silver.py` | `clean_data()` |
| Change DAG schedule | `airflow/dags/daily_data_pipeline_dag.py` | `schedule_interval` |
| Modify Kafka settings | `config.py` | `KafkaConfig` class |
| Add new task to DAG | `airflow/dags/daily_data_pipeline_dag.py` | Create new Operator + add to dag |

---

## 🏗️ Scaling to Production

### Migration Path: Local → EKS

#### Phase 1: Prepare Docker Images

```bash
# Build Spark image
cd spark
docker build -t your-registry/spark-etl:v1.0 .
docker push your-registry/spark-etl:v1.0

# Airflow uses official image, just configure
```

#### Phase 2: Create EKS Cluster

```bash
# Create cluster
eksctl create cluster --name data-pipeline --region us-east-1 --nodes 3

# Verify
kubectl get nodes
```

#### Phase 3: Deploy to EKS

```bash
# 1. Create namespace
kubectl apply -f kubernetes/namespace.yaml

# 2. Create secrets
kubectl create secret generic aws-credentials \
  --from-literal=access-key=YOUR_KEY \
  --from-literal=secret-key=YOUR_SECRET \
  -n data-pipeline

# 3. Deploy PostgreSQL (Airflow metadata DB)
kubectl apply -f kubernetes/airflow/postgres-statefulset.yaml

# 4. Deploy Airflow
kubectl apply -f kubernetes/airflow/airflow-deployment.yaml

# 5. Deploy Spark RBAC
kubectl apply -f kubernetes/spark/spark-rbac.yaml

# 6. Verify
kubectl get all -n data-pipeline
```

#### Phase 4: Configure Airflow

```bash
# Port forward to access UI
kubectl port-forward svc/airflow-webserver 8080:8080 -n data-pipeline

# Go to http://localhost:8080
# Create new connections/variables for your environment
```

### Performance Tuning

```python
# In spark/jobs/bronze_to_silver.py

# Increase parallelism for large datasets
.config("spark.sql.shuffle.partitions", "500")

# Cache data if reused
df_cached = df.cache()
result1 = df_cached.filter(...).agg(...)
result2 = df_cached.groupBy(...).agg(...)

# Broadcast small dimensions
large_df.join(broadcast(small_df), "id")
```

---

## 🛠️ Common Tasks

### Task 1: Add a New Data Source

**Goal**: Process purchase events from a different Kafka topic

**Steps**:

1. Create new Spark job:
```bash
cp spark/jobs/ingest_kafka.py spark/jobs/ingest_purchases.py
```

2. Modify `ingest_purchases.py`:
```python
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'purchases')
# Schema for purchases is different - update parse function
```

3. Add to DAG (`airflow/dags/daily_data_pipeline_dag.py`):
```python
ingest_purchases = SparkSubmitOperator(
    task_id='ingest_purchases_to_bronze',
    application=f'{SPARK_JOBS_PATH}/ingest_purchases.py',
    env_vars={'KAFKA_TOPIC': 'purchases', ...}
)

# Add to dependency chain
validate_data >> [ingest_kafka_to_bronze, ingest_purchases] >> bronze_to_silver
```

4. Test:
```bash
airflow dags test daily_data_pipeline 2024-04-22
```

### Task 2: Add Data Quality Check

**Goal**: Validate row count before and after transformation

**Steps**:

1. Add to DAG:
```python
def check_row_count(**context):
    """Verify data counts match expectations."""
    # Read Silver layer
    df = spark.read.parquet("s3://datalake/silver/...")
    count = df.count()

    if count == 0:
        raise AirflowException("No data in Silver layer!")

    logger.info(f"✓ Silver layer has {count} records")

quality_check = PythonOperator(
    task_id='quality_check',
    python_callable=check_row_count,
)

# Add to DAG
silver_to_gold >> quality_check >> success_notification
```

### Task 3: Change DAG Schedule

**Goal**: Run pipeline every 6 hours instead of daily

**Steps**:

1. Edit `airflow/dags/daily_data_pipeline_dag.py`:
```python
with DAG(
    'daily_data_pipeline',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    ...
)
```

2. Refresh in Airflow UI (it auto-detects changes)

3. Old runs automatically backfilled

### Task 4: Debug Pipeline Failure

**Scenario**: Pipeline fails at `bronze_to_silver` stage

**Steps**:

1. Check logs:
```bash
# In Airflow UI
# Click "bronze_to_silver" task → Logs

# Or via CLI
airflow tasks log daily_data_pipeline bronze_to_silver 2024-04-22
```

2. Check Spark UI:
```bash
# While job is running
# http://localhost:4040 (local)
# or
# kubectl port-forward pod/spark-job 4040:4040 (Kubernetes)
```

3. Investigate data:
```bash
# Read intermediate data to inspect
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("s3://datalake/bronze/events/2024-04-22/")
df.show()
df.printSchema()
df.describe().show()
```

4. Fix issue and rerun:
```bash
# Airflow automatically retries based on config
# Or manually trigger again
```

---

## 📊 Example: Complete Day in Pipeline Life

**08:00 AM** - Airflow scheduler starts checking for jobs to run

**01:00 AM (next day)** - DAG trigger fires:
```
[01:00] validate_inputs: ✓ Input data available
[01:02] ingest_kafka_to_bronze: ✓ Processed 10,000 events
[01:15] bronze_to_silver: ✓ Cleaned to 9,800 records
[01:25] silver_to_gold: ✓ Created 3 aggregation tables
[01:27] quality_checks: ✓ All checks passed
[01:28] success_notification: ✓ Team notified
```

**Result** - Data available for analysts:
- Daily metrics in `gold/daily_metrics/`
- User segments in `gold/user_metrics/`
- Trends in `gold/event_trends/`

**Analytics team** uses data for:
- Dashboards (Grafana/Tableau)
- ML features
- Business reports

---

## 📚 Further Learning

1. **Understand Spark better**:
   - Read: `docs/PIPELINE_ARCHITECTURE.md` - Medallion layer explanation
   - Experiment: Modify `spark/jobs/silver_to_gold.py` aggregations
   - Test: Write your own Spark job

2. **Master Airflow**:
   - Read: `BEST_PRACTICES.md` - Airflow patterns section
   - Task: Create new DAG for different data source
   - Explore: Airflow UI → Admin → Variables & Connections

3. **Deploy to production**:
   - Read: `docs/DEPLOYMENT_GUIDE.md` - EKS setup guide
   - Practice: Deploy to staging EKS cluster
   - Monitor: Setup CloudWatch alerts

4. **Optimize pipeline**:
   - Profile: Use Spark UI to identify bottlenecks
   - Tune: Adjust partitions, batch sizes, cache
   - Test: Run benchmarks before/after changes

---

## 📚 Learning Pandas for Data Manipulation

While Spark handles large-scale processing, **Pandas** is essential for:
- Data cleaning and preparation
- Exploratory data analysis (EDA)
- Small to medium dataset handling
- Integration with Spark workflows

### Getting Started with Pandas

```bash
# Read the comprehensive guide
cat pandas/README.md

# Follow the 20-week learning path
cat pandas/PANDAS_LEARNING_PATH.md

# Run working examples
python pandas/examples/level1_fundamentals.py
python pandas/examples/level2_intermediate.py
python pandas/examples/level3_advanced.py
python pandas/examples/level4_mastery.py
```

### Pandas Materials Included

- **PANDAS_COMPLETE_GUIDE.md** - 4-level reference (fundamentals → mastery)
- **PANDAS_LEARNING_PATH.md** - 20-week structured learning roadmap
- **pandas/examples/** - Runnable code examples for each level

### Using Pandas in Your Pipeline

```python
# Example: Load and clean data before sending to Kafka
import pandas as pd

df = pd.read_csv('raw_events.csv')
df = df.dropna()                    # Remove missing values
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df[df['value'] > 0]            # Filter invalid data

# Convert to Kafka format
for _, row in df.iterrows():
    producer.send('events', value=row.to_dict())
```

### When to Use Pandas vs Spark

| Scenario | Use |
|----------|-----|
| Large distributed data (100M+ rows) | Spark |
| Local data exploration & analysis | Pandas |
| Data cleaning before pipelines | Pandas |
| Aggregations across millions of records | Spark |
| Quick experiments & prototyping | Pandas |

---

## 🎓 Quick Reference

### File to Edit For...

| Goal | File |
|------|------|
| Change spark memory | `config.py` (SparkConfig) |
| Add transformation step | `spark/jobs/bronze_to_silver.py` |
| Change pipeline schedule | `airflow/dags/daily_data_pipeline_dag.py` |
| Modify Kafka topic | `.env` (KAFKA_TOPIC) |
| Change output location | `airflow/dags/` (env_vars) |
| Add monitoring alert | `kubernetes/configmaps/` |

### Docker Compose Commands

```bash
# Start services
docker-compose -f docker-compose.full.yml up -d

# View logs
docker-compose -f docker-compose.full.yml logs -f <service>
# Services: zookeeper, kafka, postgres, airflow-webserver, airflow-scheduler, spark

# Stop services
docker-compose -f docker-compose.full.yml down

# Stop and remove data
docker-compose -f docker-compose.full.yml down -v
```

### Kubernetes Commands

```bash
# Namespace operations
kubectl get namespaces
kubectl get all -n data-pipeline

# Pod operations
kubectl describe pod <pod-name> -n data-pipeline
kubectl logs -f <pod-name> -n data-pipeline
kubectl exec -it <pod-name> -n data-pipeline -- /bin/bash

# Troubleshooting
kubectl get events -n data-pipeline
kubectl get pvc -n data-pipeline
```

---

**Next Step**: Open http://localhost:8080 and trigger your first pipeline! 🚀
