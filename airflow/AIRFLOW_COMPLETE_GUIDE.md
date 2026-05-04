# Apache Airflow Complete Guide: Junior to Senior Level

## Table of Contents
1. [LEVEL 1: JUNIOR](#level-1-junior)
2. [LEVEL 2: INTERMEDIATE](#level-2-intermediate)
3. [LEVEL 3: ADVANCED](#level-3-advanced)
4. [LEVEL 4: SENIOR](#level-4-senior)

---

# LEVEL 1: JUNIOR
## Fundamentals (Weeks 1-2)

### What is Apache Airflow?

**Definition:** Airflow is a platform to programmatically author, schedule, and monitor workflows.

**Why Airflow?**
```
Problem 1: Cron jobs don't handle dependencies well
  If job_a fails, job_b still runs (bad)

Solution: Airflow enforces dependencies
  job_a >> job_b  (job_b waits for job_a)

Problem 2: No monitoring or alerting
  Job fails silently, data never updates

Solution: Airflow monitors and alerts
  Failed tasks trigger notifications
  SLA monitoring (must complete in X hours)
  Retry logic (automatic recovery)

Problem 3: No visibility into workflow
  Did job run? When? Did it succeed?

Solution: Airflow UI
  See all DAGs, schedule, status, history
  Trigger manually if needed
  View logs from anywhere
```

### Airflow Architecture

```
                    Web Server
                   (UI, API)
                       ↑
                       │
        ┌──────────────┼──────────────┐
        │              │              │
    Scheduler      Workers          Database
    (decides)    (executes)      (state, logs)

Scheduler:
- Parses DAGs
- Creates task instances
- Submits to executor

Executor:
- LocalExecutor: Runs tasks locally (dev only)
- CeleryExecutor: Distributed (Kafka-like)
- KubernetesExecutor: Runs in K8s pods

Database:
- Stores DAG definitions
- Task execution history
- XCom messages
- User info
```

### DAG (Directed Acyclic Graph)

A DAG is a blueprint of your workflow:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'data-team',          # Who owns this
    'retries': 1,                  # Retry 1 time on failure
    'retry_delay': timedelta(minutes=5),  # Wait 5 min before retry
    'start_date': datetime(2024, 1, 1),   # When to start scheduling
}

# Create DAG
dag = DAG(
    'my_first_dag',                # Unique ID
    default_args=default_args,
    schedule_interval='@daily',    # Run every day at midnight
    description='My first DAG',
)
```

### Tasks and Operators

An **operator** is a task type. A **task** is an instance of an operator:

```python
# Operator = class
# Task = instance

# Python task
def print_hello():
    print("Hello from Airflow!")

task1 = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Bash task
from airflow.operators.bash import BashOperator

task2 = BashOperator(
    task_id='bash_task',
    bash_command='echo "Hello from bash"',
    dag=dag,
)

# SQL task
from airflow.operators.postgres_operator import PostgresOperator

task3 = PostgresOperator(
    task_id='create_table',
    sql='CREATE TABLE IF NOT EXISTS users (id INT, name STRING)',
    postgres_conn_id='postgres_default',
    dag=dag,
)
```

### Task Dependencies

Define which tasks run before others:

```python
# Method 1: >> operator (readable)
task1 >> task2  # task1 runs first, then task2
task2 >> task3

# Method 2: << operator
task3 << task2  # Same as: task2 >> task3

# Method 3: set_upstream / set_downstream
task2.set_upstream(task1)  # Same as: task1 >> task2
task2.set_downstream(task3)  # Same as: task2 >> task3

# Multiple dependencies
task1 >> task2 >> task3
task1 >> task4  # task4 also depends on task1

# Graph looks like:
#   task1
#   /   \
# task2 task4
#   |
# task3
```

### Scheduling

```python
from datetime import timedelta

# Simple schedules
schedule_interval='@daily'      # Every day at UTC midnight
schedule_interval='@hourly'     # Every hour
schedule_interval='@weekly'     # Every Monday at midnight
schedule_interval=None          # Manual trigger only

# Cron expressions
schedule_interval='0 2 * * *'   # Every day at 2 AM
schedule_interval='*/15 * * * *' # Every 15 minutes
schedule_interval='0 0 1 * *'   # First day of month at midnight

# Timedelta
schedule_interval=timedelta(hours=1)  # Every 1 hour
schedule_interval=timedelta(days=1)   # Every 1 day

# Important: First run happens AFTER start_date + schedule_interval
start_date = datetime(2024, 1, 1)
schedule_interval = timedelta(days=1)
# First run: 2024-01-02 (tomorrow at midnight UTC)
```

### Running DAGs Locally

```python
# File: ~/airflow/dags/my_dag.py

# Step 1: Test the DAG
airflow dags test my_dag 2024-01-01

# Step 2: List all DAGs
airflow dags list
# Should see: my_dag

# Step 3: Trigger DAG
airflow dags trigger my_dag

# Step 4: Check DAG run
airflow dags list-runs my_dag

# Step 5: Check task
airflow tasks list my_dag
airflow tasks list-runs my_dag 2024-01-01
```

### Basic Example: ETL DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    description='Simple ETL pipeline',
)

# Task 1: Extract
def extract_data():
    print("Extracting data from source...")
    import pandas as pd
    df = pd.read_csv('/data/input.csv')
    print(f"Extracted {len(df)} rows")

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Task 2: Transform
def transform_data():
    print("Transforming data...")
    # Load from previous task's output
    # Apply transformations
    # Save to intermediate file

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Task 3: Load
def load_data():
    print("Loading data to warehouse...")
    # Load from intermediate file
    # Insert to database

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Define dependencies
extract >> transform >> load
```

### Common Operators

```python
# PythonOperator
PythonOperator(
    task_id='my_task',
    python_callable=my_function,
    op_kwargs={'param1': 'value1'},  # Arguments to function
    dag=dag,
)

# BashOperator
BashOperator(
    task_id='bash_task',
    bash_command='echo "Hello" && python script.py',
    dag=dag,
)

# PostgresOperator
PostgresOperator(
    task_id='sql_task',
    sql='SELECT * FROM users LIMIT 10',
    postgres_conn_id='postgres_default',
    dag=dag,
)

# EmailOperator (send email)
EmailOperator(
    task_id='send_email',
    to='admin@company.com',
    subject='DAG Completed',
    html_content='Your DAG run completed successfully',
    dag=dag,
)

# DummyOperator (placeholder)
DummyOperator(
    task_id='dummy',
    dag=dag,
)
```

---

# LEVEL 2: INTERMEDIATE
## Error Handling & Complex Workflows (Weeks 3-5)

### Retries and Error Handling

```python
from datetime import timedelta

# Retry configuration
default_args = {
    'retries': 3,  # Retry 3 times on failure
    'retry_delay': timedelta(minutes=5),  # Wait 5 min before retry
    'retry_exponential_backoff': True,  # Exponential: 5, 10, 20 min
    'max_retry_delay': timedelta(hours=1),  # Max 1 hour between retries
}

# Task-specific retry
task = PythonOperator(
    task_id='my_task',
    python_callable=my_func,
    retries=5,  # Override default
    retry_delay=timedelta(minutes=1),
    dag=dag,
)

# Trigger rules (when should task run?)
# all_success: All upstream tasks succeeded (default)
# all_failed: All upstream tasks failed
# one_success: At least one upstream succeeded
# one_failed: At least one upstream failed
# none_failed: No upstream tasks failed

task_if_error = PythonOperator(
    task_id='handle_error',
    python_callable=error_handler,
    trigger_rule='one_failed',  # Run even if previous failed
    dag=dag,
)
```

### Monitoring and Alerting

```python
# On failure callback
def on_failure_callback(context):
    ti = context['task_instance']
    task_id = ti.task_id
    error_msg = str(context['exception'])
    print(f"Task {task_id} failed: {error_msg}")
    # Send email, Slack, etc.

# On success callback
def on_success_callback(context):
    ti = context['task_instance']
    print(f"Task {ti.task_id} succeeded!")

default_args = {
    'on_failure_callback': on_failure_callback,
    'on_success_callback': on_success_callback,
}

# SLA monitoring (must complete within X time)
dag = DAG(
    'my_dag',
    default_args={'sla': timedelta(hours=4)},  # Must finish in 4 hours
)

# On SLA miss: Airflow alerts immediately
```

### XCom (Cross-Communication)

Share data between tasks without external storage:

```python
# Task 1: Push data
def extract_and_count():
    count = 1000
    return {'row_count': count, 'status': 'success'}

task1 = PythonOperator(
    task_id='extract',
    python_callable=extract_and_count,
    dag=dag,
)

# Task 2: Pull data
def log_count(context):
    ti = context['task_instance']
    # Pull from task1
    data = ti.xcom_pull(task_ids='extract')
    print(f"Extracted {data['row_count']} rows")
    return data['row_count']

task2 = PythonOperator(
    task_id='log',
    python_callable=log_count,
    provide_context=True,  # Gives context to function
    dag=dag,
)

task1 >> task2

# XCom is good for small data (< 100 MB)
# For large data: Use files, database, or message queue
```

### Task Groups (Organize Complex DAGs)

```python
from airflow.utils.task_group import TaskGroup

with DAG('organized_dag', ...) as dag:
    # Create a group of related tasks
    with TaskGroup('extract_group') as extract_group:
        extract_api = PythonOperator(task_id='from_api', python_callable=...)
        extract_db = PythonOperator(task_id='from_db', python_callable=...)
        extract_file = PythonOperator(task_id='from_file', python_callable=...)

    with TaskGroup('transform_group') as transform_group:
        clean = PythonOperator(task_id='clean', python_callable=...)
        enrich = PythonOperator(task_id='enrich', python_callable=...)
        validate = PythonOperator(task_id='validate', python_callable=...)

    load = PythonOperator(task_id='load', python_callable=...)

    # Groups as dependencies
    extract_group >> transform_group >> load

# Graph looks like:
# [extract_group] → [transform_group] → [load]
# Inside extract_group:
#   - from_api
#   - from_db
#   - from_file
```

### Branching (Conditional Logic)

```python
from airflow.operators.python import BranchPythonOperator

def decide_path(context):
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='check_data')

    if data['rows'] > 1000:
        return 'process_large'  # Return task_id
    else:
        return 'process_small'

# Branch task
branch = BranchPythonOperator(
    task_id='branch_logic',
    python_callable=decide_path,
    provide_context=True,
    dag=dag,
)

process_large = PythonOperator(task_id='process_large', python_callable=...)
process_small = PythonOperator(task_id='process_small', python_callable=...)
merge = PythonOperator(task_id='merge', trigger_rule='one_success', python_callable=...)

branch >> [process_large, process_small] >> merge
```

### Sensors (Wait for Something)

```python
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.sql import SqlSensor

# Wait for S3 file
wait_for_s3 = S3KeySensor(
    task_id='wait_for_s3_file',
    bucket_name='my-bucket',
    bucket_key='data/input.parquet',
    poke_interval=30,  # Check every 30 seconds
    timeout=3600,  # Give up after 1 hour
    mode='poke',  # or 'reschedule' (less resource intensive)
    dag=dag,
)

# Wait for file on disk
wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='/data/ready.txt',
    poke_interval=10,
    timeout=300,
    dag=dag,
)

# Wait for SQL condition
wait_for_sql = SqlSensor(
    task_id='wait_for_data',
    sql='SELECT COUNT(*) FROM staging WHERE processed = 0',
    conn_id='postgres_default',
    success_condition='COUNT(*) > 0',  # Success when this SQL is true
    poke_interval=60,
    timeout=1800,
    dag=dag,
)
```

---

# LEVEL 3: ADVANCED
## Production Systems (Weeks 6-12)

### Orchestrating Spark Jobs

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_task = SparkSubmitOperator(
    task_id='run_spark_job',
    application='/home/user/spark_jobs/transform.py',
    conf={
        'spark.executor.memory': '4g',
        'spark.executor.cores': 4,
        'spark.driver.memory': '2g',
    },
    total_executor_cores=16,
    num_executors=4,
    verbose=True,
    dag=dag,
)

# With parameters
spark_task = SparkSubmitOperator(
    task_id='spark_with_params',
    application='job.py',
    application_args=['/data/input.csv', '/data/output.parquet'],
    conf={'spark.executor.memory': '4g'},
    dag=dag,
)
```

### Dynamic DAG Generation

```python
# Generate tasks programmatically

sources = ['api_1', 'api_2', 'database_1', 'database_2']

extract_tasks = []
for source in sources:
    extract = PythonOperator(
        task_id=f'extract_{source}',
        python_callable=lambda src=source: extract_from_source(src),
        dag=dag,
    )
    extract_tasks.append(extract)

# All extract tasks run in parallel, then merge
merge = PythonOperator(task_id='merge_data', python_callable=merge_func, dag=dag)
extract_tasks >> merge
```

### Backfilling Data

```bash
# Scenario: New DAG, need to run for past dates

# Backfill for date range
airflow dags backfill my_dag \
    --start-date 2024-01-01 \
    --end-date 2024-01-31

# Backfill one day
airflow dags backfill my_dag \
    --start-date 2024-01-15 \
    --end-date 2024-01-15

# Backfill with parallelism
airflow dags backfill my_dag \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    --num-runs 4  # Run 4 at a time
```

### SLA Monitoring

```python
from datetime import timedelta

default_args = {
    'owner': 'data-team',
    'sla': timedelta(hours=4),  # Task must complete within 4 hours
    'sla_miss_callback': notify_sla_miss,
}

def notify_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called when SLA is missed"""
    print(f"SLA missed for tasks: {task_list}")
    # Send Slack/email alert

# Task-level SLA
task = PythonOperator(
    task_id='critical_task',
    python_callable=my_func,
    sla=timedelta(hours=2),  # This task specifically
    dag=dag,
)

# Monitor via UI:
# - Home → SLA Misses (shows recent SLA violations)
```

### Production Deployment

```yaml
# Typical production setup

Airflow Cluster:
├── Web Server (1 instance)
│   ├── UI for monitoring
│   ├── REST API
│   └── Authentication
│
├── Scheduler (1-2 instances, HA)
│   ├── Parse DAGs every 30s
│   ├── Schedule tasks
│   └── Monitor SLAs
│
├── Executor Pool (many instances)
│   ├── CeleryExecutor with Kafka queue
│   ├── 10-100 workers
│   └── Scales automatically
│
├── Database (high-availability)
│   ├── PostgreSQL or MySQL
│   ├── Stores all metadata
│   └── Backed up daily
│
└── Logs
    ├── Centralized logging (ELK, Splunk)
    └── Searchable from UI
```

---

# LEVEL 4: SENIOR
## Enterprise Systems (Weeks 13-20)

### Custom Operators

```python
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataValidationOperator(BaseOperator):
    """Custom operator for data validation"""

    template_fields = ['sql', 'min_rows']

    def __init__(self, sql, min_rows=0, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.min_rows = min_rows

    def execute(self, context):
        """Run validation"""
        # Connect to DB
        # Run SQL
        # Check minimum rows
        # Raise exception if invalid
        pass

# Use custom operator
validate = DataValidationOperator(
    task_id='validate_data',
    sql='SELECT COUNT(*) FROM staging',
    min_rows=1000,
    dag=dag,
)
```

### Multi-Environment Deployment

```python
import os

# Configuration per environment
ENV = os.environ.get('AIRFLOW_ENV', 'dev')

CONFIGS = {
    'dev': {
        'db_host': 'localhost',
        'num_workers': 2,
        'alert_email': 'dev-team@company.com',
    },
    'staging': {
        'db_host': 'staging-db.company.com',
        'num_workers': 5,
        'alert_email': 'staging-team@company.com',
    },
    'prod': {
        'db_host': 'prod-db.company.com',
        'num_workers': 20,
        'alert_email': 'prod-team@company.com',
    }
}

config = CONFIGS[ENV]

# Use config in DAG
dag = DAG(
    f'my_dag_{ENV}',
    default_args={
        'email': config['alert_email'],
    }
)

spark_task = SparkSubmitOperator(
    task_id='spark',
    num_executors=config['num_workers'],
    dag=dag,
)
```

### High Availability

```yaml
# HA setup (what production looks like)

Master Nodes (3, multi-AZ):
├── Scheduler-1 (active)
├── Scheduler-2 (standby)
└── Scheduler-3 (standby)
    └─ Heartbeat every 1s
    └─ If active fails → standby takes over

Database:
├── Primary (active writes)
├── Replica-1 (read-only)
├── Replica-2 (read-only)
└─ If primary fails → Replica-1 promoted

Executors:
├── Pool of 20-100 workers
├─ Auto-scale based on queue size
└─ No single point of failure

Result:
- Scheduler failure: 1-5 second downtime (failover)
- Worker failure: That job retried on another worker
- No data loss (everything in DB)
```

### Cost Optimization

```python
# Strategy 1: Schedule appropriately
dag = DAG(
    'expensive_computation',
    schedule_interval='@weekly',  # Not daily!
)

# Strategy 2: Right-size resources
spark_task = SparkSubmitOperator(
    task_id='spark',
    num_executors=5,  # Not 100!
    executor_memory='4g',  # Appropriate size
)

# Strategy 3: Conditional execution
def should_run_expensive_job():
    # Only run expensive job if needed
    return has_new_data()

branch = BranchPythonOperator(
    task_id='check_data',
    python_callable=should_run_expensive_job,
)

cheap_job = DummyOperator(task_id='skip_expensive')
expensive_job = SparkSubmitOperator(
    task_id='expensive_computation',
    num_executors=20,
)

branch >> [expensive_job, cheap_job]

# Strategy 4: Use spot instances for workers
# Spot instances = 70% cheaper but can be interrupted
# Airflow retries automatically, so safe to use

# Strategy 5: Resource pooling
# Share resources across multiple DAGs
from airflow.models import Pool

# Create pool with Airflow UI
# Then use in task
task = PythonOperator(
    task_id='my_task',
    python_callable=my_func,
    pool='shared_pool',  # Limited to 5 concurrent
    pool_slots=1,
)
```

### Enterprise Example: Data Lake Pipeline

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=4),
    'sla_miss_callback': notify_sla,
    'email_on_failure': True,
    'email': ['data-team@company.com'],
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'data_lake_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    description='Daily data lake update',
)

# Task 1: Wait for data
wait_for_data = S3KeySensor(
    task_id='wait_for_raw_data',
    bucket_name='raw-bucket',
    bucket_key='{{ ds }}/',  # Today's date as folder
    poke_interval=300,  # Check every 5 min
    timeout=3600,  # Max 1 hour wait
    dag=dag,
)

# Task 2: Validate schema
def validate_schema(context):
    ds = context['ds']
    # Load from S3
    # Check schema matches expected
    # Raise if invalid

validate = PythonOperator(
    task_id='validate_schema',
    python_callable=validate_schema,
    provide_context=True,
    dag=dag,
)

# Task 3: Load to Bronze layer
load_bronze = SparkSubmitOperator(
    task_id='load_bronze_layer',
    application='s3://scripts-bucket/ingest_bronze.py',
    application_args=['{{ ds }}'],
    conf={'spark.executor.memory': '4g'},
    num_executors=4,
    dag=dag,
)

# Task 4: Transform to Silver layer
transform_silver = SparkSubmitOperator(
    task_id='transform_silver_layer',
    application='s3://scripts-bucket/transform_silver.py',
    application_args=['{{ ds }}'],
    conf={'spark.executor.memory': '8g'},
    num_executors=8,
    dag=dag,
)

# Task 5: Aggregate to Gold layer
aggregate_gold = SparkSubmitOperator(
    task_id='aggregate_gold_layer',
    application='s3://scripts-bucket/aggregate_gold.py',
    application_args=['{{ ds }}'],
    conf={'spark.executor.memory': '4g'},
    num_executors=4,
    dag=dag,
)

# Task 6: Data quality check
def check_quality(context):
    ds = context['ds']
    # Check counts, nulls, etc.
    # Alert if issues

quality_check = PythonOperator(
    task_id='quality_check',
    python_callable=check_quality,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

# Task 7: Notify success
notify = PythonOperator(
    task_id='notify_complete',
    python_callable=lambda: print("Pipeline succeeded!"),
    trigger_rule='all_success',
    dag=dag,
)

# Define dependencies
wait_for_data >> validate >> load_bronze >> transform_silver >> aggregate_gold >> quality_check >> notify
```

---

## Interview Preparation

### Common Questions

**Q: What is a DAG?**
A: Directed acyclic graph representing workflow tasks and dependencies

**Q: How does Airflow handle failures?**
A: Retries (configurable), alerts, SLA monitoring, manual recovery

**Q: How to pass data between tasks?**
A: XCom for small data, files/DB for large data

**Q: Airflow vs Cron?**
A: Airflow: Dependencies, monitoring, UI, error handling. Cron: Simple, lightweight

**Q: How to schedule job every 15 minutes?**
A: `schedule_interval='*/15 * * * *'` or `timedelta(minutes=15)`

**Q: What happens if task fails?**
A: Airflow retries based on config, marks task as failed, alerts

**Q: How to stop a running DAG?**
A: In Airflow UI: Delete DAG run or mark task as skipped

---

## Quick Reference

### Scheduling
```python
'@daily'           # Every day UTC midnight
'@hourly'          # Every hour
'0 2 * * *'        # 2 AM daily (cron)
'*/15 * * * *'     # Every 15 min
timedelta(hours=1) # Every 1 hour
```

### Common Callbacks
```python
default_args = {
    'on_failure_callback': on_fail_func,
    'on_success_callback': on_success_func,
    'on_retry_callback': on_retry_func,
}
```

### Trigger Rules
```python
'all_success'  # All upstream succeeded (default)
'one_failed'   # Run if any upstream failed
'all_done'     # Run regardless of success/failure
'none_skipped' # Run if no upstream skipped
```

### Context Variables
```python
context['ds']           # Data interval start date (YYYY-MM-DD)
context['execution_date']  # DAG execution date
context['task_instance']   # Task instance object
context['dag_run']         # DAG run object
```

---

**Congratulations! You now understand Airflow from junior to senior level.** 🎉

**Next:** Combine with Spark orchestration to build complete data platforms!
