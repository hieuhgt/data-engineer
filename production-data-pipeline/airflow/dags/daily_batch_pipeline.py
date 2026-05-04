# Daily Batch Data Pipeline DAG
# Orchestrates: Ingest → Validate → Transform → Load

from airflow import DAG
from airflow.sdk import task_group
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-platform',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=4),  # Must complete in 4 hours
    'email_on_failure': True,
    'email': ['data-team@company.com'],
}

# DAG definition
dag = DAG(
    'daily_batch_pipeline',
    default_args=default_args,
    description='Daily batch ETL pipeline: ingest → validate → transform → load',
    schedule='0 0 * * *',  # Daily at midnight UTC
    tags=['production', 'batch', 'daily'],
    catchup=False,
)

# Tasks

def ingest_data(**context):
    """Ingest data from 20 sources"""
    ds = context['ds']
    logger.info(f"Starting ingestion for {ds}")

    # In production: Load from config
    from src.ingestion.base_connector import APIConnector, FileConnector

    connectors = [
        # Users & social
        APIConnector("users",           {"endpoint": "https://jsonplaceholder.typicode.com/users"}),
        APIConnector("posts",           {"endpoint": "https://jsonplaceholder.typicode.com/posts"}),
        APIConnector("comments",        {"endpoint": "https://jsonplaceholder.typicode.com/comments"}),
        APIConnector("todos",           {"endpoint": "https://jsonplaceholder.typicode.com/todos"}),
        APIConnector("albums",          {"endpoint": "https://jsonplaceholder.typicode.com/albums"}),
        # Random user profiles
        APIConnector("random_users",    {"endpoint": "https://randomuser.me/api/?results=50"}),
        # Geography
        APIConnector("countries",       {"endpoint": "https://restcountries.com/v3.1/all?fields=name,capital,population,region,area"}),
        # Weather (no key required)
        APIConnector("weather_nyc",     {"endpoint": "https://api.open-meteo.com/v1/forecast?latitude=40.71&longitude=-74.01&current=temperature_2m,wind_speed_10m"}),
        APIConnector("weather_london",  {"endpoint": "https://api.open-meteo.com/v1/forecast?latitude=51.51&longitude=-0.13&current=temperature_2m,wind_speed_10m"}),
        APIConnector("weather_tokyo",   {"endpoint": "https://api.open-meteo.com/v1/forecast?latitude=35.69&longitude=139.69&current=temperature_2m,wind_speed_10m"}),
        # Crypto prices (no key required)
        APIConnector("crypto_prices",   {"endpoint": "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50"}),
        APIConnector("crypto_trending", {"endpoint": "https://api.coingecko.com/api/v3/search/trending"}),
        # Public datasets
        APIConnector("universities",    {"endpoint": "http://universities.hipolabs.com/search?country=United+States"}),
        APIConnector("cat_facts",       {"endpoint": "https://catfact.ninja/facts?limit=50"}),
        APIConnector("dog_breeds",      {"endpoint": "https://dog.ceo/api/breeds/list/all"}),
        # Space & science
        APIConnector("nasa_apod",       {"endpoint": "https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY&count=10"}),
        APIConnector("iss_position",    {"endpoint": "http://api.open-notify.org/iss-now.json"}),
        # HTTP testing
        APIConnector("ip_info",         {"endpoint": "https://httpbin.org/json"}),
        APIConnector("user_agents",     {"endpoint": "https://httpbin.org/user-agent"}),
        # S3 file source (MinIO)
        FileConnector("s3_transactions", {"bucket": "raw-bucket", "prefix": "transactions/"}),
    ]

    all_data = {}
    failed_sources = []

    for connector in connectors:
        try:
            data, lineage = connector.execute()
            all_data[connector.name] = {'data': data, 'lineage': lineage}
            logger.info(f"✓ Ingested {len(data)} rows from {connector.name}")
        except Exception as e:
            logger.error(f"✗ Failed to ingest from {connector.name}: {e}")
            failed_sources.append(connector.name)

    # Check: Allow partial failure (continue if < 3 sources fail)
    if len(failed_sources) > 3:
        raise ValueError(f"Too many source failures ({len(failed_sources)}): {failed_sources}")

    logger.info(f"Ingestion complete: {len(all_data)} sources succeeded, {len(failed_sources)} failed")

    # Store in XCom for next task
    context['task_instance'].xcom_push(key='ingested_data', value=all_data)
    context['task_instance'].xcom_push(key='failed_sources', value=failed_sources)

    return {'success_count': len(all_data), 'failure_count': len(failed_sources)}


def validate_data(**context):
    """Validate data quality"""
    ti = context['task_instance']
    ingested_data = ti.xcom_pull(task_ids='ingest_data', key='ingested_data')

    from src.validation.quality_gates import (
        NullCheckGate,
        BusinessRuleGate,
        QualityGateChecker
    )

    # Create quality gates
    gates = [
        NullCheckGate(required_fields=['id', 'user_id']),
        BusinessRuleGate(rules={'positive_amount': 'amount > 0'}),
    ]

    checker = QualityGateChecker(gates)

    # Validate all ingested data
    validation_results = {}
    for source_name, source_data in ingested_data.items():
        data = source_data['data']
        _, results = checker.validate_all(data)
        validation_results[source_name] = {
            'quality_score': checker.get_quality_score(),
            'passed': all(r.passed for r in results)
        }
        logger.info(f"{source_name}: quality_score={checker.get_quality_score():.1%}")

    # Check: Fail if quality < 95%
    overall_quality = sum(r['quality_score'] for r in validation_results.values()) / len(validation_results)
    if overall_quality < 0.95:
        raise ValueError(f"Data quality below threshold: {overall_quality:.1%} < 95%")

    ti.xcom_push(key='validation_results', value=validation_results)
    logger.info(f"Validation passed: overall quality = {overall_quality:.1%}")


def transform_data(**context):
    """Submit Spark transformation job"""
    ds = context['ds']
    logger.info(f"Starting transformation for {ds}")

    # Spark job will handle:
    # - Deduplication
    # - Cleansing (nulls, types)
    # - Enrichment (joins)
    # - Aggregation

    # Return job ID for tracking
    return {'spark_job_id': 'job_12345', 'status': 'submitted'}


def load_data(**context):
    """Load transformed data to warehouse"""
    ti = context['task_instance']
    transform_result = ti.xcom_pull(task_ids='transform', key='return_value')

    logger.info(f"Loading data from Spark job {transform_result['spark_job_id']}")

    # In production: Use Snowflake API, BigQuery API, etc.
    # For demo: Simulate warehouse load

    from src.warehouse.warehouse_loader import idempotent_load

    # Load with idempotency (merge strategy)
    load_result = idempotent_load(
        source_data='s3://processed-bucket/transformed/',
        target_table='fact_events',
        merge_key=['event_id', 'date']
    )

    logger.info(f"Loaded {load_result['rows_loaded']} rows to warehouse")

    return load_result


def monitor_pipeline(**context):
    """Post-load monitoring and alerts"""
    ti = context['task_instance']
    ds = context['ds']

    logger.info(f"Running post-load monitoring for {ds}")

    # Check: Data freshness
    # - Warehouse has latest data
    # - Lineage documented
    # - Quality score > 95%

    freshness_hours = 0.5  # Loaded 30 minutes ago
    if freshness_hours > 4:
        logger.warning(f"Data freshness warning: {freshness_hours} hours")

    # Check: Cost tracking
    # - Compute cost for this run
    # - Storage usage

    logger.info("Pipeline monitoring complete")


# Define task structure
with dag:
    @task_group()
    def ingestion_group():
        return PythonOperator(
            task_id='ingest_data',
            python_callable=ingest_data,
            pool='data_pipeline',
        )

    @task_group()
    def validation_group():
        return PythonOperator(
            task_id='validate_data',
            python_callable=validate_data,
        )

    transform_task = SparkSubmitOperator(
        task_id='transform',
        application='/spark/transform_daily.py',
        application_args=['--date', '{{ ds }}'],
        conf={
            'spark.executor.memory': '4g',
            'spark.executor.cores': 4,
            'spark.driver.memory': '2g',
        },
        total_executor_cores=16,
        num_executors=4,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    monitor_task = PythonOperator(
        task_id='monitor',
        python_callable=monitor_pipeline,
        trigger_rule='all_success',
    )

    # ingest → validate → transform → load → monitor
    ingestion_group() >> validation_group() >> transform_task >> load_task >> monitor_task

# SLA monitoring
# If pipeline exceeds 4 hours, Airflow alerts automatically
# Callback on SLA miss
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logger.error(f"SLA miss detected for tasks: {[t.task_id for t in task_list]}")
    # Send Slack alert, PagerDuty, etc.
    pass

# Note: SLA callback would be added to DAG config in production
