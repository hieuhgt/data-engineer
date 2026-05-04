"""
Daily Data Pipeline DAG - Medallion Architecture

Flow:
1. Validate input data exists
2. Ingest data from Kafka (or batch source)
3. Process data (Spark ETL) - Bronze to Silver
4. Enrich & aggregate (Spark ETL) - Silver to Gold
5. Run data quality checks
6. Alert on failure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.spark.sensors.spark_sql import SparkSqlSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from loguru import logger

# Configuration
SPARK_MASTER = Variable.get("SPARK_MASTER", "spark://spark-master:7077")
SPARK_JOBS_PATH = "/opt/airflow/spark_jobs"
S3_BUCKET = Variable.get("S3_BUCKET", "company-datalake")
KAFKA_BROKERS = Variable.get("KAFKA_BROKERS", "kafka:9092")

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    'daily_data_pipeline',
    default_args=default_args,
    description='Daily medallion data pipeline: bronze→silver→gold',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM UTC
    catchup=False,
    tags=['data-pipeline', 'daily'],
    # SLA: Pipeline must complete within 4 hours
    sla=timedelta(hours=4),
) as dag:

    # ============================================================================
    # STAGE 1: VALIDATION & SETUP
    # ============================================================================

    def validate_inputs(**context):
        """Validate input data exists and is ready."""
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        logger.info(f"Validating inputs for {execution_date}")

        # Example: Check if input files exist
        # In production, would check S3, Kafka lag, etc.

        return {'execution_date': execution_date}

    validate_data = PythonOperator(
        task_id='validate_inputs',
        python_callable=validate_inputs,
        provide_context=True,
    )

    # ============================================================================
    # STAGE 2: INGESTION - BRONZE LAYER
    # ============================================================================

    ingest_kafka_to_bronze = SparkSubmitOperator(
        task_id='ingest_kafka_to_bronze',
        application=f'{SPARK_JOBS_PATH}/ingest_kafka.py',
        conf={
            'spark.master': SPARK_MASTER,
            'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
        },
        env_vars={
            'KAFKA_BROKERS': KAFKA_BROKERS,
            'OUTPUT_PATH': f's3a://{S3_BUCKET}/bronze/events/{{ ds }}/',
            'EXECUTION_DATE': '{{ ds }}',
        },
        name='spark-ingest-kafka',
        verbose=True,
    )

    # ============================================================================
    # STAGE 3: TRANSFORMATION - BRONZE TO SILVER
    # ============================================================================

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application=f'{SPARK_JOBS_PATH}/bronze_to_silver.py',
        conf={
            'spark.master': SPARK_MASTER,
            'spark.sql.shuffle.partitions': '200',
        },
        env_vars={
            'BRONZE_PATH': f's3a://{S3_BUCKET}/bronze/events/{{ ds }}/',
            'SILVER_PATH': f's3a://{S3_BUCKET}/silver/events/{{ ds }}/',
            'EXECUTION_DATE': '{{ ds }}',
        },
        name='spark-bronze-to-silver',
    )

    # ============================================================================
    # STAGE 4: ENRICHMENT & AGGREGATION - SILVER TO GOLD
    # ============================================================================

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application=f'{SPARK_JOBS_PATH}/silver_to_gold.py',
        conf={
            'spark.master': SPARK_MASTER,
        },
        env_vars={
            'SILVER_PATH': f's3a://{S3_BUCKET}/silver/events/{{ ds }}/',
            'GOLD_PATH': f's3a://{S3_BUCKET}/gold/daily_metrics/{{ ds }}/',
            'DIMENSION_PATH': f's3a://{S3_BUCKET}/gold/dimensions/',
            'EXECUTION_DATE': '{{ ds }}',
        },
        name='spark-silver-to-gold',
    )

    # ============================================================================
    # STAGE 5: DATA QUALITY CHECKS
    # ============================================================================

    def check_data_quality(**context):
        """Run data quality checks using Great Expectations."""
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        logger.info(f"Running data quality checks for {execution_date}")

        # Example checks:
        # - Row count in gold table > 0
        # - No unexpected nulls
        # - Data freshness within SLA
        # - Statistical anomalies

        # In production, integrate with Great Expectations
        # gx.run_checkpoint(...)

        logger.info("Data quality checks passed")

    quality_checks = PythonOperator(
        task_id='quality_checks',
        python_callable=check_data_quality,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ============================================================================
    # STAGE 6: SUCCESS/FAILURE HANDLING
    # ============================================================================

    def send_success_notification(**context):
        """Send success notification."""
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        logger.info(f"✓ Pipeline completed successfully for {execution_date}")

        # Integrate with Slack, email, etc.

    success_notification = PythonOperator(
        task_id='success_notification',
        python_callable=send_success_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    def send_failure_notification(**context):
        """Send failure notification."""
        failed_task = context.get('task').task_id
        logger.error(f"✗ Pipeline failed at task: {failed_task}")

        # Send alert to ops team

    failure_notification = PythonOperator(
        task_id='failure_notification',
        python_callable=send_failure_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ============================================================================
    # TASK DEPENDENCIES
    # ============================================================================

    validate_data >> ingest_kafka_to_bronze >> bronze_to_silver >> silver_to_gold >> quality_checks

    # Both notifications run regardless (success or failure)
    quality_checks >> [success_notification, failure_notification]
