"""
Advanced Multi-Stage Pipeline DAG with Dynamic Task Generation

Features:
- Dynamic task generation (one task per data source)
- Conditional branching (different processing per source)
- Backfill support
- SLA monitoring per stage
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

SPARK_JOBS_PATH = "/opt/airflow/spark_jobs"
S3_BUCKET = Variable.get("S3_BUCKET", "company-datalake")

# Define data sources to process
DATA_SOURCES = {
    'user_events': {
        'input': 's3a://company-datalake/raw/user_events/',
        'output': 's3a://company-datalake/bronze/user_events/',
    },
    'purchases': {
        'input': 's3a://company-datalake/raw/purchases/',
        'output': 's3a://company-datalake/bronze/purchases/',
    },
    'pageviews': {
        'input': 's3a://company-datalake/raw/pageviews/',
        'output': 's3a://company-datalake/bronze/pageviews/',
    },
}

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'multi_stage_pipeline',
    default_args=default_args,
    description='Advanced pipeline with dynamic tasks and branching',
    schedule_interval='0 2 * * *',  # 2 AM UTC
    catchup=True,  # Enable backfilling
    tags=['data-pipeline', 'advanced'],
) as dag:

    start = DummyOperator(task_id='start')

    # ============================================================================
    # DYNAMICALLY CREATE TASKS FOR EACH DATA SOURCE
    # ============================================================================

    ingest_tasks = {}

    for source_name, paths in DATA_SOURCES.items():
        task = SparkSubmitOperator(
            task_id=f'ingest_{source_name}',
            application=f'{SPARK_JOBS_PATH}/ingest_generic.py',
            env_vars={
                'SOURCE_NAME': source_name,
                'INPUT_PATH': paths['input'],
                'OUTPUT_PATH': f"{paths['output']}{{ ds }}/",
                'EXECUTION_DATE': '{{ ds }}',
            },
        )
        ingest_tasks[source_name] = task

    # ============================================================================
    # BRANCHING: Different processing per source
    # ============================================================================

    def decide_processing_path(**context):
        """Decide which processing path based on source."""
        source = context.get('task').task_id.replace('ingest_', '')

        if source == 'purchases':
            return 'enrich_purchases'  # Needs currency conversion
        elif source == 'pageviews':
            return 'deduplicate_pageviews'  # Needs dedup
        else:
            return 'generic_transform'

    branch = BranchPythonOperator(
        task_id='branch_on_source',
        python_callable=decide_processing_path,
        provide_context=True,
    )

    # Process branches
    enrich_purchases = SparkSubmitOperator(
        task_id='enrich_purchases',
        application=f'{SPARK_JOBS_PATH}/enrich_purchases.py',
    )

    deduplicate_pageviews = SparkSubmitOperator(
        task_id='deduplicate_pageviews',
        application=f'{SPARK_JOBS_PATH}/deduplicate_pageviews.py',
    )

    generic_transform = SparkSubmitOperator(
        task_id='generic_transform',
        application=f'{SPARK_JOBS_PATH}/generic_transform.py',
    )

    # Combine all processing branches
    merge = DummyOperator(
        task_id='merge_all_sources',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ============================================================================
    # DEPENDENCIES
    # ============================================================================

    start >> list(ingest_tasks.values()) >> branch

    branch >> [enrich_purchases, deduplicate_pageviews, generic_transform] >> merge


# ============================================================================
# MULTI-DAG APPROACH: Process multiple datasets in parallel
# ============================================================================
# Alternative: Instead of one DAG with many tasks, create multiple DAGs
# Each runs independently but can have cross-DAG dependencies

def create_source_dag(source_name: str, input_path: str, output_path: str):
    """Factory function to create DAGs for each source."""

    dag = DAG(
        f'pipeline_{source_name}',
        default_args=default_args,
        description=f'Pipeline for {source_name}',
        schedule_interval='0 1 * * *',
        catchup=False,
    )

    with dag:
        ingest = SparkSubmitOperator(
            task_id=f'ingest_{source_name}',
            application=f'{SPARK_JOBS_PATH}/ingest_generic.py',
            env_vars={
                'SOURCE_NAME': source_name,
                'INPUT_PATH': input_path,
                'OUTPUT_PATH': output_path,
            },
        )

    return dag


# Dynamically create DAGs for each source
for source_name, paths in DATA_SOURCES.items():
    source_dag = create_source_dag(
        source_name,
        paths['input'],
        paths['output'],
    )
    # Register the DAG
    globals()[f'pipeline_{source_name}'] = source_dag
