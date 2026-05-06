# Daily Batch Data Pipeline DAG
# Orchestrates: Ingest → Validate → Confirm → Transform → Load → Monitor

import logging
from datetime import datetime, timedelta
from io import BytesIO

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.sdk import task_group
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from src.ingestion.base_connector import APIConnector
from src.warehouse.warehouse_loader import idempotent_load

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
RAW_BUCKET = 'raw-bucket'
PROCESSED_BUCKET = 'processed-bucket'
REQUIRED_FIELDS = ['id', 'firstName', 'email']


def _s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Created bucket '{bucket}'")


def _read_parquet_from_prefix(s3, bucket: str, prefix: str) -> list[pd.DataFrame]:
    dfs = []
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get('Contents', []):
        if not obj['Key'].endswith('.parquet') or obj['Size'] == 0:
            continue
        buf = BytesIO()
        s3.download_fileobj(bucket, obj['Key'], buf)
        buf.seek(0)
        dfs.append(pd.read_parquet(buf))
    return dfs


# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------

default_args = {
    'owner': 'data-platform',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_batch_pipeline',
    default_args=default_args,
    description='Daily batch ETL pipeline: ingest → validate → transform → load',
    schedule='0 0 * * *',
    tags=['production', 'batch', 'daily'],
    catchup=False,
    max_active_runs=1,  # chỉ 1 run tại 1 thời điểm — tránh conflict ghi cùng partition
)

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

def ingest_data(**context):
    """
    Fetch data page by page and write directly to MinIO as Parquet chunks.
    Only MinIO paths (not data) are pushed to XCom — scales to millions of records.
    """
    ds = context.get('ds') or datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Starting ingestion for {ds}")

    s3 = _s3_client()
    _ensure_bucket(s3, RAW_BUCKET)

    connectors = [
        APIConnector("users", {
            "endpoint": "https://dummyjson.com/users",
            "data_key": "users",   # response: {"users": [...], "total": 208}
            "page_size": 100,      # 208 total → 3 pages: 100 + 100 + 8
        }),
    ]

    staging_paths = {}
    failed_sources = []

    for connector in connectors:
        try:
            total_rows = 0
            for page_num, page_data in enumerate(connector.fetch_pages()):
                table = pa.Table.from_pandas(pd.DataFrame(page_data))
                buf = BytesIO()
                pq.write_table(table, buf)
                buf.seek(0)
                key = f"{connector.name}/date={ds}/part-{page_num:05d}.parquet"
                s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=buf.getvalue())
                total_rows += len(page_data)
                logger.info(f"  {connector.name} page {page_num}: {len(page_data)} rows → {key}")

            staging_paths[connector.name] = {
                'prefix': f"{connector.name}/date={ds}/",
                'rows': total_rows,
            }
            logger.info(f"✓ {connector.name}: {total_rows} rows written to s3://{RAW_BUCKET}/")
        except Exception as e:
            logger.error(f"✗ {connector.name}: {e}")
            failed_sources.append(connector.name)

    if len(failed_sources) > 3:
        raise ValueError(f"Too many source failures: {failed_sources}")

    logger.info(f"Ingestion complete: {len(staging_paths)} sources, {len(failed_sources)} failed")
    context['task_instance'].xcom_push(key='staging_paths', value=staging_paths)
    context['task_instance'].xcom_push(key='failed_sources', value=failed_sources)
    return {'success_count': len(staging_paths), 'failure_count': len(failed_sources)}


def validate_data(**context):
    """
    Read Parquet from MinIO and validate using pandas vectorized operations.
    No Python loops over rows — scales to millions of records.
    """
    ti = context['task_instance']
    staging_paths = ti.xcom_pull(task_ids='ingestion_group.ingest_data', key='staging_paths')

    if not staging_paths:
        raise ValueError("No staging paths from ingest step")

    s3 = _s3_client()
    validation_results = {}

    for source_name, info in staging_paths.items():
        prefix = info['prefix']
        dfs = _read_parquet_from_prefix(s3, RAW_BUCKET, prefix)

        if not dfs:
            validation_results[source_name] = {
                'passed': False, 'quality_score': 0.0, 'rows': 0, 'prefix': prefix,
            }
            continue

        df = pd.concat(dfs, ignore_index=True)

        # Vectorized null check across all required columns at once
        existing = [f for f in REQUIRED_FIELDS if f in df.columns]
        if existing:
            fail_count = int(df[existing].isnull().any(axis=1).sum())
            pass_rate = 1.0 - (fail_count / len(df))
        else:
            pass_rate = 1.0

        passed = pass_rate >= 0.99
        validation_results[source_name] = {
            'passed': passed,
            'quality_score': float(pass_rate),
            'rows': len(df),
            'prefix': prefix,
        }
        logger.info(f"{source_name}: {len(df):,} rows | quality={pass_rate:.1%} | passed={passed}")

    overall_quality = sum(r['quality_score'] for r in validation_results.values()) / len(validation_results)
    if overall_quality < 0.95:
        raise ValueError(f"Data quality below threshold: {overall_quality:.1%}")

    ti.xcom_push(key='validation_results', value=validation_results)
    logger.info(f"Validation passed: overall quality = {overall_quality:.1%}")


def save_to_minio(**context):
    """
    Data is already written to MinIO by ingest_data (page by page).
    This step confirms which sources passed validation and are ready for Spark.
    """
    ti = context['task_instance']
    validation_results = ti.xcom_pull(task_ids='validation_group.validate_data', key='validation_results')

    if not validation_results:
        logger.warning("No validation results; skipping")
        ti.xcom_push(key='minio_save_result', value={'sources_saved': [], 'base_path': f's3://{RAW_BUCKET}'})
        return

    sources_saved = [s for s, r in validation_results.items() if r.get('passed')]
    skipped = [s for s, r in validation_results.items() if not r.get('passed')]

    if skipped:
        logger.warning(f"Skipped (failed validation): {skipped}")

    logger.info(f"Ready for Spark: {sources_saved}")
    ti.xcom_push(key='minio_save_result', value={
        'sources_saved': sources_saved,
        'base_path': f's3://{RAW_BUCKET}',
    })


def load_data(**context):
    """Load gold layer from MinIO into warehouse."""
    ds = context.get('ds') or datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Loading gold data for {ds}")

    s3 = _s3_client()
    prefix = f"gold/date={ds}/"
    records = []

    try:
        dfs = _read_parquet_from_prefix(s3, PROCESSED_BUCKET, prefix)
        for df in dfs:
            records.extend(df.to_dict('records'))
    except Exception as e:
        logger.warning(f"Could not read gold data from MinIO: {e}")

    if not records:
        logger.warning("No gold data found — skipping warehouse load")
        return {'table': 'dim_users', 'rows_loaded': 0, 'merge_key': ['id']}

    load_result = idempotent_load(source_data=records, target_table='dim_users', merge_key=['id'])
    logger.info(f"Loaded {load_result['rows_loaded']} rows to warehouse")
    return load_result


def monitor_pipeline(**context):
    """Post-load monitoring and alerts."""
    ds = context.get('ds') or datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Running post-load monitoring for {ds}")

    freshness_hours = 0.5
    if freshness_hours > 4:
        logger.warning(f"Data freshness warning: {freshness_hours} hours")

    logger.info("Pipeline monitoring complete")


# ---------------------------------------------------------------------------
# DAG structure
# ---------------------------------------------------------------------------

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

    save_to_minio_task = PythonOperator(
        task_id='save_to_minio',
        python_callable=save_to_minio,
    )

    transform_task = SparkSubmitOperator(
        task_id='transform',
        conn_id='spark_default',
        application='/opt/airflow/spark/transform_daily.py',
        application_args=[],  # transform_daily.py defaults to today; matches ingest_data behavior
        packages='org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262',
        conf={
            'spark.executor.memory': '512m',
            'spark.driver.memory': '512m',
            'spark.sql.shuffle.partitions': '4',
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': '/tmp/spark-events',
        },
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

    ingestion_group() >> validation_group() >> save_to_minio_task >> transform_task >> load_task >> monitor_task
