"""
Pipeline Health-Check DAG
Runs every hour to verify data freshness and quality scores.
"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-platform",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

dag = DAG(
    "pipeline_health_monitor",
    default_args=default_args,
    description="Hourly data freshness and quality check",
    schedule="@hourly",
    tags=["monitoring"],
    catchup=False,
)


def check_data_freshness(**context):
    """Verify warehouse tables were updated in the last 4 hours."""
    from datetime import datetime, timezone
    import os

    # In production: query Snowflake/BigQuery for MAX(loaded_at)
    # For demo: check if a local parquet file exists and is recent
    path = "/tmp/warehouse/fact_events.parquet"
    if not os.path.exists(path):
        logger.warning("Warehouse file not found – pipeline may not have run")
        return {"fresh": False, "age_hours": None}

    import time
    age_sec = time.time() - os.path.getmtime(path)
    age_hours = age_sec / 3600
    fresh = age_hours < 4

    logger.info(f"fact_events freshness: {age_hours:.1f}h old (fresh={fresh})")

    if not fresh:
        from src.monitoring.alerting import AlertManager
        AlertManager().sla_miss("daily_batch_pipeline", actual_hours=age_hours)

    return {"fresh": fresh, "age_hours": round(age_hours, 2)}


def check_row_counts(**context):
    """Verify expected minimum row counts in warehouse tables."""
    import os

    tables = {"fact_events": 1_000}
    results = {}

    for table, min_rows in tables.items():
        path = f"/tmp/warehouse/{table}.parquet"
        if not os.path.exists(path):
            results[table] = {"status": "missing"}
            continue

        import pandas as pd
        df = pd.read_parquet(path)
        ok = len(df) >= min_rows
        results[table] = {"rows": len(df), "min": min_rows, "ok": ok}

        if not ok:
            logger.warning(f"{table}: only {len(df)} rows (expected ≥ {min_rows})")

    return results


freshness_check = PythonOperator(
    task_id="check_freshness",
    python_callable=check_data_freshness,
    dag=dag,
)

row_count_check = PythonOperator(
    task_id="check_row_counts",
    python_callable=check_row_counts,
    dag=dag,
)

freshness_check >> row_count_check
