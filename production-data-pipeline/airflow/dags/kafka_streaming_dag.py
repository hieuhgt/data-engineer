"""
Kafka Streaming Monitor DAG
Runs every 15 minutes to:
  1. Verify the Spark Structured Streaming job is alive (via Spark Master REST API)
  2. Calculate real consumer group lag from Kafka
  3. Alert if lag exceeds threshold

The streaming job (spark/streaming_job.py) is a long-lived process submitted once
and runs continuously. This DAG monitors its health and restarts it if it crashes.
"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import requests

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:29092"   # internal Docker listener (not 9092)
CONSUMER_GROUP = "spark-streaming-group"
TOPIC = "raw-events"
LAG_ALERT_THRESHOLD = 50_000
SPARK_MASTER_API = "http://spark-master:8080/api/v1"
STREAMING_APP_NAME = "StreamingIngest"

default_args = {
    "owner": "data-platform",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "kafka_streaming_monitor",
    default_args=default_args,
    description="Monitor Kafka consumer lag and streaming job health",
    schedule="*/15 * * * *",
    tags=["streaming", "monitoring"],
    catchup=False,
)


def check_streaming_job_alive(**context):
    """
    Query Spark Master REST API to verify the streaming job is in RUNNING state.
    If the job is not running, push a flag so the next task can restart it.
    """
    try:
        resp = requests.get(f"{SPARK_MASTER_API}/applications", timeout=5)
        resp.raise_for_status()
        apps = resp.json()

        running = [a for a in apps if a.get("name") == STREAMING_APP_NAME and a.get("state") == "RUNNING"]
        is_alive = len(running) > 0

        logger.info(f"Streaming job alive={is_alive}, running_instances={len(running)}")
        context["task_instance"].xcom_push(key="job_alive", value=is_alive)
        return {"alive": is_alive, "app_count": len(running)}

    except requests.RequestException as e:
        logger.warning(f"Could not reach Spark Master API: {e}")
        context["task_instance"].xcom_push(key="job_alive", value=False)
        return {"alive": False, "reason": str(e)}


def restart_streaming_if_down(**context):
    """
    If the streaming job is not alive, submit it via spark-submit.
    Uses subprocess so Airflow does not block waiting for the infinite streaming job.
    """
    import subprocess

    ti = context["task_instance"]
    is_alive = ti.xcom_pull(task_ids="check_streaming_job", key="job_alive")

    if is_alive:
        logger.info("Streaming job is running — no restart needed")
        return {"action": "none"}

    logger.warning("Streaming job is DOWN — submitting restart")
    cmd = [
        "spark-submit",
        "--master", "spark://spark-master:7077",
        "--name", STREAMING_APP_NAME,
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
        "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
        "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "/opt/airflow/spark/streaming_job.py",
    ]
    # detach=True so this task completes without waiting for the streaming job
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    logger.info(f"Streaming job resubmitted (pid={proc.pid})")
    return {"action": "restarted", "pid": proc.pid}


def check_consumer_lag(**context):
    """
    Calculate real consumer group lag:
      lag = end_offset (latest on partition) - committed_offset (consumer group position)
    A high lag means the streaming job is falling behind producing events.
    """
    try:
        from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition

        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id="lag-monitor",
        )
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)

        # Get partitions for the topic
        partitions = consumer.partitions_for_topic(TOPIC)
        if not partitions:
            logger.warning(f"Topic '{TOPIC}' has no partitions (may not exist yet)")
            consumer.close()
            admin.close()
            return {"status": "no_topic", "lag": 0}

        topic_partitions = [TopicPartition(TOPIC, p) for p in partitions]

        # Latest offsets produced to the topic
        end_offsets = consumer.end_offsets(topic_partitions)

        # Consumer group committed offsets
        committed = admin.list_consumer_group_offsets(CONSUMER_GROUP)

        total_lag = 0
        partition_lags = {}
        for tp in topic_partitions:
            end = end_offsets.get(tp, 0)
            meta = committed.get(tp)
            committed_offset = meta.offset if meta else 0
            lag = max(0, end - committed_offset)
            total_lag += lag
            partition_lags[f"{TOPIC}:{tp.partition}"] = lag

        consumer.close()
        admin.close()

        logger.info(f"Total consumer lag: {total_lag:,} | per-partition: {partition_lags}")
        return {"status": "ok", "lag": total_lag, "partitions": partition_lags}

    except Exception as e:
        logger.warning(f"Kafka lag check failed: {e}")
        return {"status": "skipped", "lag": 0, "reason": str(e)}


def evaluate_lag(**context):
    """Alert if lag exceeds threshold."""
    from src.monitoring.alerting import AlertManager

    ti = context["task_instance"]
    result = ti.xcom_pull(task_ids="check_lag")
    lag = result.get("lag", 0) if result else 0
    status = result.get("status", "unknown") if result else "unknown"

    if status == "skipped":
        logger.warning("Lag check was skipped — Kafka may not be reachable")
        return

    if lag > LAG_ALERT_THRESHOLD:
        AlertManager().alert("warning", f"Kafka consumer lag is {lag:,} messages (threshold={LAG_ALERT_THRESHOLD:,})")
        raise ValueError(f"Consumer lag too high: {lag:,}")

    logger.info(f"Lag OK: {lag:,} messages")


check_streaming_job = PythonOperator(
    task_id="check_streaming_job",
    python_callable=check_streaming_job_alive,
    dag=dag,
)

restart_if_down = PythonOperator(
    task_id="restart_if_down",
    python_callable=restart_streaming_if_down,
    dag=dag,
)

check_lag = PythonOperator(
    task_id="check_lag",
    python_callable=check_consumer_lag,
    dag=dag,
)

evaluate_lag_task = PythonOperator(
    task_id="evaluate_lag",
    python_callable=evaluate_lag,
    dag=dag,
)

check_streaming_job >> restart_if_down >> check_lag >> evaluate_lag_task
