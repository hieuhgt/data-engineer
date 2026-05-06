"""
Structured Streaming job: Kafka → Bronze (Parquet on MinIO).
Run continuously alongside the daily batch pipeline.
Managed by kafka_streaming_monitor DAG (auto-restart if crashed).
"""
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.transformation.spark_transformer import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, DoubleType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:29092"   # internal Docker listener (not 9092)
MINIO_ENDPOINT = "http://minio:9000"
BRONZE_PATH = "s3a://raw-bucket/streaming/events/"
CHECKPOINT_PATH = "s3a://raw-bucket/_checkpoints/events/"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", StringType()),
])


def main():
    spark = get_spark("StreamingIngest")
    spark.sparkContext.setLogLevel("WARN")

    # MinIO S3A config (same as transform_daily.py)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Read from Kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "raw-events")
        .option("startingOffsets", "latest")
        .option("kafka.group.id", "spark-streaming-group")
        .load()
    )

    # Parse JSON value column
    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("event_date", F.to_date(F.col("timestamp")))
        .filter(F.col("event_id").isNotNull())  # drop malformed events
    )

    # Write to Bronze on MinIO (partitioned by date, checkpointed for exactly-once)
    query = (
        parsed.writeStream
        .format("parquet")
        .partitionBy("event_date")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .start()
    )

    logger.info(f"Streaming job started — writing to {BRONZE_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
