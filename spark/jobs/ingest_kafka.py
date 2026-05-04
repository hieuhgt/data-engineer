"""
Spark Job: Ingest data from Kafka to Bronze Layer

Called by Airflow DAG
Usage: spark-submit ingest_kafka.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from loguru import logger

# Get configuration from environment (set by Airflow)
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
OUTPUT_PATH = os.getenv('OUTPUT_PATH', 's3a://datalake/bronze/events/')
EXECUTION_DATE = os.getenv('EXECUTION_DATE', '2024-01-01')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'events')

logger.info(f"Starting Kafka ingestion job for {EXECUTION_DATE}")
logger.info(f"Kafka brokers: {KAFKA_BROKERS}")
logger.info(f"Output path: {OUTPUT_PATH}")


def create_spark_session() -> SparkSession:
    """Create Spark session for Kafka streaming."""
    return (
        SparkSession.builder
        .appName("kafka-ingest")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.default.parallelism", "200")
        # For S3 access (in EKS environment)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProvider")
        .getOrCreate()
    )


def parse_kafka_events(df) -> any:
    """Parse Kafka JSON events."""
    schema = StructType([
        StructField("user_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("value", DoubleType(), nullable=True),
        StructField("timestamp", StringType(), nullable=False),
    ])

    return (
        df.select(
            col("value").cast(StringType()).alias("json_str"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            from_json(col("json_str"), schema).alias("data"),
            col("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        .withColumn("ingestion_timestamp", current_timestamp())
    )


def main():
    spark = create_spark_session()

    try:
        logger.info(f"Reading Kafka topic: {KAFKA_TOPIC}")

        # Read from Kafka
        df_kafka = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .option("maxOffsetsPerTrigger", 1000000)  # Batch max records
            .load()
        )

        # Parse JSON
        df_parsed = parse_kafka_events(df_kafka)

        # Add metadata
        df_with_metadata = (
            df_parsed
            .withColumn("source_system", col("lit('kafka')"))
            .withColumn("processing_date", col(f"lit('{EXECUTION_DATE}')"))
        )

        # Remove duplicates (keep first occurrence)
        df_deduplicated = df_with_metadata.dropDuplicates(["user_id", "event_type", "timestamp"])

        # Write to Bronze layer (Parquet, partitioned by date)
        logger.info(f"Writing {df_deduplicated.count()} records to {OUTPUT_PATH}")

        (
            df_deduplicated
            .coalesce(10)  # Reduce partition fragmentation
            .write
            .mode("overwrite")
            .parquet(OUTPUT_PATH)
        )

        logger.info("✓ Ingestion completed successfully")

    except Exception as e:
        logger.error(f"✗ Ingestion failed: {e}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
