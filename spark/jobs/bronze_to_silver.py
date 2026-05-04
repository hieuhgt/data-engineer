"""
Spark Job: Transform Bronze to Silver Layer

Transformations:
- Data type validation
- Null handling
- Deduplication
- Standard transformations

Called by Airflow DAG
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lower, trim, current_timestamp,
    from_unixtime, to_timestamp
)
from loguru import logger

BRONZE_PATH = os.getenv('BRONZE_PATH', 's3a://datalake/bronze/events/')
SILVER_PATH = os.getenv('SILVER_PATH', 's3a://datalake/silver/events/')
EXECUTION_DATE = os.getenv('EXECUTION_DATE', '2024-01-01')

logger.info(f"Starting Bronze→Silver transformation for {EXECUTION_DATE}")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze-to-silver")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def clean_data(df):
    """Apply cleaning transformations."""
    return (
        df
        # Trim whitespace
        .withColumn("user_id", trim(col("user_id")))
        .withColumn("event_type", trim(lower(col("event_type"))))

        # Handle nulls
        .withColumn("value", coalesce(col("value"), col("lit(0.0)")))

        # Validate timestamps
        .filter(col("timestamp").isNotNull())
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))

        # Remove invalid records
        .filter(col("user_id").isNotNull())
        .filter(col("user_id") != "")
        .filter(col("event_type").isNotNull())
    )


def deduplicate_data(df):
    """Remove duplicates (keep first occurrence)."""
    return df.dropDuplicates(["user_id", "event_type", "event_timestamp"])


def add_silver_metadata(df):
    """Add Silver layer metadata."""
    return (
        df
        .withColumn("silver_ingestion_timestamp", current_timestamp())
        .withColumn("data_quality_flag", col("lit('valid')"))
        .withColumn("silver_layer_version", col("lit('v1')"))
    )


def main():
    spark = create_spark_session()

    try:
        logger.info(f"Reading Bronze data from {BRONZE_PATH}")

        # Read Bronze layer
        df_bronze = spark.read.parquet(BRONZE_PATH)
        logger.info(f"Read {df_bronze.count()} records from Bronze")

        # Apply transformations
        df_cleaned = clean_data(df_bronze)
        logger.info(f"After cleaning: {df_cleaned.count()} records")

        df_deduplicated = deduplicate_data(df_cleaned)
        logger.info(f"After deduplication: {df_deduplicated.count()} records")

        df_silver = add_silver_metadata(df_deduplicated)

        # Write to Silver layer
        logger.info(f"Writing Silver data to {SILVER_PATH}")

        (
            df_silver
            .coalesce(20)
            .write
            .mode("overwrite")
            .parquet(SILVER_PATH)
        )

        logger.info("✓ Bronze→Silver transformation completed")

        # Log some statistics
        df_silver.select("event_type").distinct().show()
        df_silver.describe("value").show()

    except Exception as e:
        logger.error(f"✗ Transformation failed: {e}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
