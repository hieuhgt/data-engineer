"""
Spark Job: Transform Silver to Gold Layer

Aggregations & Enrichment:
- Time-window aggregations
- Join with dimension tables
- Calculate KPIs/metrics
- Prepare for analytics

Called by Airflow DAG
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    window, date_format, current_timestamp, lit
)
from loguru import logger

SILVER_PATH = os.getenv('SILVER_PATH', 's3a://datalake/silver/events/')
GOLD_PATH = os.getenv('GOLD_PATH', 's3a://datalake/gold/daily_metrics/')
DIMENSION_PATH = os.getenv('DIMENSION_PATH', 's3a://datalake/gold/dimensions/')
EXECUTION_DATE = os.getenv('EXECUTION_DATE', '2024-01-01')

logger.info(f"Starting Silver→Gold transformation for {EXECUTION_DATE}")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver-to-gold")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def read_dimensions(spark):
    """Read dimension tables for enrichment."""
    # In production, these would come from master data
    users = spark.createDataFrame([
        ("user_1", "Premium"),
        ("user_2", "Standard"),
    ], ["user_id", "segment"])

    return {"users": users}


def create_daily_metrics(df, dimensions):
    """Create daily metrics aggregation."""
    return (
        df
        .groupBy(
            date_format(col("event_timestamp"), "yyyy-MM-dd").alias("date"),
            col("event_type")
        )
        .agg(
            count("*").alias("event_count"),
            spark_sum("value").alias("total_value"),
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count(col("user_id").distinct()).alias("unique_users"),
        )
        .withColumn("metric_timestamp", current_timestamp())
    )


def create_user_metrics(df, dimensions):
    """Create per-user metrics."""
    return (
        df
        .groupBy(
            date_format(col("event_timestamp"), "yyyy-MM-dd").alias("date"),
            col("user_id")
        )
        .agg(
            count("*").alias("user_event_count"),
            spark_sum("value").alias("user_total_value"),
        )
        .join(
            dimensions["users"],
            on="user_id",
            how="left"
        )
        .withColumn("metric_timestamp", current_timestamp())
    )


def create_event_type_trends(df):
    """Analyze event type trends."""
    return (
        df
        .groupBy(
            col("event_type"),
            date_format(col("event_timestamp"), "yyyy-MM-dd").alias("date")
        )
        .agg(
            count("*").alias("trend_count"),
            avg("value").alias("trend_avg_value"),
        )
        .orderBy("date", "event_type")
        .withColumn("metric_timestamp", current_timestamp())
    )


def main():
    spark = create_spark_session()

    try:
        logger.info(f"Reading Silver data from {SILVER_PATH}")

        # Read Silver layer
        df_silver = spark.read.parquet(SILVER_PATH)
        logger.info(f"Read {df_silver.count()} records from Silver")

        # Read dimensions for enrichment
        dimensions = read_dimensions(spark)

        # Create various gold layer aggregations
        logger.info("Creating daily metrics")
        daily_metrics = create_daily_metrics(df_silver, dimensions)

        logger.info("Creating user metrics")
        user_metrics = create_user_metrics(df_silver, dimensions)

        logger.info("Creating event type trends")
        event_trends = create_event_type_trends(df_silver)

        # Write to Gold layer
        metrics_output = f"{GOLD_PATH}/daily_metrics/{EXECUTION_DATE}/"
        users_output = f"{GOLD_PATH}/user_metrics/{EXECUTION_DATE}/"
        trends_output = f"{GOLD_PATH}/event_trends/{EXECUTION_DATE}/"

        logger.info(f"Writing daily metrics to {metrics_output}")
        daily_metrics.coalesce(1).write.mode("overwrite").parquet(metrics_output)

        logger.info(f"Writing user metrics to {users_output}")
        user_metrics.coalesce(1).write.mode("overwrite").parquet(users_output)

        logger.info(f"Writing trends to {trends_output}")
        event_trends.coalesce(1).write.mode("overwrite").parquet(trends_output)

        logger.info("✓ Silver→Gold transformation completed")

        # Show sample results
        logger.info("Sample Daily Metrics:")
        daily_metrics.show()

        logger.info("Sample User Metrics:")
        user_metrics.show()

    except Exception as e:
        logger.error(f"✗ Transformation failed: {e}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
