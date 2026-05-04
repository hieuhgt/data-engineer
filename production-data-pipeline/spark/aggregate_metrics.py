"""
Aggregates gold-layer metrics for analyst dashboards.
spark-submit spark/aggregate_metrics.py --date 2024-01-15
"""
import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.transformation.spark_transformer import get_spark
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(date: str):
    spark = get_spark("AggregateMetrics")

    gold = spark.read.parquet(f"s3://data-lake/gold/daily_user_metrics/date={date}/")

    # Weekly rolling window
    weekly = (
        gold.groupBy("user_id")
        .agg(
            F.sum("total_amount").alias("weekly_amount"),
            F.sum("event_count").alias("weekly_events"),
        )
    )

    out = f"s3://data-lake/gold/weekly_user_metrics/date={date}/"
    weekly.write.mode("overwrite").parquet(out)
    logger.info(f"Aggregates written: {weekly.count()} rows → {out}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.date)
