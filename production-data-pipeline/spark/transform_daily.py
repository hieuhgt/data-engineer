"""
Daily batch transformation job.
Invoked by Airflow SparkSubmitOperator:
  spark-submit spark/transform_daily.py --date 2024-01-15
"""
import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.transformation.spark_transformer import get_spark, SparkTransformer
from src.transformation.aggregations import daily_user_metrics
from src.transformation.enrichment import Enricher
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main(date: str):
    logger.info(f"Starting daily transform for {date}")
    spark = get_spark("DailyTransform")

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # --- BRONZE: read raw ingested data ---
    raw_path = f"s3a://raw-bucket/*/date={date}/data.parquet"
    try:
        raw = spark.read.parquet(raw_path)
        logger.info(f"Loaded {raw.count()} raw rows from {raw_path}")
    except Exception as e:
        logger.error(f"Failed to read raw data: {e}")
        raise

    transformer = SparkTransformer(spark)

    # --- SILVER: cleanse + cast ---
    silver = transformer.cleanse(raw, required_cols=["event_id", "user_id"])
    silver = transformer.cast_columns(silver, {"amount": "double", "user_id": "int"})
    silver = transformer.add_audit_columns(silver, source="events_api")
    silver = silver.withColumn("event_date", F.to_date(F.col("timestamp")))

    silver_path = f"s3a://processed-bucket/silver/date={date}/"
    silver.write.mode("overwrite").parquet(silver_path)
    logger.info(f"Silver layer written: {silver.count()} rows → {silver_path}")

    # --- GOLD: aggregate ---
    enricher = Enricher(spark)
    enriched = enricher.enrich_with_user_segments(silver, "s3://data-lake/dims/user_segments/")

    gold = daily_user_metrics(enriched, date_col="event_date")
    gold_path = f"s3a://processed-bucket/gold/date={date}/"
    gold.write.mode("overwrite").parquet(gold_path)
    logger.info(f"Gold layer written: {gold.count()} rows → {gold_path}")

    spark.stop()
    logger.info("Transform complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Partition date (YYYY-MM-DD)")
    args = parser.parse_args()
    main(args.date)
