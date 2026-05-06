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
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main(date: str):
    logger.info(f"Starting daily transform for {date}")
    spark = get_spark("DailyTransform")

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # --- BRONZE: read all raw sources for this date ---
    raw_path = f"s3a://raw-bucket/*/date={date}/data.parquet"
    try:
        raw = spark.read.parquet(raw_path)
        logger.info(f"Loaded {raw.count()} raw rows from {raw_path}")
    except Exception as e:
        logger.error(f"Failed to read raw data: {e}")
        raise

    transformer = SparkTransformer(spark)
    actual_cols = set(raw.columns)

    # --- SILVER: cleanse using columns that actually exist ---
    # Required cols are those present in the data; skip missing ones gracefully
    required_cols = [c for c in ["id", "name", "email"] if c in actual_cols]
    silver = transformer.cleanse(raw, required_cols=required_cols)
    silver = transformer.cast_columns(silver, {"id": "int"})
    silver = transformer.add_audit_columns(silver, source="users_api")

    # Add partition date (users data has no timestamp column)
    silver = silver.withColumn("event_date", F.lit(date).cast("date"))

    # Flatten nested structs if present (address, company from /users API)
    if "address" in actual_cols:
        silver = (
            silver
            .withColumn("city", F.col("address.city"))
            .withColumn("zipcode", F.col("address.zipcode"))
            .drop("address")
        )
    if "company" in actual_cols:
        silver = (
            silver
            .withColumn("company_name", F.col("company.name"))
            .drop("company")
        )

    silver_path = f"s3a://processed-bucket/silver/date={date}/"
    silver.write.mode("overwrite").parquet(silver_path)
    logger.info(f"Silver layer written: {silver.count()} rows → {silver_path}")

    # --- GOLD: aggregate users by company ---
    gold = (
        silver
        .groupBy("event_date", "company_name")
        .agg(
            F.count("*").alias("user_count"),
            F.collect_list("name").alias("user_names"),
        )
    )

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
