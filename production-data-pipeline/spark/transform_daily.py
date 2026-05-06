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
    # ingest_data writes: users/date=<date>/part-00000.parquet, part-00001.parquet ...
    raw_path = f"s3a://raw-bucket/*/date={date}/"
    try:
        raw = spark.read.parquet(raw_path)
        logger.info(f"Loaded {raw.count()} raw rows from {raw_path}")
    except Exception as e:
        logger.error(f"Failed to read raw data: {e}")
        raise

    transformer = SparkTransformer(spark)
    actual_cols = set(raw.columns)

    # --- SILVER: cleanse using columns that actually exist ---
    required_cols = [c for c in ["id", "firstName", "lastName", "email"] if c in actual_cols]
    silver = transformer.cleanse(raw, required_cols=required_cols)
    silver = transformer.cast_columns(silver, {"id": "int"})
    silver = transformer.add_audit_columns(silver, source="users_api")

    # Add partition date
    silver = silver.withColumn("event_date", F.lit(date).cast("date"))

    # Combine firstName + lastName → name
    if "firstName" in actual_cols and "lastName" in actual_cols:
        silver = (
            silver
            .withColumn("name", F.concat_ws(" ", F.col("firstName"), F.col("lastName")))
            .drop("firstName", "lastName")
        )

    # Flatten address: city, state, country (drop nested coordinates)
    if "address" in actual_cols:
        silver = (
            silver
            .withColumn("city", F.col("address.city"))
            .withColumn("state", F.col("address.state"))
            .withColumn("country", F.col("address.country"))
            .drop("address")
        )

    # Flatten company: name, department (company also has nested address — drop it)
    if "company" in actual_cols:
        silver = (
            silver
            .withColumn("company_name", F.col("company.name"))
            .withColumn("company_department", F.col("company.department"))
            .drop("company")
        )

    # Drop deep-nested columns that are hard to flatten and not needed for gold
    drop_cols = [c for c in ["hair", "bank", "crypto", "coordinates"] if c in set(silver.columns)]
    if drop_cols:
        silver = silver.drop(*drop_cols)

    silver_path = f"s3a://processed-bucket/silver/date={date}/"
    silver.write.mode("overwrite").parquet(silver_path)
    logger.info(f"Silver layer written: {silver.count()} rows → {silver_path}")

    # --- GOLD: aggregate users by company ---
    gold = (
        silver
        .groupBy("event_date", "company_name", "company_department")
        .agg(
            F.count("*").alias("user_count"),
            F.collect_list("name").alias("user_names"),
            F.collect_list("email").alias("user_emails"),
        )
    )

    gold_path = f"s3a://processed-bucket/gold/date={date}/"
    gold.write.mode("overwrite").parquet(gold_path)
    logger.info(f"Gold layer written: {gold.count()} rows → {gold_path}")

    spark.stop()
    logger.info("Transform complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=None,
        help="Partition date (YYYY-MM-DD). Defaults to today if not provided.",
    )
    args = parser.parse_args()
    from datetime import date as _date
    run_date = args.date or _date.today().isoformat()
    main(run_date)
