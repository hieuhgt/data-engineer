"""
Structured Streaming job: Kafka → Bronze (Parquet).
Run continuously alongside the daily batch pipeline.
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

    # Read from Kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "raw-events")
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON value column
    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("event_date", F.to_date(F.col("timestamp")))
    )

    # Write to Bronze (partitioned by date, checkpointed for exactly-once)
    query = (
        parsed.writeStream
        .format("parquet")
        .partitionBy("event_date")
        .option("path", "s3://data-lake/bronze/events/")
        .option("checkpointLocation", "s3://data-lake/_checkpoints/events/")
        .outputMode("append")
        .trigger(processingTime="60 seconds")  # Micro-batch every 60s
        .start()
    )

    logger.info("Streaming job started – waiting for termination")
    query.awaitTermination()


if __name__ == "__main__":
    main()
