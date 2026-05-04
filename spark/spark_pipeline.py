"""PySpark ETL pipeline example - Best practices."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, schema_of_json, current_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from loguru import logger
from config import get_config


class SparkETLPipeline:
    """Reusable Spark ETL pipeline with best practices."""

    def __init__(self):
        self.config = get_config()
        self.spark = self._initialize_spark()
        self.logger = logger

    def _initialize_spark(self) -> SparkSession:
        """Initialize Spark session with optimized configs."""
        return (
            SparkSession.builder
            .appName(self.config.spark.app_name)
            .master(self.config.spark.master)
            .config("spark.executor.memory", self.config.spark.memory)
            .config("spark.sql.shuffle.partitions", self.config.spark.shuffle_partitions)
            .config("spark.sql.adaptive.enabled", self.config.spark.sql_adaptive_enabled)
            # Performance settings
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .getOrCreate()
        )

    def read_csv(self, path: str, header: bool = True) -> DataFrame:
        """Read CSV file with type inference."""
        self.logger.info(f"Reading CSV from {path}")
        return self.spark.read.option("header", header).option("inferSchema", True).csv(path)

    def read_parquet(self, path: str) -> DataFrame:
        """Read Parquet file (preferred for data lakes)."""
        self.logger.info(f"Reading Parquet from {path}")
        return self.spark.read.parquet(path)

    def read_kafka_stream(self, topic: str, starting_offsets: str = "latest") -> DataFrame:
        """Read streaming data from Kafka."""
        self.logger.info(f"Reading Kafka topic: {topic}")
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(self.config.kafka.bootstrap_servers))
            .option("subscribe", topic)
            .option("startingOffsets", starting_offsets)
            .load()
        )

    def parse_json_events(self, df: DataFrame, json_column: str = "value") -> DataFrame:
        """Parse JSON events from Kafka."""
        # Define schema for JSON (replace with your actual schema)
        schema = StructType([
            StructField("user_id", StringType()),
            StructField("event_type", StringType()),
            StructField("value", DoubleType()),
            StructField("timestamp", StringType()),
        ])

        return df.select(
            col(json_column).cast(StringType()).alias("json_str"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            from_json(col("json_str"), schema).alias("data"),
            col("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")

    def apply_transformations(self, df: DataFrame) -> DataFrame:
        """Example transformations - customize as needed."""
        return (
            df
            .filter(col("value") > 0)  # Remove nulls/invalid
            .withColumn("processed_at", current_timestamp())
            .withColumn("event_date", col("timestamp").cast("date"))
        )

    def aggregate_by_window(self, df: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
        """Aggregate streaming data using time windows."""
        return (
            df.groupBy(
                window(col("timestamp"), window_duration),
                col("event_type")
            )
            .agg({
                "value": "sum",
                "user_id": "count"
            })
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("event_type"),
                col("sum(value)").alias("total_value"),
                col("count(user_id)").alias("event_count")
            )
        )

    def write_parquet(self, df: DataFrame, path: str, mode: str = "overwrite") -> None:
        """Write data to Parquet (columnar format, best for data lakes)."""
        self.logger.info(f"Writing Parquet to {path}")
        df.coalesce(1).write.mode(mode).parquet(path)

    def write_kafka(self, df: DataFrame, topic: str, key_column: str = None) -> None:
        """Write results back to Kafka."""
        self.logger.info(f"Writing to Kafka topic: {topic}")
        query = (
            df.select(
                col("*").cast("string")
            ).write
            .format("kafka")
            .option("kafka.bootstrap.servers", ",".join(self.config.kafka.bootstrap_servers))
            .option("topic", topic)
            .mode("append")
            .save()
        )

    def write_stream(self, df: DataFrame, checkpoint_path: str, output_path: str, mode: str = "append"):
        """Write streaming results to storage."""
        self.logger.info(f"Starting stream write to {output_path}")
        query = (
            df.writeStream
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .outputMode(mode)
            .start()
        )
        return query

    def show_sample(self, df: DataFrame, rows: int = 5) -> None:
        """Display sample of data."""
        df.show(rows, truncate=False)

    def get_stats(self, df: DataFrame) -> None:
        """Print basic statistics."""
        df.describe().show()

    def stop(self) -> None:
        """Stop Spark session."""
        self.spark.stop()
        self.logger.info("Spark session stopped")


# Example usage
if __name__ == "__main__":
    pipeline = SparkETLPipeline()

    # Batch processing example
    df = pipeline.read_csv("data/sample.csv")
    transformed = pipeline.apply_transformations(df)
    pipeline.write_parquet(transformed, "output/processed")

    pipeline.show_sample(transformed)
    pipeline.get_stats(transformed)

    pipeline.stop()
