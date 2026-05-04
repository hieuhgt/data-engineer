import logging
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def get_spark(app_name: str = "PipelineTransform") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")  # Spark 3+ adaptive query execution
        .getOrCreate()
    )


class SparkTransformer:
    """Base transformation class wrapping common Spark patterns."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def cleanse(self, df: DataFrame, required_cols: List[str]) -> DataFrame:
        """Drop nulls in required columns and remove exact duplicates."""
        df = df.dropna(subset=required_cols)
        df = df.dropDuplicates()
        return df

    def cast_columns(self, df: DataFrame, type_map: Dict[str, str]) -> DataFrame:
        """Cast columns to declared types (safe – bad casts become null)."""
        for col, dtype in type_map.items():
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(dtype))
        return df

    def add_audit_columns(self, df: DataFrame, source: str) -> DataFrame:
        """Add _source and _loaded_at for lineage."""
        return (
            df.withColumn("_source", F.lit(source))
            .withColumn("_loaded_at", F.current_timestamp())
        )

    def transform(self, df: DataFrame, config: Dict) -> DataFrame:
        raise NotImplementedError("Subclasses must implement transform()")
