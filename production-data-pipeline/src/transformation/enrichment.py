import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class Enricher:
    """Join fact data with dimension/reference tables."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _load_dim(self, table_path: str) -> DataFrame:
        return self.spark.read.parquet(table_path)

    def enrich_with_user_segments(
        self, df: DataFrame, segments_path: str, join_key: str = "user_id"
    ) -> DataFrame:
        """Left-join user segments so every row keeps its original columns."""
        segments = self._load_dim(segments_path)
        # Broadcast small dimension table – avoids shuffle
        return df.join(F.broadcast(segments), on=join_key, how="left")

    def enrich_with_products(
        self, df: DataFrame, products_path: str, join_key: str = "product_id"
    ) -> DataFrame:
        products = self._load_dim(products_path)
        return df.join(F.broadcast(products), on=join_key, how="left")
