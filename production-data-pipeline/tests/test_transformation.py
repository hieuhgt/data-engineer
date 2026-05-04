import pytest

pytest.importorskip("pyspark", reason="PySpark not installed – skipping Spark tests")

from pyspark.sql import SparkSession
from src.transformation.aggregations import daily_user_metrics, top_n_per_group
from src.transformation.spark_transformer import SparkTransformer


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestSparkTransformer:
    def test_cleanse_drops_nulls(self, spark):
        data = [{"id": 1, "user_id": 10, "amount": 5.0},
                {"id": 2, "user_id": None, "amount": 3.0}]
        df = spark.createDataFrame(data)
        transformer = SparkTransformer(spark)
        result = transformer.cleanse(df, required_cols=["user_id"])
        assert result.count() == 1

    def test_cleanse_drops_duplicates(self, spark):
        data = [{"id": 1, "user_id": 10}, {"id": 1, "user_id": 10}]
        df = spark.createDataFrame(data)
        transformer = SparkTransformer(spark)
        result = transformer.cleanse(df, required_cols=["id"])
        assert result.count() == 1

    def test_audit_columns_added(self, spark):
        df = spark.createDataFrame([{"id": 1}])
        transformer = SparkTransformer(spark)
        result = transformer.add_audit_columns(df, source="test_api")
        assert "_source" in result.columns
        assert "_loaded_at" in result.columns


class TestAggregations:
    def test_daily_user_metrics(self, spark):
        data = [
            {"event_date": "2024-01-01", "user_id": 1, "amount": 100.0, "event_type": "buy"},
            {"event_date": "2024-01-01", "user_id": 1, "amount": 50.0, "event_type": "view"},
            {"event_date": "2024-01-01", "user_id": 2, "amount": 200.0, "event_type": "buy"},
        ]
        df = spark.createDataFrame(data)
        result = daily_user_metrics(df, date_col="event_date")
        rows = {row["user_id"]: row for row in result.collect()}
        assert rows[1]["event_count"] == 2
        assert rows[1]["total_amount"] == 150.0
        assert rows[2]["event_count"] == 1

    def test_top_n_per_group(self, spark):
        data = [
            {"category": "A", "product": "p1", "sales": 10},
            {"category": "A", "product": "p2", "sales": 20},
            {"category": "A", "product": "p3", "sales": 5},
            {"category": "B", "product": "p4", "sales": 30},
        ]
        df = spark.createDataFrame(data)
        result = top_n_per_group(df, group_col="category", rank_col="sales", n=2)
        cats = result.groupBy("category").count().collect()
        count_map = {r["category"]: r["count"] for r in cats}
        assert count_map["A"] == 2
        assert count_map["B"] == 1
