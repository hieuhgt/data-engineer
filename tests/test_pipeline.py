"""Unit tests for Spark pipeline - Best practices."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_pipeline")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def sample_df(spark):
    """Create sample DataFrame for testing."""
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("event_type", StringType()),
        StructField("value", DoubleType()),
    ])

    data = [
        ("user_1", "click", 10.0),
        ("user_2", "purchase", 25.5),
        ("user_1", "click", 5.0),
    ]

    return spark.createDataFrame(data, schema)


def test_dataframe_creation(sample_df):
    """Test DataFrame creation."""
    assert sample_df.count() == 3
    assert len(sample_df.columns) == 3


def test_filter_operation(sample_df):
    """Test filtering operation."""
    from pyspark.sql.functions import col

    filtered = sample_df.filter(col("value") > 10.0)
    assert filtered.count() == 2


def test_aggregation(sample_df):
    """Test aggregation operation."""
    from pyspark.sql.functions import col, sum, count

    agg_result = sample_df.groupBy("user_id").agg(
        sum("value").alias("total_value"),
        count("*").alias("event_count")
    )

    assert agg_result.count() == 2
    rows = agg_result.collect()
    assert any(row["user_id"] == "user_1" and row["event_count"] == 2 for row in rows)


if __name__ == "__main__":
    pytest.main([__file__])
