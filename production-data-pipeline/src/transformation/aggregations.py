from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def daily_user_metrics(df: DataFrame, date_col: str = "event_date") -> DataFrame:
    """Aggregate per-user daily metrics from events."""
    return (
        df.groupBy(date_col, "user_id")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.countDistinct("event_type").alias("distinct_event_types"),
        )
    )


def top_n_per_group(df: DataFrame, group_col: str, rank_col: str, n: int = 10) -> DataFrame:
    """Return top N rows per group ordered by rank_col descending."""
    window = Window.partitionBy(group_col).orderBy(F.col(rank_col).desc())
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") <= n)
        .drop("_rank")
    )


def running_total(df: DataFrame, order_col: str, value_col: str) -> DataFrame:
    """Add cumulative sum column ordered by order_col."""
    window = Window.orderBy(order_col).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return df.withColumn(f"{value_col}_running", F.sum(value_col).over(window))
