"""Gold layer: produce daily, country-level aggregate metrics."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def aggregate_daily_country(enriched_df: DataFrame) -> DataFrame:
    """
    Produce daily, country-level metrics:
    - total_events
    - total_value
    - total_purchases
    - unique_users
    """
    agg_df = enriched_df.groupBy("event_date", "country").agg(
        F.count("*").alias("total_events"),
        F.sum("value").alias("total_value"),
        F.sum(F.when(F.col("is_purchase"), 1).otherwise(0)).alias("total_purchases"),
        F.countDistinct("user_id").alias("unique_users"),
    )

    return agg_df


def run_gold(spark: SparkSession, output_base: str):
    """Full gold layer: read silver enriched -> aggregate -> write."""
    enriched_df = spark.read.parquet(f"{output_base}/silver/events")
    agg_df = aggregate_daily_country(enriched_df)

    # Overwrite partitions for idempotency - only affected event_dates are rewritten
    agg_df.write.mode("overwrite").partitionBy("event_date").parquet(
        f"{output_base}/gold/daily_country_metrics"
    )

    return agg_df
