"""Silver layer: enrich clean events with user reference data."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_users(spark: SparkSession, users_path: str) -> DataFrame:
    """Load user reference CSV. Parse signup_date, handle invalid dates gracefully."""
    users_df = spark.read.option("header", "true").csv(users_path)

    # Parse signup_date, invalid dates become null
    users_df = users_df.withColumn(
        "signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd")
    )

    # Fill missing country with 'UNKNOWN'
    users_df = users_df.fillna({"country": "UNKNOWN"})

    return users_df


def enrich_events(clean_df: DataFrame, users_df: DataFrame) -> DataFrame:
    """
    Join events with users and add derived fields:
    - event_date (already present from bronze, kept for clarity)
    - is_purchase: boolean flag
    - days_since_signup: integer days between event_date and signup_date
    """
    enriched = clean_df.join(users_df, on="user_id", how="left")

    # Fill missing user dimensions for events with unknown users
    enriched = enriched.fillna({"country": "UNKNOWN"})

    # Derived fields
    enriched = enriched.withColumn(
        "is_purchase", F.col("event_type") == "PURCHASE"
    )
    enriched = enriched.withColumn(
        "days_since_signup",
        F.when(
            F.col("signup_date").isNotNull(),
            F.datediff(F.col("event_date"), F.col("signup_date")),
        ).otherwise(None),
    )

    return enriched


def run_silver(spark: SparkSession, output_base: str, users_path: str):
    """Full silver layer: read bronze clean -> enrich -> write."""
    clean_df = spark.read.parquet(f"{output_base}/bronze/events")
    users_df = load_users(spark, users_path)
    enriched_df = enrich_events(clean_df, users_df)

    enriched_df.write.mode("overwrite").partitionBy("event_date").parquet(
        f"{output_base}/silver/events"
    )

    return enriched_df
