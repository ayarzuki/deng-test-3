"""Bronze layer: ingest raw data, validate, clean, and quarantine bad records."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DoubleType

from job.schemas import RAW_EVENT_SCHEMA, VALID_EVENT_TYPES


def ingest_events(spark: SparkSession, raw_path: str) -> DataFrame:
    """Read raw JSONL events with explicit schema. Malformed JSON rows go to _corrupt_record."""
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType(RAW_EVENT_SCHEMA.fields + [
        StructField("_corrupt_record", StringType(), True),
    ])
    df = spark.read.schema(schema).option("mode", "PERMISSIVE").json(raw_path)
    return df


def split_valid_corrupt(df: DataFrame):
    """Separate corrupt JSON rows from parseable rows."""
    if "_corrupt_record" in df.columns:
        corrupt = df.filter(F.col("_corrupt_record").isNotNull()).select(
            F.col("_corrupt_record").alias("raw_record"),
            F.lit("corrupt_json").alias("rejection_reason"),
        )
        valid = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    else:
        corrupt = None
        valid = df
    return valid, corrupt


def validate_and_clean(df: DataFrame):
    """
    Apply data quality rules and return (clean_df, quarantined_df).

    Rules:
    - event_id must not be null
    - user_id must not be null or empty
    - event_type must not be null and must be a valid type (after uppercasing)
    - event_ts must be a valid timestamp
    - Deduplicate by event_id (keep first occurrence)
    - Normalize event_type to uppercase
    - Cast value to double, default to 0.0 if null/invalid
    """
    # Normalize event_type to uppercase
    df = df.withColumn("event_type", F.upper(F.trim(F.col("event_type"))))

    # Build rejection reason
    valid_types_list = list(VALID_EVENT_TYPES)
    df = df.withColumn(
        "rejection_reason",
        F.when(F.col("event_id").isNull(), "null_event_id")
        .when(F.col("user_id").isNull() | (F.trim(F.col("user_id")) == ""), "null_or_empty_user_id")
        .when(F.col("event_type").isNull() | ~F.col("event_type").isin(valid_types_list), "invalid_event_type")
        .when(F.col("event_ts").isNull(), "null_event_ts")
        .when(F.to_timestamp(F.col("event_ts")).isNull(), "invalid_event_ts")
        .otherwise(None),
    )

    quarantined = df.filter(F.col("rejection_reason").isNotNull())
    clean = df.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")

    # Deduplicate by event_id
    clean = clean.dropDuplicates(["event_id"])

    # Cast value to double, fill nulls with 0.0
    clean = clean.withColumn("value", F.col("value").cast(DoubleType()))
    clean = clean.fillna({"value": 0.0})

    # Parse event_ts to proper timestamp
    clean = clean.withColumn("event_ts", F.to_timestamp(F.col("event_ts")))

    return clean, quarantined


def run_bronze(spark: SparkSession, raw_path: str, output_base: str):
    """Full bronze layer: ingest -> validate -> write clean + quarantined."""
    raw_df = ingest_events(spark, raw_path)
    # Cache to avoid Spark restriction on querying only _corrupt_record from raw JSON
    raw_df = raw_df.cache()
    valid_df, corrupt_df = split_valid_corrupt(raw_df)
    clean_df, quarantined_df = validate_and_clean(valid_df)

    # Add event_date for partitioning
    clean_df = clean_df.withColumn("event_date", F.to_date(F.col("event_ts")))

    # Write clean events partitioned by event_date (overwrite for idempotency)
    clean_df.write.mode("overwrite").partitionBy("event_date").parquet(
        f"{output_base}/bronze/events"
    )

    # Write quarantined events
    quarantined_df.write.mode("overwrite").parquet(f"{output_base}/bronze/quarantined")

    # Write corrupt records if any
    if corrupt_df is not None and corrupt_df.count() > 0:
        corrupt_df.write.mode("overwrite").parquet(f"{output_base}/bronze/corrupt")

    raw_df.unpersist()
    return clean_df
