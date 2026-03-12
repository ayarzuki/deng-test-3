from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

# Raw event schema - all fields as strings for maximum ingestion flexibility
RAW_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("value", StringType(), True),
])

VALID_EVENT_TYPES = {"CLICK", "VIEW", "PURCHASE"}
