import pytest
from datetime import date, datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
    DateType, TimestampType, IntegerType,
)

from job.gold import aggregate_daily_country


class TestAggregateDailyCountry:
    def _make_enriched(self, spark):
        schema = StructType([
            StructField("user_id", StringType()),
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("event_ts", TimestampType()),
            StructField("value", DoubleType()),
            StructField("event_date", DateType()),
            StructField("country", StringType()),
            StructField("is_purchase", BooleanType()),
            StructField("days_since_signup", IntegerType()),
        ])
        data = [
            ("u1", "e1", "CLICK", datetime(2025, 1, 1, 10, 0), 3.0, date(2025, 1, 1), "ID", False, 31),
            ("u1", "e3", "PURCHASE", datetime(2025, 1, 1, 10, 10), 25.0, date(2025, 1, 1), "ID", True, 31),
            ("u2", "e2", "VIEW", datetime(2025, 1, 1, 10, 5), 0.0, date(2025, 1, 1), "US", False, 17),
            ("u1", "e10", "CLICK", datetime(2025, 1, 2, 9, 0), 1.0, date(2025, 1, 2), "ID", False, 32),
        ]
        return spark.createDataFrame(data, schema=schema)

    def test_aggregation_columns(self, spark):
        enriched = self._make_enriched(spark)
        agg = aggregate_daily_country(enriched)
        expected_cols = {"event_date", "country", "total_events", "total_value", "total_purchases", "unique_users"}
        assert set(agg.columns) == expected_cols

    def test_aggregation_values_id_day1(self, spark):
        enriched = self._make_enriched(spark)
        agg = aggregate_daily_country(enriched)

        from pyspark.sql import functions as F
        row = agg.filter(
            (F.col("event_date") == F.lit("2025-01-01").cast("date")) & (F.col("country") == "ID")
        ).collect()[0]

        assert row["total_events"] == 2
        assert row["total_value"] == 28.0
        assert row["total_purchases"] == 1
        assert row["unique_users"] == 1

    def test_aggregation_values_us_day1(self, spark):
        enriched = self._make_enriched(spark)
        agg = aggregate_daily_country(enriched)

        from pyspark.sql import functions as F
        row = agg.filter(
            (F.col("event_date") == F.lit("2025-01-01").cast("date")) & (F.col("country") == "US")
        ).collect()[0]

        assert row["total_events"] == 1
        assert row["total_value"] == 0.0
        assert row["total_purchases"] == 0
        assert row["unique_users"] == 1

    def test_multiple_dates_produce_separate_rows(self, spark):
        enriched = self._make_enriched(spark)
        agg = aggregate_daily_country(enriched)
        assert agg.count() == 3  # ID day1, US day1, ID day2
