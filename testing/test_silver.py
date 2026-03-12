import pytest
from datetime import date, datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, DateType,
)

from job.silver import load_users, enrich_events


class TestLoadUsers:
    def test_loads_csv_and_parses_dates(self, spark, tmp_path):
        csv_content = "user_id,country,signup_date\nu1,ID,2024-12-01\nu2,US,2024-12-15\n"
        csv_path = str(tmp_path / "users.csv")
        with open(csv_path, "w") as f:
            f.write(csv_content)

        users_df = load_users(spark, csv_path)
        assert users_df.count() == 2
        row = users_df.filter(F.col("user_id") == "u1").collect()[0]
        assert row["signup_date"] == date(2024, 12, 1)
        assert row["country"] == "ID"

    def test_invalid_signup_date_becomes_null(self, spark, tmp_path):
        csv_content = "user_id,country,signup_date\nu3,SG,invalid_date\n"
        csv_path = str(tmp_path / "users.csv")
        with open(csv_path, "w") as f:
            f.write(csv_content)

        users_df = load_users(spark, csv_path)
        row = users_df.collect()[0]
        assert row["signup_date"] is None

    def test_missing_country_filled_with_unknown(self, spark, tmp_path):
        csv_content = "user_id,country,signup_date\nu4,,2024-11-01\n"
        csv_path = str(tmp_path / "users.csv")
        with open(csv_path, "w") as f:
            f.write(csv_content)

        users_df = load_users(spark, csv_path)
        row = users_df.collect()[0]
        assert row["country"] == "UNKNOWN"


class TestEnrichEvents:
    def _make_events(self, spark):
        schema = StructType([
            StructField("user_id", StringType()),
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("event_ts", TimestampType()),
            StructField("value", DoubleType()),
            StructField("event_date", DateType()),
        ])
        data = [
            ("u1", "e1", "CLICK", datetime(2025, 1, 1, 10, 0), 3.0, date(2025, 1, 1)),
            ("u1", "e3", "PURCHASE", datetime(2025, 1, 1, 10, 10), 25.0, date(2025, 1, 1)),
            ("u99", "e99", "VIEW", datetime(2025, 1, 1, 11, 0), 1.0, date(2025, 1, 1)),
        ]
        return spark.createDataFrame(data, schema=schema)

    def _make_users(self, spark):
        schema = StructType([
            StructField("user_id", StringType()),
            StructField("country", StringType()),
            StructField("signup_date", DateType()),
        ])
        data = [
            ("u1", "ID", date(2024, 12, 1)),
        ]
        return spark.createDataFrame(data, schema=schema)

    def test_enrichment_adds_derived_fields(self, spark):
        events = self._make_events(spark)
        users = self._make_users(spark)
        enriched = enrich_events(events, users)

        assert "is_purchase" in enriched.columns
        assert "days_since_signup" in enriched.columns

    def test_is_purchase_flag(self, spark):
        events = self._make_events(spark)
        users = self._make_users(spark)
        enriched = enrich_events(events, users)

        purchase_row = enriched.filter(F.col("event_id") == "e3").collect()[0]
        click_row = enriched.filter(F.col("event_id") == "e1").collect()[0]
        assert purchase_row["is_purchase"] is True
        assert click_row["is_purchase"] is False

    def test_days_since_signup_calculated(self, spark):
        events = self._make_events(spark)
        users = self._make_users(spark)
        enriched = enrich_events(events, users)

        row = enriched.filter(F.col("event_id") == "e1").collect()[0]
        # 2025-01-01 - 2024-12-01 = 31 days
        assert row["days_since_signup"] == 31

    def test_unknown_user_gets_unknown_country(self, spark):
        events = self._make_events(spark)
        users = self._make_users(spark)
        enriched = enrich_events(events, users)

        row = enriched.filter(F.col("event_id") == "e99").collect()[0]
        assert row["country"] == "UNKNOWN"

    def test_unknown_user_days_since_signup_is_null(self, spark):
        events = self._make_events(spark)
        users = self._make_users(spark)
        enriched = enrich_events(events, users)

        row = enriched.filter(F.col("event_id") == "e99").collect()[0]
        assert row["days_since_signup"] is None
