"""End-to-end test: run the full pipeline on sample data and verify outputs."""

import json
import os
import pytest
from datetime import date

from pyspark.sql import functions as F

from job.bronze import run_bronze
from job.silver import run_silver, load_users
from job.gold import run_gold


@pytest.fixture
def sample_data(tmp_path):
    """Create sample raw event files and users CSV."""
    events_dir = tmp_path / "raw" / "events"
    events_dir.mkdir(parents=True)

    day1 = [
        {"event_id": "e1", "user_id": "u1", "event_type": "CLICK", "event_ts": "2025-01-01T10:00:00Z", "value": 3},
        {"event_id": "e2", "user_id": "u2", "event_type": "VIEW", "event_ts": "2025-01-01T10:05:00Z", "value": None},
        {"event_id": "e3", "user_id": "u1", "event_type": "PURCHASE", "event_ts": "2025-01-01T10:10:00Z", "value": 25},
        {"event_id": "e4", "user_id": None, "event_type": "CLICK", "event_ts": "invalid_ts", "value": 1},
        {"event_id": "e5", "user_id": "u1", "event_type": "click", "event_ts": "2025-01-01T11:05:00Z", "value": 2},
    ]
    day2 = [
        {"event_id": "e10", "user_id": "u1", "event_type": "CLICK", "event_ts": "2025-01-02T09:00:00Z", "value": 1},
        {"event_id": "e3", "user_id": "u1", "event_type": "PURCHASE", "event_ts": "2025-01-01T10:10:00Z", "value": 25},
        {"event_id": "e14", "user_id": "", "event_type": "CLICK", "event_ts": "2025-01-02T10:00:00Z", "value": 1},
    ]

    with open(str(events_dir / "day_2025-01-01.jsonl"), "w") as f:
        for row in day1:
            f.write(json.dumps(row) + "\n")
    with open(str(events_dir / "day_2025-01-02.jsonl"), "w") as f:
        for row in day2:
            f.write(json.dumps(row) + "\n")

    users_path = str(tmp_path / "users.csv")
    with open(users_path, "w") as f:
        f.write("user_id,country,signup_date\n")
        f.write("u1,ID,2024-12-01\n")
        f.write("u2,US,2024-12-15\n")

    output_dir = str(tmp_path / "output")
    return {
        "raw_path": str(events_dir),
        "users_path": users_path,
        "output_base": output_dir,
    }


class TestEndToEnd:
    def test_full_pipeline(self, spark, sample_data):
        raw_path = sample_data["raw_path"]
        users_path = sample_data["users_path"]
        output_base = sample_data["output_base"]

        # Bronze
        clean_df = run_bronze(spark, raw_path, output_base)
        assert clean_df.count() > 0

        # Verify quarantined records exist
        quarantined = spark.read.parquet(f"{output_base}/bronze/quarantined")
        assert quarantined.count() > 0

        # Silver
        enriched_df = run_silver(spark, output_base, users_path)
        assert "is_purchase" in enriched_df.columns
        assert "days_since_signup" in enriched_df.columns
        assert "country" in enriched_df.columns

        # Gold
        agg_df = run_gold(spark, output_base)
        assert set(agg_df.columns) == {
            "event_date", "country", "total_events",
            "total_value", "total_purchases", "unique_users",
        }
        assert agg_df.count() > 0

    def test_idempotency(self, spark, sample_data):
        """Running the pipeline twice should produce the same results."""
        raw_path = sample_data["raw_path"]
        users_path = sample_data["users_path"]
        output_base = sample_data["output_base"]

        # Run pipeline twice
        run_bronze(spark, raw_path, output_base)
        run_silver(spark, output_base, users_path)
        run_gold(spark, output_base)

        first_count = spark.read.parquet(f"{output_base}/gold/daily_country_metrics").count()

        run_bronze(spark, raw_path, output_base)
        run_silver(spark, output_base, users_path)
        run_gold(spark, output_base)

        second_count = spark.read.parquet(f"{output_base}/gold/daily_country_metrics").count()

        assert first_count == second_count

    def test_deduplication_across_files(self, spark, sample_data):
        """event_id e3 appears in both day1 and day2 files - should appear only once after bronze."""
        raw_path = sample_data["raw_path"]
        output_base = sample_data["output_base"]

        run_bronze(spark, raw_path, output_base)
        clean = spark.read.parquet(f"{output_base}/bronze/events")
        e3_count = clean.filter(F.col("event_id") == "e3").count()
        assert e3_count == 1
