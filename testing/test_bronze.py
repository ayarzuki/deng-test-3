import json
import os
import tempfile
import pytest
from pyspark.sql import functions as F

from job.bronze import ingest_events, split_valid_corrupt, validate_and_clean


class TestIngestEvents:
    def test_reads_jsonl_with_schema(self, spark, tmp_path):
        data = [
            {"event_id": "e1", "user_id": "u1", "event_type": "CLICK", "event_ts": "2025-01-01T10:00:00Z", "value": "3"},
        ]
        path = str(tmp_path / "events")
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, "test.jsonl"), "w") as f:
            for row in data:
                f.write(json.dumps(row) + "\n")

        df = ingest_events(spark, path)
        assert df.count() == 1
        assert "event_id" in df.columns


class TestValidateAndClean:
    def _make_df(self, spark, rows):
        from job.schemas import RAW_EVENT_SCHEMA
        return spark.createDataFrame(rows, schema=RAW_EVENT_SCHEMA)

    def test_valid_records_pass(self, spark):
        rows = [("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", "3")]
        df = self._make_df(spark, rows)
        clean, quarantined = validate_and_clean(df)
        assert clean.count() == 1
        assert quarantined.count() == 0

    def test_null_user_id_quarantined(self, spark):
        rows = [("e1", None, "CLICK", "2025-01-01T10:00:00Z", "3")]
        df = self._make_df(spark, rows)
        clean, quarantined = validate_and_clean(df)
        assert clean.count() == 0
        assert quarantined.count() == 1

    def test_empty_user_id_quarantined(self, spark):
        rows = [("e1", "", "CLICK", "2025-01-01T10:00:00Z", "3")]
        df = self._make_df(spark, rows)
        clean, quarantined = validate_and_clean(df)
        assert clean.count() == 0
        assert quarantined.count() == 1

    def test_invalid_event_type_quarantined(self, spark):
        rows = [("e1", "u1", "INVALID", "2025-01-01T10:00:00Z", "3")]
        df = self._make_df(spark, rows)
        clean, quarantined = validate_and_clean(df)
        assert clean.count() == 0
        assert quarantined.count() == 1

    def test_null_event_type_quarantined(self, spark):
        rows = [("e1", "u1", None, "2025-01-01T10:00:00Z", "3")]
        df = self._make_df(spark, rows)
        clean, quarantined = validate_and_clean(df)
        assert clean.count() == 0
        assert quarantined.count() == 1

    def test_invalid_timestamp_quarantined(self, spark):
        rows = [("e1", "u1", "CLICK", "invalid_ts", "3")]
        df = self._make_df(spark, rows)
        clean, quarantined = validate_and_clean(df)
        assert clean.count() == 0
        assert quarantined.count() == 1

    def test_null_timestamp_quarantined(self, spark):
        rows = [("e1", "u1", "CLICK", None, "3")]
        df = self._make_df(spark, rows)
        clean, quarantined = validate_and_clean(df)
        assert clean.count() == 0
        assert quarantined.count() == 1

    def test_event_type_normalized_to_uppercase(self, spark):
        rows = [("e1", "u1", "click", "2025-01-01T10:00:00Z", "3")]
        df = self._make_df(spark, rows)
        clean, _ = validate_and_clean(df)
        assert clean.count() == 1
        assert clean.collect()[0]["event_type"] == "CLICK"

    def test_deduplication_by_event_id(self, spark):
        rows = [
            ("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", "3"),
            ("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", "3"),
        ]
        df = self._make_df(spark, rows)
        clean, _ = validate_and_clean(df)
        assert clean.count() == 1

    def test_null_value_filled_with_zero(self, spark):
        rows = [("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", None)]
        df = self._make_df(spark, rows)
        clean, _ = validate_and_clean(df)
        row = clean.collect()[0]
        assert row["value"] == 0.0

    def test_value_cast_to_double(self, spark):
        rows = [("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", "30")]
        df = self._make_df(spark, rows)
        clean, _ = validate_and_clean(df)
        row = clean.collect()[0]
        assert row["value"] == 30.0

    def test_invalid_month_timestamp_quarantined(self, spark):
        rows = [("e1", "u1", "PURCHASE", "2025-13-01T00:00:00Z", "5")]
        df = self._make_df(spark, rows)
        clean, quarantined = validate_and_clean(df)
        assert clean.count() == 0
        assert quarantined.count() == 1
