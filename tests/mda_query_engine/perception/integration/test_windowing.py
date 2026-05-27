"""Integration tests for mda_query_engine.perception.windowing.filter_dataframe_to_windows.

Requires a real SparkSession; uses the session-scoped `spark` fixture from
tests/conftest.py.
"""

import pyspark.sql.types as T
import pytest

from mda_query_engine.perception.windowing import filter_dataframe_to_windows


ROW_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("frame_ts", T.LongType(), nullable=False),
        T.StructField("object_id", T.LongType(), nullable=False),
    ]
)

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("event_id", T.StringType(), nullable=False),
        T.StructField("start_ts", T.LongType(), nullable=False),
        T.StructField("end_ts", T.LongType(), nullable=False),
    ]
)


def _rows(spark, data):
    return spark.createDataFrame(data, schema=ROW_SCHEMA)


def _events(spark, data):
    return spark.createDataFrame(data, schema=EVENT_SCHEMA)


class TestFilterDataframeToWindows:
    def test_keeps_only_rows_inside_a_window(self, spark):
        rows = _rows(
            spark,
            [
                (1, 1_000_000, 100),  # before window  — drop
                (1, 1_500_000, 101),  # inside         — keep
                (1, 2_500_000, 102),  # after window   — drop
            ],
        )
        events = _events(spark, [(1, "e1", 1_200_000, 1_800_000)])
        out = filter_dataframe_to_windows(
            rows, events, pre_buffer_us=0, post_buffer_us=0
        ).collect()
        assert {r["object_id"] for r in out} == {101}

    def test_buffer_extends_window_both_sides(self, spark):
        rows = _rows(
            spark,
            [
                (1, 700_000, 100),  # 500 ms before start, inside pre-buffer
                (1, 2_300_000, 101),  # 300 ms after end,    inside post-buffer
                (1, 3_000_000, 102),  # 1 s after end,       outside
            ],
        )
        events = _events(spark, [(1, "e1", 1_200_000, 2_000_000)])
        out = filter_dataframe_to_windows(rows, events).collect()
        assert {r["object_id"] for r in out} == {100, 101}

    def test_per_container_isolation(self, spark):
        # Same frame_ts on a different container_id must not match.
        rows = _rows(
            spark,
            [
                (1, 1_500_000, 100),  # matches container 1's event
                (2, 1_500_000, 200),  # container 2 has no event — drop
            ],
        )
        events = _events(spark, [(1, "e1", 1_200_000, 1_800_000)])
        out = filter_dataframe_to_windows(
            rows, events, pre_buffer_us=0, post_buffer_us=0
        ).collect()
        assert {r["object_id"] for r in out} == {100}

    def test_row_matching_two_overlapping_windows_returns_once(self, spark):
        rows = _rows(spark, [(1, 1_500_000, 100)])
        events = _events(
            spark,
            [
                (1, "e1", 1_000_000, 1_600_000),
                (1, "e2", 1_400_000, 1_800_000),
            ],
        )
        out = filter_dataframe_to_windows(
            rows, events, pre_buffer_us=0, post_buffer_us=0
        ).collect()
        assert len(out) == 1
        assert out[0]["object_id"] == 100

    def test_empty_events_returns_empty(self, spark):
        rows = _rows(spark, [(1, 1_500_000, 100)])
        events = spark.createDataFrame([], schema=EVENT_SCHEMA)
        out = filter_dataframe_to_windows(rows, events).collect()
        assert out == []

    def test_preserves_input_columns(self, spark):
        rows = _rows(spark, [(1, 1_500_000, 100)])
        events = _events(spark, [(1, "e1", 1_200_000, 1_800_000)])
        out = filter_dataframe_to_windows(rows, events)
        assert out.columns == ["container_id", "frame_ts", "object_id"]
