"""Integration tests for mda_query_engine.perception.perception_db.frame_nearest_to.

Requires a real SparkSession; uses the session-scoped `spark` fixture from
tests/conftest.py.
"""

import pytest
import pyspark.sql.types as T

from mda_query_engine.perception.perception_db import frame_nearest_to


_PERCEPTION_SCHEMA = T.StructType([
    T.StructField("container_id", T.LongType(),    nullable=False),
    T.StructField("channel_id",   T.IntegerType(), nullable=False),
    T.StructField("timestamp",    T.LongType(),    nullable=False),
    T.StructField("file_path",    T.StringType(),  nullable=False),
    T.StructField("format",       T.StringType(),  nullable=True),
])


@pytest.fixture
def populated_perception_table(spark):
    table = "spark_catalog.silver.test_perception_frame_nearest"
    rows = [
        (1, 10, 1_000_000, "/a", "jpg"),
        (1, 10, 1_500_000, "/b", "jpg"),
        (1, 10, 2_000_000, "/c", "jpg"),
        (1, 20, 1_000_000, "/d", "bin"),  # different channel — should be filtered
        (2, 10, 1_000_000, "/e", "jpg"),  # different container — should be filtered
    ]
    df = spark.createDataFrame(rows, _PERCEPTION_SCHEMA)
    df.write.format("delta").mode("overwrite").saveAsTable(table)
    yield table
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_picks_exact_match(spark, populated_perception_table):
    row = frame_nearest_to(spark, populated_perception_table, container_id=1, channel_id=10, target_ts=1_500_000)
    assert row.file_path == "/b"


def test_picks_closest_when_no_exact_match(spark, populated_perception_table):
    row = frame_nearest_to(spark, populated_perception_table, container_id=1, channel_id=10, target_ts=1_300_000)
    # 1_300_000 is closer to 1_500_000 (Δ=200_000) than 1_000_000 (Δ=300_000).
    assert row.file_path == "/b"


def test_filters_by_container(spark, populated_perception_table):
    row = frame_nearest_to(spark, populated_perception_table, container_id=2, channel_id=10, target_ts=1_500_000)
    assert row.file_path == "/e"


def test_filters_by_channel(spark, populated_perception_table):
    row = frame_nearest_to(spark, populated_perception_table, container_id=1, channel_id=20, target_ts=1_500_000)
    assert row.file_path == "/d"


def test_extreme_target_picks_closest_endpoint(spark, populated_perception_table):
    row = frame_nearest_to(spark, populated_perception_table, container_id=1, channel_id=10, target_ts=999_999_999)
    assert row.file_path == "/c"
    row = frame_nearest_to(spark, populated_perception_table, container_id=1, channel_id=10, target_ts=0)
    assert row.file_path == "/a"


def test_returns_none_when_no_match(spark, populated_perception_table):
    row = frame_nearest_to(spark, populated_perception_table, container_id=99, channel_id=99, target_ts=0)
    assert row is None
