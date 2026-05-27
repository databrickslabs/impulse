"""Integration tests for mda_query_engine.perception.scalar_metrics.derive_channel_metrics_from_channels.

Requires a real SparkSession; uses the session-scoped `spark` fixture from
tests/conftest.py.
"""

import pytest
import pyspark.sql.types as T

from mda_query_engine.perception.scalar_metrics import derive_channel_metrics_from_channels


_CHANNELS_SCHEMA = T.StructType([
    T.StructField("container_id", T.LongType(), nullable=False),
    T.StructField("channel_id",   T.IntegerType(), nullable=False),
    T.StructField("tstart",       T.LongType(),  nullable=False),
    T.StructField("tend",         T.LongType(),  nullable=False),
    T.StructField("value",        T.DoubleType(), nullable=True),
])


@pytest.fixture
def populated_channels_table(spark, tmp_path):
    table = "spark_catalog.silver.test_scalar_metrics_channels"
    rows = [
        # Container 1, channel 100: 3 samples
        (1, 100, 1_000_000, 1_100_000, 10.0),
        (1, 100, 1_100_000, 1_200_000, 20.0),
        (1, 100, 1_200_000, 1_300_000, 30.0),
        # Container 1, channel 200: 2 samples (one NaN)
        (1, 200, 2_000_000, 2_100_000, 5.0),
        (1, 200, 2_100_000, 2_200_000, float("nan")),
        # Container 2, channel 100: 1 sample
        (2, 100, 3_000_000, 3_100_000, 42.0),
    ]
    df = spark.createDataFrame(rows, _CHANNELS_SCHEMA)
    df.write.format("delta").mode("overwrite").saveAsTable(table)
    yield table
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_row_count_matches_distinct_pairs(spark, populated_channels_table):
    result = derive_channel_metrics_from_channels(spark, populated_channels_table).collect()
    pairs = {(r.container_id, r.channel_id) for r in result}
    assert pairs == {(1, 100), (1, 200), (2, 100)}
    assert len(result) == 3


def test_sample_count_is_correct(spark, populated_channels_table):
    by_key = {
        (r.container_id, r.channel_id): r
        for r in derive_channel_metrics_from_channels(spark, populated_channels_table).collect()
    }
    assert by_key[(1, 100)].sample_count == 3
    assert by_key[(1, 200)].sample_count == 2
    assert by_key[(2, 100)].sample_count == 1


def test_nan_ratio_counts_nans(spark, populated_channels_table):
    by_key = {
        (r.container_id, r.channel_id): r
        for r in derive_channel_metrics_from_channels(spark, populated_channels_table).collect()
    }
    # Channel 200 has 1/2 NaN.
    assert by_key[(1, 200)].nan_ratio == pytest.approx(0.5)
    # Channel 100 has 0/3 NaN.
    assert by_key[(1, 100)].nan_ratio == pytest.approx(0.0)


def test_value_aggregates(spark, populated_channels_table):
    by_key = {
        (r.container_id, r.channel_id): r
        for r in derive_channel_metrics_from_channels(spark, populated_channels_table).collect()
    }
    # Channel (1, 100): values [10, 20, 30].
    assert by_key[(1, 100)].min == pytest.approx(10.0)
    assert by_key[(1, 100)].max == pytest.approx(30.0)
    assert by_key[(1, 100)].mean == pytest.approx(20.0)


def test_time_range_columns_present(spark, populated_channels_table):
    by_key = {
        (r.container_id, r.channel_id): r
        for r in derive_channel_metrics_from_channels(spark, populated_channels_table).collect()
    }
    # Channel (1, 100): tstart range [1_000_000, 1_300_000] microseconds → begin/end in seconds.
    assert by_key[(1, 100)].begin_s == pytest.approx(1.0)
    assert by_key[(1, 100)].end_s == pytest.approx(1.3)


def test_required_columns_are_present(spark, populated_channels_table):
    df = derive_channel_metrics_from_channels(spark, populated_channels_table)
    cols = set(df.columns)
    required = {
        "container_id", "channel_id", "value_type", "sample_count", "nan_ratio",
        "begin_s", "end_s", "duration_ms", "original_sample_count", "original_sr",
        "min", "max", "mean", "std", "pz1", "pz10", "pz90", "pz99",
    }
    assert required.issubset(cols)
