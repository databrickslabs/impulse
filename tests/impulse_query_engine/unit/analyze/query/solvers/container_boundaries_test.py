# pylint: disable=missing-function-docstring, redefined-outer-name
"""Tests for SolverConfig.with_window_bounds.

``TimeWindowEvent`` windows are computed in the channel time frame
(``channel_time_unit`` / ``channel_time_origin``). ``with_window_bounds`` derives the container
start/stop in that frame as two extra columns and leaves the raw ``start_ts`` / ``stop_ts``
untouched for everyone else (UDFs, ``ContainerEvent``, ``measurement_dimension``).
"""

import datetime as dt

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig
from tests.conftest import spark  # noqa: F401  (pytest fixture)

# 2025-07-03 07:41:41.483456 UTC
_EPOCH_MICROS = 1_751_528_501_483_456
# One hour and half a second later.
_SPAN_MICROS = 3_600_500_000
_START, _STOP = "__window_start", "__window_stop"


def _boundaries_df(spark: SparkSession):  # noqa: F811
    """container_metrics-like frame with TIMESTAMP boundaries (and a null row)."""
    df = spark.createDataFrame(
        [(1, _EPOCH_MICROS, _EPOCH_MICROS + _SPAN_MICROS), (2, None, None)],
        "container_id int, start_us long, stop_us long",
    )
    return df.select(
        "container_id",
        F.timestamp_micros("start_us").alias("start_ts"),
        F.timestamp_micros("stop_us").alias("stop_ts"),
    )


def _bounds(cfg: SolverConfig, df) -> dict:
    out = cfg.with_window_bounds(df)
    return {r.container_id: (r[_START], r[_STOP]) for r in out.collect()}


def test_window_bound_column_names():
    cfg = SolverConfig()
    assert (cfg.window_start_col, cfg.window_stop_col) == (_START, _STOP)


@pytest.mark.parametrize(
    "unit, expected_type, expected_start",
    [
        ("s", T.DoubleType(), _EPOCH_MICROS / 1e6),
        ("ms", T.DoubleType(), _EPOCH_MICROS / 1e3),
        ("us", T.LongType(), _EPOCH_MICROS),
        ("ns", T.LongType(), _EPOCH_MICROS * 1000),
    ],
)
@pytest.mark.parametrize("session_tz", ["UTC", "Europe/Berlin"])
def test_epoch_origin_converts_timestamps_to_unit(
    spark, unit, expected_type, expected_start, session_tz  # noqa: F811
):
    previous_tz = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", session_tz)
    try:
        out = SolverConfig(channel_time_unit=unit).with_window_bounds(_boundaries_df(spark))
        rows = {r.container_id: r for r in out.collect()}
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous_tz)

    assert out.schema[_START].dataType == expected_type
    assert rows[1][_START] == expected_start  # exact, independent of the session time zone
    assert rows[2][_START] is None and rows[2][_STOP] is None


def test_epoch_seconds_match_spark_cast_to_double(spark):  # noqa: F811
    # "s" equals Spark's cast(timestamp as double), bit for bit.
    df = _boundaries_df(spark)
    casted = {
        r.container_id: (r.s, r.e)
        for r in df.select(
            "container_id",
            F.col("start_ts").cast("double").alias("s"),
            F.col("stop_ts").cast("double").alias("e"),
        ).collect()
    }
    assert _bounds(SolverConfig(channel_time_unit="s"), df) == casted


@pytest.mark.parametrize(
    "unit, expected_stop", [("s", 3600.5), ("ms", 3_600_500.0), ("us", _SPAN_MICROS)]
)
@pytest.mark.parametrize("session_tz", ["UTC", "Europe/Berlin"])
def test_container_start_origin_gives_relative_bounds(
    spark, unit, expected_stop, session_tz  # noqa: F811
):
    previous_tz = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", session_tz)
    try:
        cfg = SolverConfig(channel_time_unit=unit, channel_time_origin="container_start")
        bounds = _bounds(cfg, _boundaries_df(spark))
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous_tz)

    assert bounds[1] == (0, expected_stop)
    # A null boundary leaves a null stop bound, so the container gets no windows.
    assert bounds[2][1] is None


def test_numeric_boundaries_epoch_as_is_and_container_start_shifted(spark):  # noqa: F811
    df = spark.createDataFrame(
        [(1, 1000.5, 4601.0)], "container_id int, start_ts double, stop_ts double"
    )
    assert _bounds(SolverConfig(), df) == {1: (1000.5, 4601.0)}
    # Shift only: numeric boundaries are already in the channels' unit, so no unit is needed.
    assert _bounds(SolverConfig(channel_time_origin="container_start"), df) == {1: (0, 3600.5)}


def _ms_boundaries_df(spark: SparkSession):  # noqa: F811
    """The TIMESTAMP boundaries of _boundaries_df as epoch-ms longs (container 1 only)."""
    return (
        _boundaries_df(spark)
        .filter(F.col("container_id") == 1)
        .select(
            "container_id",
            F.unix_millis("start_ts").alias("start_ts"),
            F.unix_millis("stop_ts").alias("stop_ts"),
        )
    )


def test_numeric_ms_boundaries_converted_to_finer_channel_unit_exactly(spark):  # noqa: F811
    # Boundaries in epoch ms, channels in µs: an integer factor keeps the longs exact.
    cfg = SolverConfig(channel_time_unit="us", container_time_unit="ms")
    out = cfg.with_window_bounds(_ms_boundaries_df(spark))
    start_ms = _EPOCH_MICROS // 1000
    stop_ms = (_EPOCH_MICROS + _SPAN_MICROS) // 1000
    assert out.schema[_START].dataType == T.LongType()
    assert _bounds(cfg, _ms_boundaries_df(spark)) == {1: (start_ms * 1000, stop_ms * 1000)}

    relative = SolverConfig(
        channel_time_unit="us", channel_time_origin="container_start", container_time_unit="ms"
    )
    # The difference is taken in ms first, then converted.
    assert _bounds(relative, _ms_boundaries_df(spark)) == {1: (0, (stop_ms - start_ms) * 1000)}


def test_numeric_boundaries_converted_to_coarser_channel_unit(spark):  # noqa: F811
    # Boundaries in epoch µs, channels in ms: a division, giving doubles.
    df = spark.createDataFrame(
        [(1, _EPOCH_MICROS, _EPOCH_MICROS + _SPAN_MICROS)],
        "container_id int, start_ts long, stop_ts long",
    )
    cfg = SolverConfig(channel_time_unit="ms", container_time_unit="us")
    out = cfg.with_window_bounds(df)
    assert out.schema[_START].dataType == T.DoubleType()
    assert _bounds(cfg, df) == {
        1: (_EPOCH_MICROS / 1000.0, (_EPOCH_MICROS + _SPAN_MICROS) / 1000.0)
    }


def test_numeric_boundaries_unchanged_for_equal_or_unset_container_unit(spark):  # noqa: F811
    df = _ms_boundaries_df(spark)
    raw = {r.container_id: (r.start_ts, r.stop_ts) for r in df.collect()}
    assert _bounds(SolverConfig(channel_time_unit="ms", container_time_unit="ms"), df) == raw
    assert _bounds(SolverConfig(channel_time_unit="us"), df) == raw


def test_container_time_unit_rejected_for_timestamp_boundaries(spark):  # noqa: F811
    cfg = SolverConfig(channel_time_unit="s", container_time_unit="ms")
    with pytest.raises(ValueError, match="container_time_unit only applies to numeric"):
        cfg.with_window_bounds(_boundaries_df(spark))


def test_container_time_unit_requires_channel_time_unit():
    with pytest.raises(ValueError, match="container_time_unit requires channel_time_unit"):
        SolverConfig.model_validate({"container_time_unit": "ms"})
    cfg = SolverConfig.model_validate({"channel_time_unit": "us", "container_time_unit": "ms"})
    assert (cfg.channel_time_unit, cfg.container_time_unit) == ("us", "ms")


def test_raw_boundaries_stay_unchanged(spark):  # noqa: F811
    df = _boundaries_df(spark)
    cfg = SolverConfig(channel_time_unit="s", channel_time_origin="container_start")
    out = cfg.with_window_bounds(df)
    assert isinstance(out.schema["start_ts"].dataType, T.TimestampType)
    assert out.select("container_id", "start_ts", "stop_ts").collect() == df.collect()


def test_timestamp_boundaries_without_unit_rejected(spark):  # noqa: F811
    for origin in ("epoch", "container_start"):
        with pytest.raises(ValueError, match=r"TimeWindowEvent.*channel_time_unit"):
            SolverConfig(channel_time_origin=origin).with_window_bounds(_boundaries_df(spark))


@pytest.mark.parametrize(
    "value, ddl",
    [(dt.datetime(2025, 7, 3, 7, 41, 41), "timestamp_ntz"), (dt.date(2025, 7, 3), "date")],
)
def test_zone_less_types_rejected(spark, value, ddl):  # noqa: F811
    df = spark.createDataFrame(
        [(1, value, value)], f"container_id int, start_ts {ddl}, stop_ts {ddl}"
    )
    with pytest.raises(ValueError, match="start_ts"):
        SolverConfig(channel_time_unit="s").with_window_bounds(df)


def test_mixed_and_missing_boundaries_rejected(spark):  # noqa: F811
    mixed = _boundaries_df(spark).withColumn("stop_ts", F.lit(1.0))
    with pytest.raises(ValueError, match="both be TIMESTAMP or both be numeric"):
        SolverConfig(channel_time_unit="s").with_window_bounds(mixed)
    with pytest.raises(ValueError, match="stop_ts"):
        SolverConfig().with_window_bounds(_boundaries_df(spark).drop("stop_ts"))


def test_channel_time_settings_validated():
    cfg = SolverConfig.model_validate(
        {"channel_time_unit": "ns", "channel_time_origin": "container_start"}
    )
    assert (cfg.channel_time_unit, cfg.channel_time_origin) == ("ns", "container_start")
    assert SolverConfig().channel_time_origin == "epoch"
    with pytest.raises(ValueError):
        SolverConfig.model_validate({"channel_time_unit": "minutes"})
    with pytest.raises(ValueError):
        SolverConfig.model_validate({"channel_time_origin": "recording_start"})
