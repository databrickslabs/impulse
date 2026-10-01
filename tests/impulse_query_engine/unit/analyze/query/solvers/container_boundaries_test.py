# pylint: disable=missing-function-docstring, redefined-outer-name
"""Tests for SolverConfig.normalize_container_boundaries / require_epoch_boundaries.

TIMESTAMP-typed container ``start_ts`` / ``stop_ts`` are converted to epoch numbers in
``SolverConfig.epoch_unit`` (opt-in), so container-boundary events and the solve see the
same values. With ``epoch_unit`` unset nothing changes.
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


def _boundaries_df(spark: SparkSession):  # noqa: F811
    """container_metrics-like frame with TIMESTAMP boundaries (and a null row)."""
    df = spark.createDataFrame(
        [(1, _EPOCH_MICROS, _EPOCH_MICROS + 60_000_000), (2, None, None)],
        "container_id int, start_us long, stop_us long",
    )
    return df.select(
        "container_id",
        F.timestamp_micros("start_us").alias("start_ts"),
        F.timestamp_micros("stop_us").alias("stop_ts"),
    )


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
def test_timestamp_boundaries_converted_to_epoch_unit(
    spark, unit, expected_type, expected_start, session_tz  # noqa: F811
):
    previous_tz = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", session_tz)
    try:
        out = SolverConfig(epoch_unit=unit).normalize_container_boundaries(_boundaries_df(spark))
        rows = {r.container_id: r for r in out.collect()}
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous_tz)

    assert out.schema["start_ts"].dataType == expected_type
    assert out.schema["stop_ts"].dataType == expected_type
    assert rows[1].start_ts == expected_start  # exact, independent of the session time zone
    assert rows[2].start_ts is None and rows[2].stop_ts is None


def test_seconds_match_spark_cast_to_double(spark):  # noqa: F811
    # "s" must equal today's ContainerEvent cast(timestamp as double), bit for bit.
    df = _boundaries_df(spark)
    out = SolverConfig(epoch_unit="s").normalize_container_boundaries(df)
    converted = {r.container_id: (r.start_ts, r.stop_ts) for r in out.collect()}
    casted = {
        r.container_id: (r.s, r.e)
        for r in df.select(
            "container_id",
            F.col("start_ts").cast("double").alias("s"),
            F.col("stop_ts").cast("double").alias("e"),
        ).collect()
    }
    assert converted == casted


def test_unset_epoch_unit_leaves_frame_unchanged(spark):  # noqa: F811
    df = _boundaries_df(spark)
    out = SolverConfig().normalize_container_boundaries(df)
    assert out is df
    assert isinstance(out.schema["start_ts"].dataType, T.TimestampType)


def test_numeric_boundaries_unchanged(spark):  # noqa: F811
    df = spark.createDataFrame([(1, 100, 200)], "container_id int, start_ts long, stop_ts long")
    out = SolverConfig(epoch_unit="ms").normalize_container_boundaries(df)
    assert out.schema == df.schema
    assert out.collect() == df.collect()


@pytest.mark.parametrize(
    "value, ddl",
    [(dt.datetime(2025, 7, 3, 7, 41, 41), "timestamp_ntz"), (dt.date(2025, 7, 3), "date")],
)
def test_zone_less_types_rejected_when_unit_set(spark, value, ddl):  # noqa: F811
    df = spark.createDataFrame(
        [(1, value, value)], f"container_id int, start_ts {ddl}, stop_ts {ddl}"
    )
    with pytest.raises(ValueError, match="start_ts"):
        SolverConfig(epoch_unit="s").normalize_container_boundaries(df)
    # Opt-in only: without epoch_unit the frame passes through untouched.
    assert SolverConfig().normalize_container_boundaries(df) is df


def test_require_epoch_boundaries(spark):  # noqa: F811
    df = _boundaries_df(spark)
    with pytest.raises(ValueError, match=r"TimeWindowEvent.*epoch_unit"):
        SolverConfig().require_epoch_boundaries(df, owner="TimeWindowEvent")

    cfg = SolverConfig(epoch_unit="s")
    cfg.require_epoch_boundaries(cfg.normalize_container_boundaries(df), owner="TimeWindowEvent")

    numeric = spark.createDataFrame(
        [(1, 1.0, 2.0)], "container_id int, start_ts double, stop_ts double"
    )
    SolverConfig().require_epoch_boundaries(numeric, owner="TimeWindowEvent")


def test_epoch_unit_validated():
    assert SolverConfig.model_validate({"epoch_unit": "ns"}).epoch_unit == "ns"
    with pytest.raises(ValueError):
        SolverConfig.model_validate({"epoch_unit": "minutes"})
