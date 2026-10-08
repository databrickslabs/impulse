# pylint: disable=missing-function-docstring, redefined-outer-name
"""Tests for solvers.utils.window_bounds.with_window_bounds.

``TimeWindowEvent`` windows are computed in the channel time frame
(``channel_time_unit`` / ``channel_time_origin``). ``with_window_bounds`` derives the container
start/stop in that frame as two extra columns and leaves the raw ``start_ts`` / ``stop_ts``
untouched for everyone else (UDFs, ``ContainerEvent``, ``measurement_dimension``).

All frames are the ``basic_narrow_db`` fixture's ``container_metrics`` boundaries (epoch-ms
longs), recast per test; container 2's boundaries are nulled to cover missing values.
"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from pyspark.sql import DataFrame, SparkSession

from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig
from impulse_query_engine.analyze.query.solvers.utils.window_bounds import with_window_bounds
from impulse_query_engine.measurement_db import MeasurementDB
from tests.conftest import basic_narrow_db, spark  # noqa: F401  (pytest fixtures)

_START, _STOP = "__window_start", "__window_stop"
_NULL_CONTAINER = 2


def _ms_boundaries(spark: SparkSession, db: MeasurementDB) -> DataFrame:  # noqa: F811
    """The fixture's container boundaries (epoch-ms longs), container 2's set to null."""

    def unless_null_container(name: str):
        return F.when(F.col("container_id") != _NULL_CONTAINER, F.col(name)).alias(name)

    return db.container_metrics(spark).select(
        "container_id", unless_null_container("start_ts"), unless_null_container("stop_ts")
    )


def _recast(df: DataFrame, cast) -> DataFrame:
    """*df* with ``start_ts`` / ``stop_ts`` passed through *cast* (a Column -> Column)."""
    return df.select(
        "container_id",
        cast(F.col("start_ts")).alias("start_ts"),
        cast(F.col("stop_ts")).alias("stop_ts"),
    )


def _timestamp_boundaries(spark: SparkSession, db: MeasurementDB) -> DataFrame:  # noqa: F811
    return _recast(_ms_boundaries(spark, db), F.timestamp_millis)


def _raw_ms(spark: SparkSession, db: MeasurementDB) -> dict:  # noqa: F811
    """``{container_id: (start_ms, stop_ms)}`` of the containers with boundaries."""
    return {
        r.container_id: (r.start_ts, r.stop_ts)
        for r in _ms_boundaries(spark, db).collect()
        if r.start_ts is not None
    }


def _bounds(cfg: SolverConfig, df: DataFrame) -> dict:
    out = with_window_bounds(df, cfg)
    return {r.container_id: (r[_START], r[_STOP]) for r in out.collect()}


def test_window_bound_column_names():
    cfg = SolverConfig()
    assert (cfg.window_start_col, cfg.window_stop_col) == (_START, _STOP)


@pytest.mark.parametrize(
    "unit, expected_type, from_micros",
    [
        ("s", T.DoubleType(), lambda us: us / 1e6),
        ("ms", T.DoubleType(), lambda us: us / 1e3),
        ("us", T.LongType(), lambda us: us),
        ("ns", T.LongType(), lambda us: us * 1000),
    ],
)
@pytest.mark.parametrize("session_tz", ["UTC", "Europe/Berlin"])
def test_epoch_origin_converts_timestamps_to_unit(
    spark, basic_narrow_db, unit, expected_type, from_micros, session_tz  # noqa: F811
):
    previous_tz = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", session_tz)
    try:
        df = _timestamp_boundaries(spark, basic_narrow_db)
        out = with_window_bounds(df, SolverConfig(channel_time_unit=unit))
        bounds = {r.container_id: (r[_START], r[_STOP]) for r in out.collect()}
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous_tz)

    assert out.schema[_START].dataType == expected_type
    # Exact and independent of the session time zone.
    for cid, (start_ms, stop_ms) in _raw_ms(spark, basic_narrow_db).items():
        assert bounds[cid] == (from_micros(start_ms * 1000), from_micros(stop_ms * 1000))
    assert bounds[_NULL_CONTAINER] == (None, None)


def test_epoch_seconds_match_spark_cast_to_double(spark, basic_narrow_db):  # noqa: F811
    # "s" equals Spark's cast(timestamp as double), bit for bit.
    df = _timestamp_boundaries(spark, basic_narrow_db)
    casted = {
        r.container_id: (r.start_ts, r.stop_ts)
        for r in _recast(df, lambda c: c.cast("double")).collect()
    }
    assert _bounds(SolverConfig(channel_time_unit="s"), df) == casted


@pytest.mark.parametrize(
    "unit, from_micros",
    [("s", lambda us: us / 1e6), ("ms", lambda us: us / 1e3), ("us", lambda us: us)],
)
@pytest.mark.parametrize("session_tz", ["UTC", "Europe/Berlin"])
def test_container_start_origin_gives_relative_bounds(
    spark, basic_narrow_db, unit, from_micros, session_tz  # noqa: F811
):
    previous_tz = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", session_tz)
    try:
        cfg = SolverConfig(channel_time_unit=unit, channel_time_origin="container_start")
        bounds = _bounds(cfg, _timestamp_boundaries(spark, basic_narrow_db))
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous_tz)

    for cid, (start_ms, stop_ms) in _raw_ms(spark, basic_narrow_db).items():
        assert bounds[cid] == (0, from_micros((stop_ms - start_ms) * 1000))
    # A null boundary leaves a null stop bound, so the container gets no windows.
    assert bounds[_NULL_CONTAINER][1] is None


def test_numeric_boundaries_epoch_as_is_and_container_start_shifted(
    spark, basic_narrow_db  # noqa: F811
):
    df = _ms_boundaries(spark, basic_narrow_db)
    raw = _raw_ms(spark, basic_narrow_db)
    epoch = _bounds(SolverConfig(), df)
    assert all(epoch[cid] == bounds for cid, bounds in raw.items())
    # Shift only: numeric boundaries are already in the channels' unit, so no unit is needed.
    relative = _bounds(SolverConfig(channel_time_origin="container_start"), df)
    assert all(relative[cid] == (0, stop - start) for cid, (start, stop) in raw.items())


def test_numeric_ms_boundaries_converted_to_finer_channel_unit_exactly(
    spark, basic_narrow_db  # noqa: F811
):
    # Boundaries in epoch ms, channels in µs: an integer factor keeps the longs exact.
    df = _ms_boundaries(spark, basic_narrow_db)
    raw = _raw_ms(spark, basic_narrow_db)
    cfg = SolverConfig(channel_time_unit="us", container_time_unit="ms")
    assert with_window_bounds(df, cfg).schema[_START].dataType == T.LongType()
    bounds = _bounds(cfg, df)
    assert all(bounds[cid] == (start * 1000, stop * 1000) for cid, (start, stop) in raw.items())

    relative = SolverConfig(
        channel_time_unit="us", channel_time_origin="container_start", container_time_unit="ms"
    )
    # The difference is taken in ms first, then converted.
    bounds = _bounds(relative, df)
    assert all(bounds[cid] == (0, (stop - start) * 1000) for cid, (start, stop) in raw.items())


@pytest.mark.parametrize("ansi", ["true", "false"])
def test_int_boundaries_widen_to_long_instead_of_overflowing(
    spark, basic_narrow_db, ansi  # noqa: F811
):
    """INT epoch seconds * 1000 exceeds int32. Spark keeps int * int as int, which raised
    ARITHMETIC_OVERFLOW under ANSI and silently wrapped to negative bounds without it."""
    int_seconds = _recast(
        _ms_boundaries(spark, basic_narrow_db), lambda c: (c / F.lit(1000)).cast("int")
    )
    raw_s = {
        cid: (start // 1000, stop // 1000)
        for cid, (start, stop) in _raw_ms(spark, basic_narrow_db).items()
    }
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    try:
        assert int_seconds.schema["start_ts"].dataType == T.IntegerType()
        cfg = SolverConfig(channel_time_unit="ms", container_time_unit="s")
        assert with_window_bounds(int_seconds, cfg).schema[_START].dataType == T.LongType()
        bounds = _bounds(cfg, int_seconds)
        assert all(bounds[cid] == (s * 1000, e * 1000) for cid, (s, e) in raw_s.items())

        relative = SolverConfig(
            channel_time_unit="ms", channel_time_origin="container_start", container_time_unit="s"
        )
        bounds = _bounds(relative, int_seconds)
        assert all(bounds[cid] == (0, (e - s) * 1000) for cid, (s, e) in raw_s.items())
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


def test_double_boundaries_keep_fractions_when_converted_to_finer_unit(
    spark, basic_narrow_db  # noqa: F811
):
    # Seconds as doubles (the fixture's ms values / 1000, so with a fractional part).
    seconds = _recast(_ms_boundaries(spark, basic_narrow_db), lambda c: c / F.lit(1000.0))
    cfg = SolverConfig(channel_time_unit="ms", container_time_unit="s")
    assert with_window_bounds(seconds, cfg).schema[_START].dataType == T.DoubleType()
    bounds = _bounds(cfg, seconds)
    for cid, (start_ms, stop_ms) in _raw_ms(spark, basic_narrow_db).items():
        assert bounds[cid] == ((start_ms / 1000.0) * 1000, (stop_ms / 1000.0) * 1000)
        # Not truncated to whole seconds before the conversion.
        assert bounds[cid][0] != (start_ms // 1000) * 1000


def test_numeric_boundaries_converted_to_coarser_channel_unit(
    spark, basic_narrow_db  # noqa: F811
):
    # Boundaries in epoch µs, channels in ms: a division, giving doubles.
    micros = _recast(_ms_boundaries(spark, basic_narrow_db), lambda c: c * F.lit(1000))
    cfg = SolverConfig(channel_time_unit="ms", container_time_unit="us")
    assert with_window_bounds(micros, cfg).schema[_START].dataType == T.DoubleType()
    bounds = _bounds(cfg, micros)
    for cid, (start_ms, stop_ms) in _raw_ms(spark, basic_narrow_db).items():
        assert bounds[cid] == (float(start_ms), float(stop_ms))


def test_numeric_boundaries_unchanged_for_equal_or_unset_container_unit(
    spark, basic_narrow_db  # noqa: F811
):
    df = _ms_boundaries(spark, basic_narrow_db)
    raw = {r.container_id: (r.start_ts, r.stop_ts) for r in df.collect()}
    assert _bounds(SolverConfig(channel_time_unit="ms", container_time_unit="ms"), df) == raw
    assert _bounds(SolverConfig(channel_time_unit="us"), df) == raw


def test_container_time_unit_rejected_for_timestamp_boundaries(
    spark, basic_narrow_db  # noqa: F811
):
    cfg = SolverConfig(channel_time_unit="s", container_time_unit="ms")
    with pytest.raises(ValueError, match="container_time_unit only applies to numeric"):
        with_window_bounds(_timestamp_boundaries(spark, basic_narrow_db), cfg)


def test_container_time_unit_requires_channel_time_unit():
    with pytest.raises(ValueError, match="container_time_unit requires channel_time_unit"):
        SolverConfig.model_validate({"container_time_unit": "ms"})
    cfg = SolverConfig.model_validate({"channel_time_unit": "us", "container_time_unit": "ms"})
    assert (cfg.channel_time_unit, cfg.container_time_unit) == ("us", "ms")


def test_raw_boundaries_stay_unchanged(spark, basic_narrow_db):  # noqa: F811
    df = _timestamp_boundaries(spark, basic_narrow_db)
    cfg = SolverConfig(channel_time_unit="s", channel_time_origin="container_start")
    out = with_window_bounds(df, cfg)
    assert isinstance(out.schema["start_ts"].dataType, T.TimestampType)
    assert out.select("container_id", "start_ts", "stop_ts").collect() == df.collect()


def test_timestamp_boundaries_without_unit_rejected(spark, basic_narrow_db):  # noqa: F811
    for origin in ("epoch", "container_start"):
        with pytest.raises(ValueError, match=r"TimeWindowEvent.*channel_time_unit"):
            with_window_bounds(
                _timestamp_boundaries(spark, basic_narrow_db),
                SolverConfig(channel_time_origin=origin),
            )


@pytest.mark.parametrize("zone_less_type", ["timestamp_ntz", "date"])
def test_zone_less_types_rejected(spark, basic_narrow_db, zone_less_type):  # noqa: F811
    df = _recast(_timestamp_boundaries(spark, basic_narrow_db), lambda c: c.cast(zone_less_type))
    with pytest.raises(ValueError, match="start_ts"):
        with_window_bounds(df, SolverConfig(channel_time_unit="s"))


def test_mixed_and_missing_boundaries_rejected(spark, basic_narrow_db):  # noqa: F811
    timestamps = _timestamp_boundaries(spark, basic_narrow_db)
    mixed = timestamps.withColumn("stop_ts", F.unix_millis("stop_ts"))
    with pytest.raises(ValueError, match="both be TIMESTAMP or both be numeric"):
        with_window_bounds(mixed, SolverConfig(channel_time_unit="s"))
    with pytest.raises(ValueError, match="stop_ts"):
        with_window_bounds(timestamps.drop("stop_ts"), SolverConfig())
    # An unmapped physical name: the error lists it and points to the column mapping.
    unmapped = timestamps.withColumnRenamed("stop_ts", "measurement_end")
    with pytest.raises(
        ValueError, match=r"'measurement_end'.*container_metrics\.column_name_mapping"
    ):
        with_window_bounds(unmapped, SolverConfig(channel_time_unit="s"))


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
