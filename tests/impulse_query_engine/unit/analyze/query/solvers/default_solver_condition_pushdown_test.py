# pylint: disable=missing-function-docstring
"""Condition pushdown (``channel op scalar``) in the solver: UDF-free solves and the UDF route."""

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.query.optimizer import (
    PRESENCE_VARIANT,
    RAW_VARIANT,
    PushdownContext,
    QueryOptimizer,
)
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import spark
from tests.impulse_query_engine.unit.analyze.query.solvers.default_solver_pushdown_test import (
    PUSHED_SCANS,
    RATE,
    SAMPLE_SHUFFLE,
    SMALL_CHUNKS,
    _assert_same,
    _final_plan,
    _kvs_config,
    _solve_both,
    _unit_conversion_config,
    channel_data_scans,
    shuffles,
    udf_rows,
)
import pyspark.sql.types as T

RPM_HIGH = 1500


def _rows_both(spark, db, make_selections, **solver_kwargs):
    """``solve()`` rows with pushdown off and on, sorted by container, and the solve on."""
    results = []
    for enable_pushdown in (False, True):
        solver = DefaultSolver(
            spark,
            enable_pushdown=enable_pushdown,
            pushdown_chunk_rows=SMALL_CHUNKS,
            **solver_kwargs,
        )
        q = db.query
        df = q.select(*make_selections(q)).solve(spark, solver=solver)
        results.append((sorted(df.collect(), key=lambda r: r["container_id"]), df))
    (off, _), (on, df_on) = results
    return off, on, df_on


def _assert_udf_free(df):
    plan = _final_plan(df)
    assert "FlatMapGroupsInPandas" not in plan and "ArrowEvalPython" not in plan


class TestNativeSolve:
    def test_outlier_detection_runs_without_the_udf(
        self, spark: SparkSession, key_value_store_db, udf_rows
    ):
        def selections(q):
            return [(q.channel(channel_name="Engine RPM") > RPM_HIGH).alias("rpm_high")]

        off, on, df_on = _rows_both(spark, key_value_store_db, selections, config=_kvs_config())
        assert on == off
        assert any(len(r["rpm_high"]) > 0 for r in on)
        # Only the solve with pushdown off reached the grouped-map UDF.
        assert len(udf_rows) == 1
        _assert_udf_free(df_on)
        assert channel_data_scans(df_on) == PUSHED_SCANS
        samples = [e for e in shuffles(df_on) if "_pd_chunk" in e]
        assert len(samples) == 1 and samples[0].startswith(SAMPLE_SHUFFLE)

    def test_several_conditions_and_thresholds_on_one_channel(
        self, spark: SparkSession, key_value_store_db, udf_rows
    ):
        def selections(q):
            rpm = q.channel(channel_name="Engine RPM")
            spd = q.channel(channel_name="Vehicle Speed Sensor")
            return [
                (rpm > RPM_HIGH).alias("rpm_high"),
                (rpm <= 900).alias("rpm_low"),
                (2000 < rpm).alias("rpm_very_high"),
                (spd != 0).alias("moving"),
            ]

        off, on, df_on = _rows_both(spark, key_value_store_db, selections, config=_kvs_config())
        assert on == off
        assert all(any(len(r[c]) > 0 for r in on) for c in ("rpm_high", "moving"))
        assert len(udf_rows) == 1
        _assert_udf_free(df_on)

    def test_resampled_outliers_run_without_the_udf(
        self, spark: SparkSession, key_value_store_db, udf_rows
    ):
        # Comparisons of the resampled and of the raw channel, in one pass over its samples.
        def selections(q):
            rpm = q.channel(channel_name="Engine RPM")
            return [
                (rpm.resample(RATE) > RPM_HIGH).alias("rpm_high_10s"),
                (rpm.resample(RATE) <= 900).alias("rpm_low_10s"),
                (rpm > RPM_HIGH).alias("rpm_high"),
            ]

        off, on, df_on = _rows_both(spark, key_value_store_db, selections, config=_kvs_config())
        assert on == off
        assert all(any(len(r[c]) > 0 for r in on) for c in ("rpm_high_10s", "rpm_high"))
        assert len(udf_rows) == 1
        _assert_udf_free(df_on)
        assert channel_data_scans(df_on) == PUSHED_SCANS
        samples = [e for e in shuffles(df_on) if "_pd_chunk" in e]
        assert len(samples) == 1 and samples[0].startswith(SAMPLE_SHUFFLE)

    def test_condition_that_never_holds_keeps_every_container(
        self, spark: SparkSession, key_value_store_db
    ):
        def selections(q):
            return [(q.channel(channel_name="Engine RPM") > 1e12).alias("never")]

        off, on, _ = _rows_both(spark, key_value_store_db, selections, config=_kvs_config())
        assert on == off
        assert len(on) > 0 and all(r["never"] == [] for r in on)

    def test_aliased_channel_with_unit_conversion(
        self, spark: SparkSession, key_value_store_unit_conversion_db, udf_rows
    ):
        def selections(q):
            speed = q.channel_with_alias(channel_alias="vehicle_speed")
            return [(speed > 10).alias("fast")]

        off, on, df_on = _rows_both(
            spark,
            key_value_store_unit_conversion_db,
            selections,
            config=_unit_conversion_config(),
        )
        assert on == off
        assert any(len(r["fast"]) > 0 for r in on)
        _assert_udf_free(df_on)

    def test_raw_data(self, spark: SparkSession, setup_raw_channels_db, mock_workspace_client):
        tables = {
            name: spark.read.table(f"spark_catalog.silver_raw.{name}")
            for name in ("container_metrics", "channel_metrics", "channels")
        }
        db = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=mock_workspace_client)
        off, on, df_on = _rows_both(
            spark,
            db,
            lambda q: [(q.channel(channel_name="Engine RPM") > RPM_HIGH).alias("rpm_high")],
            is_raw_data=True,
        )
        assert on == off
        assert any(len(r["rpm_high"]) > 0 for r in on)
        _assert_udf_free(df_on)

    @pytest.mark.parametrize("id_type", ["int", "string"])
    def test_id_types(self, spark: SparkSession, key_value_store_db, id_type):
        # The fixture's tables with container_id and channel_id recast.
        tables = {
            name: df.select(
                *[
                    F.col(c).cast(id_type).alias(c) if c in ("container_id", "channel_id") else c
                    for c in df.columns
                ]
            )
            for name, df in key_value_store_db.config.debug_tables.items()
        }
        db = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=key_value_store_db.ws)
        off, on, df_on = _rows_both(
            spark,
            db,
            lambda q: [(q.channel(channel_name="Engine RPM") > RPM_HIGH).alias("rpm_high")],
            config=_kvs_config(),
        )
        assert on == off
        assert any(len(r["rpm_high"]) > 0 for r in on)
        assert df_on.schema["container_id"].dataType.simpleString() == id_type
        _assert_udf_free(df_on)


class TestUdfRoute:
    def test_mixed_with_a_raw_selection(self, spark: SparkSession, key_value_store_db, udf_rows):
        def selections(q):
            rpm = q.channel(channel_name="Engine RPM")
            spd = q.channel(channel_name="Vehicle Speed Sensor")
            return [(rpm > RPM_HIGH).alias("rpm_high"), spd.max().alias("spd_max")]

        off, on, _ = _rows_both(spark, key_value_store_db, selections, config=_kvs_config())
        assert on == off
        assert any(len(r["rpm_high"]) > 0 for r in on)
        # The UDF gets the merged intervals of rpm (variant 1) and its presence rows.
        rows_on = udf_rows[1]
        assert {v for (ch, v) in rows_on if ch == 5} == {1, PRESENCE_VARIANT}
        assert {v for (ch, v) in rows_on if ch == 7} == {RAW_VARIANT}

    def test_fully_pushed_series_selection_keeps_the_udf(
        self, spark: SparkSession, key_value_store_db, udf_rows
    ):
        # Both selections are pushed down as a whole, but solve() returns a series as a
        # serialized SampleSeries, which only the UDF produces.
        def selections(q):
            rpm = q.channel(channel_name="Engine RPM")
            spd = q.channel(channel_name="Vehicle Speed Sensor")
            return [(rpm > RPM_HIGH).alias("rpm_high"), spd.resample(RATE).alias("spd_10s")]

        pushed = selections(key_value_store_db.query)
        plan = QueryOptimizer().optimize(pushed, PushdownContext(T.LongType(), T.LongType()))
        assert all(plan.variant_of(s) is not None for s in pushed)
        assert plan.native_selections(pushed) is None

        off, on = _solve_both(spark, key_value_store_db, selections, config=_kvs_config())
        _assert_same(on, off)
        assert any(len(intervals) > 0 for intervals in on["rpm_high"])
        assert all(len(series) > 1 for series in on["spd_10s"])
        # Both solves reached the UDF; with pushdown it gets derived rows only.
        assert len(udf_rows) == 2
        assert {v for (ch, v) in udf_rows[1] if ch == 5} == {1, PRESENCE_VARIANT}
        assert {v for (ch, v) in udf_rows[1] if ch == 7} == {2}

    @pytest.mark.parametrize(
        "build",
        [
            lambda rpm, _: (rpm > RPM_HIGH).debounce(2e6),
            lambda rpm, spd: spd.where(rpm > RPM_HIGH).mean(),
            lambda rpm, _: (rpm > RPM_HIGH).merge_intervals(5e6),
        ],
    )
    def test_nested_conditions(self, spark: SparkSession, key_value_store_db, build):
        def selections(q):
            rpm = q.channel(channel_name="Engine RPM")
            spd = q.channel(channel_name="Vehicle Speed Sensor")
            return [build(rpm, spd).alias("nested")]

        plan = QueryOptimizer().optimize(
            selections(key_value_store_db.query), PushdownContext(T.LongType(), T.LongType())
        )
        assert plan.describe().startswith("variants [1=condition(op=gt, threshold=1500.0")
        assert plan.native_selections(selections(key_value_store_db.query)) is None
        off, on = _solve_both(spark, key_value_store_db, selections, config=_kvs_config())
        _assert_same(on, off)

    def test_nested_resampled_comparison(self, spark: SparkSession, key_value_store_db):
        # The comparison absorbs the resample; the debounce runs in the UDF on its intervals.
        def selections(q):
            rpm = q.channel(channel_name="Engine RPM")
            return [(rpm.resample(RATE) > RPM_HIGH).debounce(2e7).alias("nested")]

        plan = QueryOptimizer().optimize(
            selections(key_value_store_db.query), PushdownContext(T.LongType(), T.LongType())
        )
        assert plan.describe().startswith(
            f"variants [1=condition(op=gt, threshold=1500.0, alias=False) absorbing "
            f"resample(sample_rate={float(RATE)})]"
        )
        off, on = _solve_both(spark, key_value_store_db, selections, config=_kvs_config())
        _assert_same(on, off)
