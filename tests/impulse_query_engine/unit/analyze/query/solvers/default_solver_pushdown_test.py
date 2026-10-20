# pylint: disable=missing-function-docstring
"""DefaultSolver with ``enable_pushdown``: identical results, fewer rows into the UDF."""

import re
from collections import Counter

import numpy as np
import numpy.testing as nptest
import pandas as pd
import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from impulse_query_engine.analyze.query.channels.calculated_channel import CalculatedChannel
from impulse_query_engine.analyze.query.optimizer import (
    RAW_VARIANT,
    VARIANT_COL,
    Classifier,
    PushdownPlan,
    QueryOptimizer,
    ResamplePushdownRule,
    pipeline,
)
from impulse_query_engine.analyze.query.solvers.default_solver import (
    DefaultSolver,
    TimeSeriesCache,
)
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from impulse_query_engine.model.series.intervals import Intervals
from impulse_query_engine.model.series.sample_series import SampleSeries
from tests.conftest import spark

RATE = 1e7  # 10 s; the fixture channels are sampled roughly every 0.9 s


def _kvs_config() -> SolverConfig:
    return SolverConfig(
        project_id="SAMPLE_PROJECT",
        container_tags=TableConfig(column_name_mapping={"element_id": "key"}),
        container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
        channel_mapping=ChannelMappingConfig(),
    )


def _unit_conversion_config() -> SolverConfig:
    return SolverConfig(
        project_id="SAMPLE_PROJECT",
        container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
        channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
    )


@pytest.fixture
def small_chunks(monkeypatch):
    """Run the pushdown pipeline in chunks of a few samples, so channels span many chunks."""
    monkeypatch.setattr(pipeline, "CHUNK_ROWS", 7)


def _final_plan(df) -> str:
    """The final physical plan of *df*, which must have been executed (e.g. collected)."""
    return df._jdf.queryExecution().executedPlan().toString().split("== Initial Plan ==")[0]


def channel_data_scans(df) -> list[str]:
    """Columns of each channels-table scan in the final plan of the executed *df*."""
    return sorted(
        re.sub(r"#\d+L?", "", columns)
        for columns in re.findall(r"FileScan parquet \S+\.channels\[([^\]]*)\]", _final_plan(df))
    )


def shuffles(df) -> list[str]:
    """Partitionings of the shuffles in the final plan of the executed *df* (reuses once)."""
    return [
        re.sub(r"#\d+L?", "", e)
        for e in re.findall(r"(?<=- )Exchange (\w+\([^)]*\))", _final_plan(df))
    ]


def container_shuffles_of_channel_data(df) -> int:
    """Shuffles by container alone that read the channels table, in the executed *df*."""
    lines = _final_plan(df).splitlines()

    def depth(line: str) -> int:
        return len(line) - len(line.lstrip(" :+-"))

    count = 0
    for i, line in enumerate(lines):
        if re.search(r"- Exchange hashpartitioning\(container_id#\d+L?, \d+\)", line):
            subtree = []
            for below in lines[i + 1 :]:
                if depth(below) <= depth(line):
                    break
                subtree.append(below)
            count += any(re.search(r"FileScan parquet \S+\.channels\[", b) for b in subtree)
    return count


#: Pushed channels: one full read, and one narrow read for the series boundaries.
PUSHED_SCANS = [
    "container_id,channel_id,tstart,tend",
    "container_id,channel_id,tstart,tend,value",
]
#: Channels also used raw add one read.
MIXED_SCANS = sorted([*PUSHED_SCANS, "container_id,channel_id,tstart,tend,value"])
#: The one shuffle of the pushed samples: by series and chunk.
SAMPLE_SHUFFLE = f"hashpartitioning(container_id, channel_id, {VARIANT_COL}, _pd_chunk"


@pytest.fixture
def udf_rows(monkeypatch):
    """Rows reaching the grouped-map UDF, per solve: ``{(channel_id, variant): count}``."""
    recorded: list[Counter] = []
    original = DefaultSolver._apply_grouped_map

    def _recording(self, joined_df, *args, **kwargs):
        has_variants = VARIANT_COL in joined_df.columns
        cols = ["channel_id", VARIANT_COL] if has_variants else ["channel_id"]
        counts = Counter()
        for row in joined_df.groupBy(*cols).count().collect():
            variant = row[VARIANT_COL] if has_variants else None
            counts[(row["channel_id"], variant)] += row["count"]
        recorded.append(counts)
        return original(self, joined_df, *args, **kwargs)

    monkeypatch.setattr(DefaultSolver, "_apply_grouped_map", _recording)
    return recorded


def _solve_both(spark, db, make_selections, **solver_kwargs) -> tuple[pd.DataFrame, pd.DataFrame]:
    """``toPandas`` results with pushdown off and on, sorted by container."""
    results = []
    for enable_pushdown in (False, True):
        solver = DefaultSolver(spark, enable_pushdown=enable_pushdown, **solver_kwargs)
        query = db.query
        pdf = query.select(*make_selections(query)).toPandas(spark, solver=solver)
        results.append(pdf.sort_values("container_id").reset_index(drop=True))
    return results[0], results[1]


def _assert_same(actual, expected):
    assert list(actual.columns) == list(expected.columns)
    assert len(actual) == len(expected) > 0
    for col in expected.columns:
        for a, e in zip(actual[col], expected[col], strict=True):
            if isinstance(e, SampleSeries):
                assert isinstance(a, SampleSeries)
                nptest.assert_array_equal(a.tstarts, e.tstarts)
                nptest.assert_array_equal(a.tends, e.tends)
                nptest.assert_array_equal(a.values, e.values)
            else:
                nptest.assert_equal(a, e)


def _derived_rows(pdf: pd.DataFrame, col: str) -> int:
    return sum(len(s) for s in pdf[col])


@pytest.mark.usefixtures("small_chunks")
class TestIdenticalResults:
    def test_resample_only(self, spark: SparkSession, key_value_store_db, udf_rows):
        off, on = _solve_both(
            spark,
            key_value_store_db,
            lambda q: [q.channel(channel_name="Engine RPM").resample(RATE).alias("rpm")],
            config=_kvs_config(),
        )
        _assert_same(on, off)
        assert all(np.isfinite(s.values).all() and len(s) > 1 for s in on["rpm"])

        rows_off, rows_on = udf_rows
        assert set(rows_on) == {(5, 1)}
        assert rows_on[(5, 1)] == _derived_rows(off, "rpm") < rows_off[(5, None)] / 5

    def test_pushed_channels_are_read_twice_and_shuffled_by_chunk(
        self, spark: SparkSession, key_value_store_db
    ):
        q = key_value_store_db.query
        result = q.select(q.channel(channel_name="Engine RPM").resample(RATE).alias("rpm")).solve(
            spark, solver=DefaultSolver(spark, config=_kvs_config(), enable_pushdown=True)
        )
        assert len(result.collect()) > 0  # executes result's own plan, which the checks read
        assert channel_data_scans(result) == PUSHED_SCANS
        samples = [e for e in shuffles(result) if "_pd_chunk" in e]
        assert len(samples) == 1 and samples[0].startswith(SAMPLE_SHUFFLE)
        # The grouped-map UDF still groups the shipped rows by container.
        assert container_shuffles_of_channel_data(result) == 1

    def test_channels_used_raw_add_one_read(self, spark: SparkSession, key_value_store_db):
        q = key_value_store_db.query
        result = q.select(
            q.channel(channel_name="Engine RPM").resample(RATE).alias("rpm"),
            q.channel(channel_name="Vehicle Speed Sensor").max().alias("speed_max"),
        ).solve(spark, solver=DefaultSolver(spark, config=_kvs_config(), enable_pushdown=True))
        rows = result.collect()
        assert rows and all(row["speed_max"] > 0 for row in rows)
        assert channel_data_scans(result) == MIXED_SCANS

    def test_raw_use_of_the_same_channel_ships_raw_rows_only(
        self, spark: SparkSession, key_value_store_db, udf_rows
    ):
        def selections(q):
            rpm = q.channel(channel_name="Engine RPM")
            return [rpm.resample(RATE).alias("rpm_10s"), rpm.max().alias("rpm_max")]

        off, on = _solve_both(spark, key_value_store_db, selections, config=_kvs_config())
        _assert_same(on, off)
        rows_off, rows_on = udf_rows
        # Nothing is pushed down, so the frame keeps today's shape.
        assert rows_on == rows_off

    def test_mixed_channels(self, spark: SparkSession, key_value_store_db, udf_rows):
        def selections(q):
            rpm = q.channel(channel_name="Engine RPM")
            spd = q.channel(channel_name="Vehicle Speed Sensor")
            return [
                rpm.resample(RATE).alias("rpm_10s"),
                spd.max().alias("spd_max"),
                spd.resample(RATE).alias("spd_10s"),
            ]

        off, on = _solve_both(spark, key_value_store_db, selections, config=_kvs_config())
        _assert_same(on, off)
        rows_off, rows_on = udf_rows
        assert rows_on == {
            (7, RAW_VARIANT): rows_off[(7, None)],
            (5, 1): _derived_rows(off, "rpm_10s"),
        }

    def test_two_rates_and_nested_ops(self, spark: SparkSession, key_value_store_db, udf_rows):
        def selections(q):
            rpm = q.channel(channel_name="Engine RPM")
            return [
                rpm.resample(RATE).cumtrapz().alias("rpm_integral"),
                (rpm.resample(RATE / 2) > 1500).alias("rpm_high"),
                rpm.resample(RATE / 2).mean().alias("rpm_mean"),
            ]

        off, on = _solve_both(spark, key_value_store_db, selections, config=_kvs_config())
        _assert_same(on, off)
        assert any(len(intervals) > 0 for intervals in on["rpm_high"])
        _, rows_on = udf_rows
        assert set(rows_on) == {(5, 1), (5, 2)}

    def test_aliased_selector_with_unit_conversion(
        self, spark: SparkSession, key_value_store_unit_conversion_db, udf_rows
    ):
        def selections(q):
            speed = q.channel_with_alias(channel_alias="vehicle_speed")
            return [speed.resample(RATE).alias("speed_10s")]

        off, on = _solve_both(
            spark, key_value_store_unit_conversion_db, selections, config=_unit_conversion_config()
        )
        _assert_same(on, off)
        _, rows_on = udf_rows
        assert {variant for _, variant in rows_on} == {1}

    def test_alongside_points_in_time_selection(
        self, spark: SparkSession, basic_narrow_db, udf_rows
    ):
        def selections(q):
            amb = q.channel(channel_name="Ambient Air Temperature")
            dtc_count = q.poi_channel(channel_name="DTC_count")
            rpm = q.channel(channel_name="Engine RPM")
            return [
                amb.where(dtc_count.to_points_in_time()).alias("sampled"),
                rpm.resample(RATE).alias("rpm_10s"),
            ]

        off, on = _solve_both(spark, basic_narrow_db, selections)
        _assert_same(on, off)
        _, rows_on = udf_rows
        assert rows_on[(91, RAW_VARIANT)] == 3  # POI rows keep the raw variant
        assert {variant for (ch, variant) in rows_on if ch == 5} == {1}

    def test_raw_data_with_trailing_point(
        self, spark: SparkSession, setup_raw_channels_db, mock_workspace_client
    ):
        tables = {
            name: spark.read.table(f"spark_catalog.silver_raw.{name}")
            for name in ("container_metrics", "channel_metrics", "channels")
        }
        db = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=mock_workspace_client)
        off, on = _solve_both(
            spark,
            db,
            lambda q: [q.channel(channel_name="Engine RPM").resample(2e6).alias("rpm")],
            is_raw_data=True,
        )
        _assert_same(on, off)
        rpm = on["rpm"].iloc[0]
        assert rpm.tstarts[-1] == rpm.tends[-1]  # the series ends on a point

    @pytest.mark.parametrize("channel_id_type", ["int", "string"])
    def test_channel_id_types(
        self,
        spark: SparkSession,
        setup_key_value_store_db,
        mock_workspace_client,
        udf_rows,
        channel_id_type,
    ):
        tables = {
            name: spark.read.table(f"spark_catalog.silver_key_value_store.{name}")
            for name in ("container_tags", "container_metrics", "channel_metrics", "channels")
        }
        for name in ("channel_metrics", "channels"):
            tables[name] = tables[name].withColumn(
                "channel_id", F.col("channel_id").cast(channel_id_type)
            )
        db = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=mock_workspace_client)
        off, on = _solve_both(
            spark,
            db,
            lambda q: [q.channel(channel_name="Engine RPM").resample(RATE).alias("rpm")],
            config=_kvs_config(),
        )
        _assert_same(on, off)
        assert all(np.isfinite(s.values).all() and len(s) > 1 for s in on["rpm"])
        _, rows_on = udf_rows
        assert {variant for _, variant in rows_on} == {1}


class TestCalculatedChannels:
    def test_calculated_channel_with_resample(
        self, spark: SparkSession, key_value_store_db, udf_rows
    ):
        results = []
        for enable_pushdown in (False, True):
            q = key_value_store_db.query
            rpm = q.channel(channel_name="Engine RPM")
            channel = CalculatedChannel(rpm.resample(RATE) * 2, {"channel_name": "rpm_x2"})
            solver = DefaultSolver(spark, config=_kvs_config(), enable_pushdown=enable_pushdown)
            df = q.select(channel).solve_calculated_channels(spark, solver=solver)
            results.append(
                df.drop("identity")
                .toPandas()
                .sort_values(["container_id", "tstart"])
                .reset_index(drop=True)
            )
        off, on = results
        assert len(off) > 0
        assert off["value"].gt(0).any()
        pd.testing.assert_frame_equal(on, off)
        assert set(udf_rows[1]) == {(5, 1)}


class TestTimeSeriesCacheVariants:
    """Channel 5 carries raw rows (variant 0) and ``resample(10)`` rows (variant 1)."""

    @staticmethod
    def _cache(rule, node) -> TimeSeriesCache:
        pushdown = Classifier([rule])(node)
        plan = PushdownPlan.build([rule], [pushdown], [pushdown.selector.selector_id])
        pdf = pd.DataFrame(
            {
                "container_id": [1] * 5,
                "channel_id": [5] * 5,
                VARIANT_COL: [1, 1, 0, 0, 0],
                "tstart": [0, 10, 0, 5, 10],
                "tend": [10, 20, 5, 10, 20],
                "value": [1.0, 3.0, 1.0, 2.0, 3.0],
                "conversion_factor": [2.0] * 5,
                "selector_ids": [[pushdown.selector.selector_id], None, None, None, None],
            }
        )
        col_map = {
            "cid": "container_id",
            "ch": "channel_id",
            "ts": "tstart",
            "te": "tend",
            "val": "value",
            "conv": "conversion_factor",
        }
        return TimeSeriesCache(pdf, col_map=col_map, plan=plan)

    def test_raw_and_derived_rows_are_served_separately(self):
        node = _rpm().resample(10)
        cache = self._cache(ResamplePushdownRule(), node)
        assert VARIANT_COL not in cache.mdf.columns
        nptest.assert_array_equal(cache.load_blob(1, 5).tstarts, [0, 5, 10])
        derived = node.build(cache)
        nptest.assert_array_equal(derived.tstarts, [0, 10])
        nptest.assert_array_equal(derived.values, [1.0, 3.0])

    def test_conversion_factor_applies_to_aliased_derived_rows(self):
        node = _rpm(uses_alias=True).resample(10)
        cache = self._cache(ResamplePushdownRule(), node)
        nptest.assert_array_equal(node.build(cache).values, [2.0, 6.0])

    def test_falls_back_without_derived_rows(self):
        rpm = _rpm()
        cache = self._cache(ResamplePushdownRule(), rpm.resample(10))
        other_rate = rpm.resample(5)  # not part of the plan
        assert cache.materialized(other_rate) is None
        nptest.assert_array_equal(other_rate.build(cache).tstarts, [0, 5, 10, 15])

    def test_rule_decides_the_result_type(self):
        class _IntervalsRule(ResamplePushdownRule):
            def decode(self, rows, pushdown, cache):
                return Intervals(
                    rows[cache.tstart_col].to_numpy(), rows[cache.tend_col].to_numpy()
                )

        node = _rpm().resample(10)
        result = self._cache(_IntervalsRule(), node).materialized(node)
        assert isinstance(result, Intervals)
        nptest.assert_array_equal(result.tstarts, [0, 10])

    def test_each_node_is_classified_once_per_container(self):
        matched = []

        class _Recording(ResamplePushdownRule):
            def match(self, node, operand):
                matched.append(node)
                return super().match(node, operand)

        resampled = _rpm().resample(10)
        cache = self._cache(_Recording(), resampled)
        matched.clear()
        integral = resampled.cumtrapz().build(cache)
        # The root and the served resample, each matched once across both lookups.
        assert len(matched) == 2 and len({id(node) for node in matched}) == 2
        nptest.assert_array_equal(integral.tstarts, [0, 10])

    def test_optimizer_default_rules(self):
        assert [type(r) for r in QueryOptimizer().rules] == [ResamplePushdownRule]


def _rpm(uses_alias: bool = False) -> TimeSeriesSelector:
    return TimeSeriesSelector(TagSelector("channel_name") == "rpm", uses_alias=uses_alias)
