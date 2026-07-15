# pylint: disable=missing-function-docstring
"""Parity tests for the DefaultSolver value-predicate pushdown.

Every scenario runs the same query twice -- ``enable_predicate_pushdown``
off and on -- and asserts identical results, plus at least one expected
literal value so the tests cannot pass vacuously on two empty results.
"""

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.query.aggregations.point_value_aggregator import (
    PointValueAggregator,
)
from impulse_query_engine.analyze.query.aggregations.stats_aggregator import StatsAggregator
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.analyze.query.solvers.utils.predicate_pushdown import (
    ALL_ROWS,
    ValueComparison,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig


def _kvs_cfg(
    project_id: str = "SAMPLE_PROJECT",
    channel_mapping: ChannelMappingConfig | None = None,
) -> SolverConfig:
    """SolverConfig wired up for the KVS fixtures (see default_solver_test.py)."""
    return SolverConfig(
        project_id=project_id,
        container_tags=TableConfig(column_name_mapping={"element_id": "key"}),
        container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
        channel_mapping=channel_mapping or ChannelMappingConfig(),
    )


def _solve(spark, db, build_selections, pushdown, config=None, **solver_kwargs):
    """Run one query against *db* with the pushdown flag set to *pushdown*."""
    solver = DefaultSolver(
        spark,
        config=config or _kvs_cfg(),
        enable_predicate_pushdown=pushdown,
        **solver_kwargs,
    )
    query = db.query
    return query.select(*build_selections(query)).solve(spark=spark, solver=solver)


def _intervals_by_container(result_df, col: str) -> dict:
    return {
        row["container_id"]: [[float(t) for t in pair] for pair in row[col]]
        for row in result_df.collect()
    }


def _column_by_container(result_df, col: str) -> dict:
    return {row["container_id"]: row[col] for row in result_df.collect()}


def _struct_by_container(result_df, col: str) -> dict:
    return {row["container_id"]: row[col].asDict(recursive=True) for row in result_df.collect()}


# Engine RPM values in the fixture range ~800-3700; Ambient Air Temperature
# ranges 13-33.  Thresholds are chosen so every case matches a strict,
# non-empty subset of rows in every container (a container whose rows are
# all pruned would vanish -- that documented divergence has its own test).
_PARITY_CASES = {
    "gt": lambda rpm, _temp: rpm > 1300,
    "and_same_channel": lambda rpm, _temp: (rpm > 900) & (rpm < 1300),
    "or_same_channel": lambda rpm, _temp: (rpm > 1300) | (rpm < 850),
    "eq": lambda rpm, _temp: rpm == 852,  # value present in all three containers
    "merge_intervals": lambda rpm, _temp: ((rpm > 900) & (rpm < 1300)).merge_intervals(5e6),
    "debounce": lambda rpm, _temp: (rpm > 1300).debounce(5e5),
    "cross_channel_and": lambda rpm, temp: (rpm > 1300) & (temp < 20),
}


class TestPushdownParity:
    """Pushdown on/off must produce identical interval results."""

    @pytest.mark.parametrize("case", sorted(_PARITY_CASES))
    def test_parity(self, spark: SparkSession, key_value_store_db: MeasurementDB, case):
        def build(query):
            rpm = query.channel(channel_name="Engine RPM")
            temp = query.channel(channel_name="Ambient Air Temperature")
            return [_PARITY_CASES[case](rpm, temp).alias("res")]

        off = _intervals_by_container(_solve(spark, key_value_store_db, build, False), "res")
        on = _intervals_by_container(_solve(spark, key_value_store_db, build, True), "res")

        assert on == off
        assert set(off) == {1, 2, 3}
        # Guard against vacuous parity: some container must produce intervals.
        assert any(len(intervals) > 0 for intervals in off.values())

    def test_expected_intervals(self, spark: SparkSession, key_value_store_db: MeasurementDB):
        """rpm > 1300 in container 1 spans exactly two multi-sample runs."""

        def build(query):
            return [(query.channel(channel_name="Engine RPM") > 1300).alias("res")]

        expected_container_1 = [
            [1499929261121000.0, 1499929262921999.0],  # samples 1378, 1578
            [1499929290411000.0, 1499929293081999.0],  # samples 1409, 1385, 1326
        ]
        for pushdown in (False, True):
            result = _intervals_by_container(
                _solve(spark, key_value_store_db, build, pushdown), "res"
            )
            assert result[1] == expected_container_1

    def test_mixed_selection_keeps_all_rows_for_shared_channel(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """A mean() on the same channel must disable its filter entirely."""

        def build(query):
            rpm = query.channel(channel_name="Engine RPM")
            return [(rpm > 1300).alias("iv"), rpm.mean().alias("rpm_mean")]

        off = _solve(spark, key_value_store_db, build, False)
        on = _solve(spark, key_value_store_db, build, True)

        assert _intervals_by_container(on, "iv") == _intervals_by_container(off, "iv")
        off_means = _column_by_container(off, "rpm_mean")
        on_means = _column_by_container(on, "rpm_mean")
        assert on_means == pytest.approx(off_means)
        # The mean is over ALL samples (including those failing > 1300).
        assert 0 < off_means[1] < 1300

    def test_gap_between_matching_rows_is_preserved(
        self, spark: SparkSession, key_value_store_db: MeasurementDB, mock_workspace_client
    ):
        """Dropping a failing row between two passing rows must not merge them."""
        channels = spark.createDataFrame(
            [(1, 5, 0, 1, 3000.0), (1, 5, 1, 2, 1500.0), (1, 5, 2, 3, 3000.0)],
            "container_id int, channel_id int, tstart long, tend long, value double",
        )
        db = MeasurementDB(
            MeasurementDBConfig.for_debug(
                {
                    "container_tags": spark.read.table(
                        "spark_catalog.silver_key_value_store.container_tags"
                    ),
                    "container_metrics": spark.read.table(
                        "spark_catalog.silver_key_value_store.container_metrics"
                    ),
                    "channel_metrics": spark.read.table(
                        "spark_catalog.silver_key_value_store.channel_metrics"
                    ),
                    "channels": channels,
                }
            ),
            ws=mock_workspace_client,
        )

        def build(query):
            return [(query.channel(channel_name="Engine RPM") > 2000).alias("res")]

        for pushdown in (False, True):
            result = _intervals_by_container(_solve(spark, db, build, pushdown), "res")
            assert result[1] == [[0.0, 1.0], [2.0, 3.0]]

    def test_container_without_matching_rows_vanishes_with_pushdown(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """Documented divergence: fully pruned containers produce no output row."""

        def build(query):
            return [(query.channel(channel_name="Engine RPM") > 10**9).alias("res")]

        off = _intervals_by_container(_solve(spark, key_value_store_db, build, False), "res")
        on = _intervals_by_container(_solve(spark, key_value_store_db, build, True), "res")

        assert off == {1: [], 2: [], 3: []}
        assert on == {}

    def test_raw_data_mode_parity(
        self, spark: SparkSession, key_value_store_db: MeasurementDB, mock_workspace_client
    ):
        """The filter runs after tend derivation; raw point data stays consistent."""
        raw_channels = spark.read.table("spark_catalog.silver_key_value_store.channels").select(
            "container_id", "channel_id", F.col("tstart").alias("timestamp"), "value"
        )
        db = MeasurementDB(
            MeasurementDBConfig.for_debug(
                {
                    "container_tags": spark.read.table(
                        "spark_catalog.silver_key_value_store.container_tags"
                    ),
                    "container_metrics": spark.read.table(
                        "spark_catalog.silver_key_value_store.container_metrics"
                    ),
                    "channel_metrics": spark.read.table(
                        "spark_catalog.silver_key_value_store.channel_metrics"
                    ),
                    "channels": raw_channels,
                }
            ),
            ws=mock_workspace_client,
        )

        def build(query):
            return [(query.channel(channel_name="Engine RPM") > 1300).alias("res")]

        off = _intervals_by_container(_solve(spark, db, build, False, is_raw_data=True), "res")
        on = _intervals_by_container(_solve(spark, db, build, True, is_raw_data=True), "res")

        assert on == off
        assert any(len(intervals) > 0 for intervals in off.values())

    def test_alias_and_direct_selection_share_channel(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        """An aliased selector on the same physical channel keeps its rows unfiltered."""
        cfg = _kvs_cfg(
            channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
        )

        def build(query):
            rpm = query.channel(channel_name="Engine RPM")
            engine_speed = query.channel_with_alias(channel_alias="engine_speed")
            return [(rpm > 1300).alias("iv"), engine_speed.mean().alias("es_mean")]

        off = _solve(spark, key_value_store_alias_db, build, False, config=cfg)
        on = _solve(spark, key_value_store_alias_db, build, True, config=cfg)

        assert _intervals_by_container(on, "iv") == _intervals_by_container(off, "iv")
        off_means = _column_by_container(off, "es_mean")
        on_means = _column_by_container(on, "es_mean")
        assert on_means == pytest.approx(off_means)
        assert set(off_means) == {1, 2, 3}
        assert all(mean is not None and mean > 0 for mean in off_means.values())


class TestAggregationPushdownParity:
    """Event-gated aggregations must produce identical results with pushdown on."""

    def test_stats_aggregator_event_gate(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """The event channel is filtered; the input channel keeps all rows."""

        def build(query):
            rpm = query.channel(channel_name="Engine RPM")
            temp = query.channel(channel_name="Ambient Air Temperature")
            return [
                StatsAggregator(
                    input_expressions=[temp],
                    statistics=["min", "max", "mean"],
                    event_expression=rpm > 1300,
                ).alias("stats")
            ]

        off = _struct_by_container(_solve(spark, key_value_store_db, build, False), "stats")
        on = _struct_by_container(_solve(spark, key_value_store_db, build, True), "stats")

        assert on == off
        # Container 1: temp is constant 20 resp. 19 across the two rpm>1300 intervals.
        assert off[1]["event_timestamps"] == [
            [1499929261121000.0, 1499929262921999.0],
            [1499929290411000.0, 1499929293081999.0],
        ]
        assert off[1]["numeric_values"] == [
            [
                {"min": 20.0, "max": 20.0, "mean": 20.0},
                {"min": 19.0, "max": 19.0, "mean": 19.0},
            ]
        ]

    def test_point_value_aggregator_event_gate(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        def build(query):
            rpm = query.channel(channel_name="Engine RPM")
            temp = query.channel(channel_name="Ambient Air Temperature")
            return [
                PointValueAggregator(
                    input_expressions=[temp],
                    event_expression=(rpm > 1300).start_points(),
                ).alias("points")
            ]

        off = _struct_by_container(_solve(spark, key_value_store_db, build, False), "points")
        on = _struct_by_container(_solve(spark, key_value_store_db, build, True), "points")

        assert on == off
        # Container 1: temp values valid at the two rpm>1300 interval starts.
        assert off[1]["point_timestamps"] == [[1499929261121000.0, 1499929290411000.0]]
        assert off[1]["values"] == [[20.0, 19.0]]

    def test_histogram_bin_range(self, spark: SparkSession, key_value_store_db: MeasurementDB):
        """Rows outside the outer bin edges are prunable without changing the histogram."""

        def build(query):
            rpm = query.channel(channel_name="Engine RPM")
            return [rpm.histogram([800.0, 1000.0, 1400.0, 4000.0]).alias("hist")]

        off = _struct_by_container(_solve(spark, key_value_store_db, build, False), "hist")
        on = _struct_by_container(_solve(spark, key_value_store_db, build, True), "hist")

        assert on == off
        assert set(off) == {1, 2, 3}
        assert off[1]["bin_edges"] == [800.0, 1000.0, 1400.0, 4000.0]
        # The fixture contains rpm samples with value 0, so the filter prunes
        # rows while every bin still collects duration weight.
        assert all(v > 0 for v in off[1]["H"])

    def test_where_gated_histogram(self, spark: SparkSession, key_value_store_db: MeasurementDB):
        """Both the histogram channel (bin range) and the gate channel are filtered."""

        def build(query):
            rpm = query.channel(channel_name="Engine RPM")
            temp = query.channel(channel_name="Ambient Air Temperature")
            return [rpm.where(temp < 20).histogram([800.0, 4000.0]).alias("hist")]

        off = _struct_by_container(_solve(spark, key_value_store_db, build, False), "hist")
        on = _struct_by_container(_solve(spark, key_value_store_db, build, True), "hist")

        assert on == off
        assert sum(off[1]["H"]) > 0


class TestBuildPushdownFilter:
    """White-box tests for DefaultSolver._build_pushdown_filter."""

    def test_returns_none_without_selector_ids_column(self, spark: SparkSession):
        solver = DefaultSolver(spark, config=SolverConfig(), enable_predicate_pushdown=True)
        df = spark.createDataFrame([(1, 5.0)], "container_id int, value double")
        assert solver._build_pushdown_filter(df, {123: ValueComparison("gt", 1.0)}) is None

    def test_returns_none_when_no_selector_has_a_predicate(self, spark: SparkSession):
        solver = DefaultSolver(spark, config=SolverConfig(), enable_predicate_pushdown=True)
        df = spark.createDataFrame(
            [(1, [123], 5.0)], "container_id int, selector_ids array<int>, value double"
        )
        assert solver._build_pushdown_filter(df, {}) is None
        assert solver._build_pushdown_filter(df, {123: ALL_ROWS}) is None

    def test_filters_rows_only_for_predicated_selectors(self, spark: SparkSession):
        solver = DefaultSolver(spark, config=SolverConfig(), enable_predicate_pushdown=True)
        df = spark.createDataFrame(
            [(1, [10], 5.0), (2, [10], 50.0), (3, [20], 5.0), (4, [10, 20], 5.0)],
            "row_id int, selector_ids array<int>, value double",
        )
        needed = {10: ValueComparison("gt", 10.0), 20: ALL_ROWS}

        condition = solver._build_pushdown_filter(df, needed)

        kept = {row["row_id"] for row in df.where(condition).collect()}
        # Row 1 fails selector 10's predicate; rows 3 and 4 survive through
        # the unfiltered selector 20.
        assert kept == {2, 3, 4}

    def test_selector_id_exceeding_int32_is_cast_to_element_type(self, spark: SparkSession):
        solver = DefaultSolver(spark, config=SolverConfig(), enable_predicate_pushdown=True)
        big_id = 3_000_000_000  # crc32 ids are unsigned 32-bit
        df = spark.createDataFrame(
            [(1, [big_id], 5.0), (2, [big_id], 50.0)],
            "row_id int, selector_ids array<long>, value double",
        )
        needed = {big_id: ValueComparison("gt", 10.0)}

        condition = solver._build_pushdown_filter(df, needed)

        assert {row["row_id"] for row in df.where(condition).collect()} == {2}
