# pylint: disable=missing-function-docstring
"""End-to-end integration tests for the KeyValueStoreSolver.

Exercises the full 6-stage pipeline (filter_container_tags →
filter_container_metrics → filter_channel_tags → filter_channel_metrics →
solve) by calling ``QueryBuilder.solve`` with a ``KeyValueStoreSolver``
instance against the existing ``key_value_store_db`` /
``key_value_store_alias_db`` fixtures.
"""

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession

from mda_query_engine.analyze.metadata.metric_expression import MetricSelector
from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.query.aggregations.stats_aggregator import StatsAggregator
from mda_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
)
from mda_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig,
    TableConfig,
)
from mda_query_engine.measurement_db import MeasurementDB


def _kvs_cfg(
    project_id: str = "SAMPLE_PROJECT",
    container_tags: TableConfig | None = None,
    container_metrics: TableConfig | None = None,
    channel_mapping: TableConfig | None = None,
) -> SolverConfig:
    """Build a SolverConfig wired up for the KVS test data.

    The KVS fixture reuses the basic_narrow_csv ``container_metrics`` table,
    which uses ``project`` instead of ``project_id``.  The narrow EAV
    ``container_tags`` table uses ``element_id`` instead of ``key``.
    """
    return SolverConfig(
        project_id=project_id,
        container_tags=container_tags or TableConfig(column_name_mapping={"element_id": "key"}),
        container_metrics=container_metrics
        or TableConfig(column_name_mapping={"project": "project_id"}),
        channel_mapping=channel_mapping or TableConfig(),
    )


class TestKeyValueStoreSolverIntegration:
    """End-to-end pipeline tests against the key_value_store_db fixture."""

    def test_solve_no_filters_returns_all_containers(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """Without any filter the solver should emit one result row per container."""
        solver = KeyValueStoreSolver(spark, config=_kvs_cfg())
        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")

        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)

        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_solve_with_tag_expression_filter(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """A matching TagExpression keeps all containers; a non-matching one drops all."""
        solver = KeyValueStoreSolver(spark, config=_kvs_cfg())
        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")
        query.where(TagSelector("brand") == "Seat")

        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)
        assert {row.container_id for row in result.collect()} == {1, 2, 3}

        query2 = key_value_store_db.query
        query2.where(TagSelector("brand") == "VW")
        result2 = query2.select(
            query2.channel(channel_name="Engine RPM").mean().alias("rpm_mean")
        ).solve(spark=spark, solver=solver)
        assert result2.count() == 0

    def test_solve_with_metric_expression_filter(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """MetricExpression on container_metrics should narrow the solve result."""
        solver = KeyValueStoreSolver(spark, config=_kvs_cfg())
        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")
        query.where(MetricSelector("brand") == "Seat")

        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)
        assert {row.container_id for row in result.collect()} == {1, 2, 3}

        query2 = key_value_store_db.query
        query2.where(MetricSelector("brand") == "VW")
        result2 = query2.select(
            query2.channel(channel_name="Engine RPM").mean().alias("rpm_mean")
        ).solve(spark=spark, solver=solver)
        assert result2.count() == 0

    def test_solve_with_combined_tag_and_metric_filters(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """TagExpression (stage 1) + MetricExpression (stage 2) should both be applied."""
        solver = KeyValueStoreSolver(spark, config=_kvs_cfg())
        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")
        query.where(TagSelector("brand") == "Seat")
        query.where(MetricSelector("model") == "Leon")

        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)
        assert {row.container_id for row in result.collect()} == {1, 2, 3}

        # Tag matches, metric does not -> zero rows from stage 2
        query2 = key_value_store_db.query
        query2.where(TagSelector("brand") == "Seat")
        query2.where(MetricSelector("model") == "Ibiza")
        result2 = query2.select(
            query2.channel(channel_name="Engine RPM").mean().alias("rpm_mean")
        ).solve(spark=spark, solver=solver)
        assert result2.count() == 0

    def test_solve_with_event_expression_and_stats(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """Event-gated stats aggregation should produce well-formed results per container."""
        solver = KeyValueStoreSolver(spark, config=_kvs_cfg())
        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")
        veh_speed = query.channel(channel_name="Vehicle Speed Sensor")
        high_speed_event = veh_speed > 50

        stats_agg = StatsAggregator(
            input_expressions=[eng_rpm],
            statistics=["min", "max", "mean"],
            event_expression=high_speed_event,
        )

        result = query.select(stats_agg.alias("rpm_when_fast")).solve(spark=spark, solver=solver)

        assert result.count() == 3
        for row in result.collect():
            event_timestamps, numeric_values, _ = row["rpm_when_fast"]
            assert len(numeric_values) == 1
            for event_stats in numeric_values[0]:
                assert {"min", "max", "mean"}.issubset(event_stats.keys())
                if event_stats["min"] is not None:
                    assert event_stats["min"] <= event_stats["mean"] <= event_stats["max"]
            for ts in event_timestamps:
                assert ts[0] <= ts[1]

    def test_solve_non_existent_project_returns_empty(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """A project_id with no containers should yield zero solve rows."""
        solver = KeyValueStoreSolver(spark, config=_kvs_cfg("NON_EXISTENT_PROJECT"))
        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")

        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)
        assert result.count() == 0

    def test_solve_with_matching_parent_id_filter(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """``container_tags.filters`` should narrow containers via parent_id."""
        cfg = _kvs_cfg(
            container_tags=TableConfig(
                column_name_mapping={"element_id": "key"},
                filters={"parent_id": "container_concept"},
            ),
        )
        solver = KeyValueStoreSolver(spark, config=cfg)
        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")

        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)
        assert {row.container_id for row in result.collect()} == {1, 2, 3}

    def test_solve_with_non_matching_parent_id_filter(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """A parent_id that no row has should drop everything before stage 2."""
        cfg = _kvs_cfg(
            container_tags=TableConfig(
                column_name_mapping={"element_id": "key"},
                filters={"parent_id": "no_such_concept"},
            ),
        )
        solver = KeyValueStoreSolver(spark, config=cfg)
        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")

        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)
        assert result.count() == 0

    def test_solve_with_pre_filtered_containers(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """``pre_filtered_containers_df`` restricts the solve to its container set."""
        solver = KeyValueStoreSolver(spark, config=_kvs_cfg())
        query = key_value_store_db.query
        pre = query.db.container_metrics(spark).where(F.col("container_id") == 1)

        result = query.select(
            query.channel(channel_name="Engine RPM").mean().alias("rpm_mean")
        ).solve(spark=spark, solver=solver, pre_filtered_containers_df=pre)

        rows = result.collect()
        assert {row.container_id for row in rows} == {1}


class TestKeyValueStoreSolverAliasIntegration:
    """End-to-end pipeline tests against the alias-enabled KVS fixture."""

    def test_solve_with_aliased_channel(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        """Aliased channel selection should resolve via channel_mapping and produce results."""
        solver = KeyValueStoreSolver(
            spark,
            config=_kvs_cfg(
                channel_mapping=TableConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = key_value_store_alias_db.query
        engine_speed = query.channel_with_alias(channel_alias="engine_speed")

        result = query.select(engine_speed.mean().alias("engine_speed_mean")).solve(
            spark=spark, solver=solver
        )

        rows = {row.container_id: row for row in result.collect()}
        assert set(rows.keys()) == {1, 2, 3}
        for row in rows.values():
            assert row["engine_speed_mean"] is not None
