# pylint: disable=missing-function-docstring

from pyspark.sql import SparkSession

from impulse_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
)
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB
from impulse_reporting.meta.container_dimensions import ChannelMappingResolutionDimension


def _kvs_solver(spark: SparkSession) -> KeyValueStoreSolver:
    return KeyValueStoreSolver(
        spark,
        config=SolverConfig(
            project_id="SAMPLE_PROJECT",
            container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
            channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
        ),
    )


def test_returns_none_when_no_aliased_selectors(
    spark: SparkSession, key_value_store_alias_db: MeasurementDB
):
    solver = _kvs_solver(spark)
    query = key_value_store_alias_db.query

    result = ChannelMappingResolutionDimension.get_dimension(
        spark=spark,
        query=query,
        solver=solver,
        aliased_selectors=[],
    )

    assert result is None


def test_returns_resolution_with_expected_schema(
    spark: SparkSession, key_value_store_alias_db: MeasurementDB
):
    solver = _kvs_solver(spark)
    query = key_value_store_alias_db.query
    aliased = query.channel_with_alias(channel_alias="engine_speed")

    result = ChannelMappingResolutionDimension.get_dimension(
        spark=spark,
        query=query,
        solver=solver,
        aliased_selectors=[aliased],
    )

    assert result is not None
    # selector_ids is dropped; no config_hash on this dimension.
    assert result.columns == [
        "container_id",
        "channel_id",
        "channel_name",
        "data_key",
        "channel_alias",
        "priority",
    ]
    rows = result.collect()
    assert len(rows) > 0
    aliases = {row.channel_alias for row in rows}
    assert aliases == {"engine_speed"}
    # Every row resolved to a known physical channel for engine_speed.
    for row in rows:
        assert row.channel_name in {"Engine RPM", "EngSpd"}
        assert row.data_key in {"TM", "ProjSpecREC_10Hz"}


def test_dimension_honors_pre_filtered_containers(
    spark: SparkSession, key_value_store_alias_db: MeasurementDB
):
    """When pre_filtered_containers_df is supplied, the result is restricted to those containers."""
    import pyspark.sql.functions as F

    solver = _kvs_solver(spark)
    query = key_value_store_alias_db.query
    aliased = query.channel_with_alias(channel_alias="engine_speed")

    # Pre-filtered containers must carry the same columns as silver
    # container_metrics so the solver's downstream project_id filter still
    # applies (matches the contract used by the incremental container
    # detector in production).
    pre_filtered = key_value_store_alias_db.container_metrics(spark).where(
        F.col("container_id") == 1
    )

    result = ChannelMappingResolutionDimension.get_dimension(
        spark=spark,
        query=query,
        solver=solver,
        aliased_selectors=[aliased],
        pre_filtered_containers_df=pre_filtered,
    )

    container_ids = {row.container_id for row in result.collect()}
    assert container_ids == {1}


def test_scopes_returns_none_when_both_empty(
    spark: SparkSession, key_value_store_alias_db: MeasurementDB
):
    solver = _kvs_solver(spark)
    query = key_value_store_alias_db.query

    result = ChannelMappingResolutionDimension.get_dimension_for_scopes(
        spark=spark,
        query=query,
        solver=solver,
        changed_aliased_selectors=[],
        unchanged_aliased_selectors=[],
    )

    assert result is None


def test_scopes_changed_alias_covers_all_containers_unchanged_alias_scoped(
    spark: SparkSession, key_value_store_alias_db: MeasurementDB
):
    """The fact-split contract: a changed alias resolves over ALL containers
    even under a pre_filter, while an alias only in unchanged definitions
    stays scoped to pre_filtered_containers_df."""
    import pyspark.sql.functions as F

    solver = _kvs_solver(spark)
    query = key_value_store_alias_db.query
    changed = query.channel_with_alias(channel_alias="engine_speed")
    unchanged = query.channel_with_alias(channel_alias="vehicle_speed")

    pre_filtered = key_value_store_alias_db.container_metrics(spark).where(
        F.col("container_id") == 1
    )

    result = ChannelMappingResolutionDimension.get_dimension_for_scopes(
        spark=spark,
        query=query,
        solver=solver,
        changed_aliased_selectors=[changed],
        unchanged_aliased_selectors=[unchanged],
        pre_filtered_containers_df=pre_filtered,
    )

    rows = result.collect()
    changed_containers = {r.container_id for r in rows if r.channel_alias == "engine_speed"}
    unchanged_containers = {r.container_id for r in rows if r.channel_alias == "vehicle_speed"}

    # Changed alias: full coverage regardless of the pre_filter.
    assert changed_containers == {1, 2, 3}
    # Unchanged-only alias: restricted to the pre-filtered container.
    assert unchanged_containers == {1}


def test_scopes_alias_in_both_lists_resolved_once(
    spark: SparkSession, key_value_store_alias_db: MeasurementDB
):
    """An alias present in both the changed and unchanged sets is resolved
    once (via the changed/full scope), so no duplicate
    (container_id, channel_alias) rows reach the downstream MERGE."""
    solver = _kvs_solver(spark)
    query = key_value_store_alias_db.query
    engine = query.channel_with_alias(channel_alias="engine_speed")
    vehicle = query.channel_with_alias(channel_alias="vehicle_speed")

    # engine_speed appears in both lists; vehicle_speed only in unchanged.
    result = ChannelMappingResolutionDimension.get_dimension_for_scopes(
        spark=spark,
        query=query,
        solver=solver,
        changed_aliased_selectors=[engine],
        unchanged_aliased_selectors=[engine, vehicle],
    )

    rows = result.collect()
    keys = [(r.container_id, r.channel_alias) for r in rows]
    # No duplicates across the two scopes.
    assert len(keys) == len(set(keys))
    # engine_speed resolved over all containers exactly once each.
    engine_containers = sorted(r.container_id for r in rows if r.channel_alias == "engine_speed")
    assert engine_containers == [1, 2, 3]
