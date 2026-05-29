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
from impulse_reporting.config.config_parser import ImpulseConfig
from impulse_reporting.meta.container_dimensions import ChannelMappingResolutionDimension


def _impulse_config() -> ImpulseConfig:
    return ImpulseConfig.model_validate(
        {
            "source": {
                "container_metrics_table": "c.s.container_metrics",
                "channel_metrics_table": "c.s.channel_metrics",
                "channels_uri": "c.s.channels",
                "channel_mapping_table": "c.s.channel_mapping",
            },
        }
    )


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
        config=_impulse_config(),
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
        config=_impulse_config(),
        aliased_selectors=[aliased],
    )

    assert result is not None
    # selector_ids must be dropped; config_hash must be appended.
    assert result.columns == [
        "container_id",
        "channel_id",
        "channel_name",
        "data_key",
        "channel_alias",
        "priority",
        "config_hash",
    ]
    rows = result.collect()
    assert len(rows) > 0
    aliases = {row.channel_alias for row in rows}
    assert aliases == {"engine_speed"}
    # Every row resolved to a known physical channel for engine_speed.
    for row in rows:
        assert row.channel_name in {"Engine RPM", "EngSpd"}
        assert row.data_key in {"TM", "ProjSpecREC_10Hz"}
        assert row.config_hash is not None


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
        config=_impulse_config(),
        aliased_selectors=[aliased],
        pre_filtered_containers_df=pre_filtered,
    )

    container_ids = {row.container_id for row in result.collect()}
    assert container_ids == {1}
