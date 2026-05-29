# pylint: disable=missing-function-docstring

import os

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
)
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    JoinKey,
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB


def _filtered_containers(spark, db: MeasurementDB, solver: KeyValueStoreSolver, query):
    tags_df = solver.filter_container_tags(spark, query)
    return solver.filter_container_metrics(spark, query, tags_df)


class TestFilterAliasedChannelMetrics:
    def test_no_aliased_selections_returns_empty(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = key_value_store_alias_db.query
        query.select(query.channel(channel_name="Engine RPM", data_key="TM"))

        container_df = _filtered_containers(spark, key_value_store_alias_db, solver, query)
        selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=True)
        result = solver.filter_aliased_channel_metrics(spark, query.db, container_df, selectors)

        assert result.columns == ["container_id", "channel_id", "selector_ids"]
        assert result.count() == 0

    def test_alias_resolves_to_correct_channels(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = key_value_store_alias_db.query
        engine_speed = query.channel_with_alias(channel_alias="engine_speed")
        query.select(engine_speed)

        container_df = _filtered_containers(spark, key_value_store_alias_db, solver, query)
        selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=True)
        result = solver.filter_aliased_channel_metrics(spark, query.db, container_df, selectors)

        rows = {
            (row.container_id, row.channel_id, row.selector_ids[0]) for row in result.collect()
        }
        assert rows == {
            (1, 5, engine_speed.selector_id),
            (2, 5, engine_speed.selector_id),
            (3, 5, engine_speed.selector_id),
        }

    def test_alias_scoped_by_project_id(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="NON_EXISTENT_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = key_value_store_alias_db.query
        query.select(query.channel_with_alias(channel_alias="engine_speed"))

        container_df = _filtered_containers(spark, key_value_store_alias_db, solver, query)
        selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=True)
        result = solver.filter_aliased_channel_metrics(spark, query.db, container_df, selectors)

        assert result.count() == 0

    def test_alias_scoped_by_toolbox_id(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(
                    filters={"toolbox_id": "non_existent_toolbox"}
                ),
            ),
        )
        query = key_value_store_alias_db.query
        query.select(query.channel_with_alias(channel_alias="engine_speed"))

        container_df = _filtered_containers(spark, key_value_store_alias_db, solver, query)
        selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=True)
        result = solver.filter_aliased_channel_metrics(spark, query.db, container_df, selectors)

        assert result.count() == 0

    def test_selector_id_consistent_for_same_expression(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = key_value_store_alias_db.query
        engine_speed = query.channel_with_alias(channel_alias="engine_speed")
        query.select(engine_speed)

        container_df = _filtered_containers(spark, key_value_store_alias_db, solver, query)
        selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=True)
        result = solver.filter_aliased_channel_metrics(spark, query.db, container_df, selectors)

        selector_ids = {row.selector_ids[0] for row in result.collect()}
        assert selector_ids == {engine_speed.selector_id}

    def test_output_columns_default_join_keys(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        """Default join keys → output carries channel_name, data_key, channel_alias, priority."""
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = key_value_store_alias_db.query
        query.select(query.channel_with_alias(channel_alias="engine_speed"))

        container_df = _filtered_containers(spark, key_value_store_alias_db, solver, query)
        selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=True)
        result = solver.filter_aliased_channel_metrics(spark, query.db, container_df, selectors)

        assert result.columns == [
            "container_id",
            "channel_id",
            "channel_name",
            "data_key",
            "channel_alias",
            "priority",
            "selector_ids",
        ]
        rows = result.collect()
        assert len(rows) > 0
        row = rows[0]
        assert row.channel_alias == "engine_speed"
        assert row.channel_name in {"Engine RPM", "EngSpd"}
        assert row.data_key in {"TM", "ProjSpecREC_10Hz"}

    def test_output_columns_single_join_key(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        """Single-column join_keys override → only that metrics column surfaces."""
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(
                    filters={"toolbox_id": "container_concept"},
                    join_keys=[
                        JoinKey(mapping_col="source_channel", metrics_col="channel_name"),
                    ],
                ),
            ),
        )
        query = key_value_store_alias_db.query
        query.select(query.channel_with_alias(channel_alias="engine_speed"))

        container_df = _filtered_containers(spark, key_value_store_alias_db, solver, query)
        selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=True)
        result = solver.filter_aliased_channel_metrics(spark, query.db, container_df, selectors)

        assert result.columns == [
            "container_id",
            "channel_id",
            "channel_name",
            "channel_alias",
            "priority",
            "selector_ids",
        ]

    def test_multiple_aliases(self, spark: SparkSession, key_value_store_alias_db: MeasurementDB):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = key_value_store_alias_db.query
        engine_speed = query.channel_with_alias(channel_alias="engine_speed")
        vehicle_speed = query.channel_with_alias(channel_alias="vehicle_speed")
        query.select(engine_speed, vehicle_speed)

        container_df = _filtered_containers(spark, key_value_store_alias_db, solver, query)
        selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=True)
        result = solver.filter_aliased_channel_metrics(spark, query.db, container_df, selectors)

        selector_ids = {row.selector_ids[0] for row in result.collect()}
        assert selector_ids == {engine_speed.selector_id, vehicle_speed.selector_id}


class TestChannelAliasEndToEnd:
    def test_solve_with_alias_only(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = key_value_store_alias_db.query
        engine_speed = query.channel_with_alias(channel_alias="engine_speed").alias("engine_speed")

        pdf = query.select(engine_speed).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        assert pdf["container_id"].tolist() == [1, 2, 3]
        assert all(length > 0 for length in pdf["engine_speed"].map(len))

    def test_solve_with_mixed_direct_and_alias(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = key_value_store_alias_db.query
        ambient_air_temp = query.channel(
            channel_name="Ambient Air Temperature", data_key="TM"
        ).alias("ambient_air_temp")
        engine_speed = query.channel_with_alias(channel_alias="engine_speed").alias("engine_speed")

        pdf = query.select(ambient_air_temp, engine_speed).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        assert pdf["container_id"].tolist() == [1, 2, 3]
        assert all(length > 0 for length in pdf["ambient_air_temp"].map(len))
        assert all(length > 0 for length in pdf["engine_speed"].map(len))

    def test_solve_deduplication(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = key_value_store_alias_db.query
        direct_engine_speed = query.channel(channel_name="Engine RPM", data_key="TM").alias(
            "direct_engine_speed"
        )
        aliased_engine_speed = query.channel_with_alias(channel_alias="engine_speed").alias(
            "aliased_engine_speed"
        )

        pdf = query.select(direct_engine_speed, aliased_engine_speed).toPandas(
            spark, solver=solver
        )
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        assert pdf["container_id"].tolist() == [1, 2, 3]
        assert [length > 0 for length in pdf["direct_engine_speed"].map(len)] == [
            True,
            True,
            False,
        ]
        assert all(length > 0 for length in pdf["aliased_engine_speed"].map(len))

    def test_alias_returns_same_channel_data_as_direct_engine_rpm(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = key_value_store_alias_db.query
        direct_engine_speed = query.channel(channel_name="Engine RPM", data_key="TM").alias(
            "direct_engine_speed"
        )
        aliased_engine_speed = query.channel_with_alias(channel_alias="engine_speed").alias(
            "aliased_engine_speed"
        )

        pdf = query.select(direct_engine_speed, aliased_engine_speed).toPandas(
            spark, solver=solver
        )
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        base_path = os.path.dirname(os.path.abspath(__file__))
        base_path = base_path[: base_path.find("tests")]

        channels_df = pd.read_csv(f"{base_path}/tests/unit/data/basic_narrow_csv/channel_data.csv")
        channel_metrics_df = pd.read_csv(
            f"{base_path}/tests/unit/data/key_value_store_alias_csv/channel_metrics.csv"
        )

        engine_rpm_channels = channel_metrics_df[
            (channel_metrics_df["channel_name"] == "Engine RPM")
            & (channel_metrics_df["data_key"] == "TM")
        ][["container_id", "channel_id"]]

        expected_by_container = {}
        for _, cm_row in engine_rpm_channels.iterrows():
            cid = int(cm_row["container_id"])
            chid = int(cm_row["channel_id"])
            data = channels_df[
                (channels_df["container_id"] == cid) & (channels_df["channel_id"] == chid)
            ].sort_values("tstart")
            expected_by_container[cid] = {
                "tstarts": data["tstart"].values.astype(np.int64),
                "tends": data["tend"].values.astype(np.int64),
                "values": data["value"].values.astype(np.float64),
            }

        assert pdf["container_id"].tolist() == [1, 2, 3]
        assert len(pdf.loc[pdf["container_id"] == 3, "direct_engine_speed"].iloc[0]) == 0
        assert len(pdf.loc[pdf["container_id"] == 3, "aliased_engine_speed"].iloc[0]) > 0

        for container_id, expected in expected_by_container.items():
            assert len(expected["tstarts"]) > 0
            assert len(expected["tstarts"]) == len(expected["tends"]) == len(expected["values"])

            row = pdf.loc[pdf["container_id"] == container_id].iloc[0]

            np.testing.assert_array_equal(row.direct_engine_speed.tstarts, expected["tstarts"])
            np.testing.assert_array_equal(row.direct_engine_speed.tends, expected["tends"])
            np.testing.assert_array_equal(row.direct_engine_speed.values, expected["values"])

            np.testing.assert_array_equal(row.aliased_engine_speed.tstarts, expected["tstarts"])
            np.testing.assert_array_equal(row.aliased_engine_speed.tends, expected["tends"])
            np.testing.assert_array_equal(row.aliased_engine_speed.values, expected["values"])

    def test_channel_with_alias_without_mapping_raises(self, key_value_store_db: MeasurementDB):
        with pytest.raises(ValueError, match="channel_mapping_table is not configured"):
            key_value_store_db.query.channel_with_alias(channel_alias="engine_speed")


class TestConfigurableJoinKeys:
    """Behavior of the configurable ``channel_mapping.join_keys`` override."""

    def test_single_column_join_key(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        # Single-column join on source_channel == channel_name only; data_key
        # is intentionally dropped from the join.  The alias resolution still
        # works and the (container_id, channel_alias) dedup keeps results
        # unique.
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_mapping=ChannelMappingConfig(
                    filters={"toolbox_id": "container_concept"},
                    join_keys=[
                        JoinKey(mapping_col="source_channel", metrics_col="channel_name"),
                    ],
                ),
            ),
        )
        query = key_value_store_alias_db.query
        engine_speed = query.channel_with_alias(channel_alias="engine_speed").alias("engine_speed")

        pdf = query.select(engine_speed).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        assert pdf["container_id"].tolist() == [1, 2, 3]
        assert all(length > 0 for length in pdf["engine_speed"].map(len))

    def test_different_data_key_names_per_side_via_rename(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        # Path 1: rename both physical `data_key` columns to a common
        # internal name (here we use a non-default name `dk`).  Two
        # JoinKey entries cover the composite key.
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_metrics=TableConfig(column_name_mapping={"data_key": "dk"}),
                channel_mapping=ChannelMappingConfig(
                    column_name_mapping={"data_key": "dk"},
                    filters={"toolbox_id": "container_concept"},
                    join_keys=[
                        JoinKey(mapping_col="source_channel", metrics_col="channel_name"),
                        JoinKey(mapping_col="dk", metrics_col="dk"),
                    ],
                ),
            ),
        )
        query = key_value_store_alias_db.query
        engine_speed = query.channel_with_alias(channel_alias="engine_speed").alias("engine_speed")

        pdf = query.select(engine_speed).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)
        assert pdf["container_id"].tolist() == [1, 2, 3]
        assert all(length > 0 for length in pdf["engine_speed"].map(len))

    def test_different_data_key_names_per_side_via_join_keys(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        # Path 2: rename the two physical `data_key` columns to *different*
        # internal names per table and reference them directly in
        # join_keys.  No common-name rename — the JoinKey's two sides are
        # independent.
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_metrics=TableConfig(column_name_mapping={"data_key": "metrics_dk"}),
                channel_mapping=ChannelMappingConfig(
                    column_name_mapping={"data_key": "map_dk"},
                    filters={"toolbox_id": "container_concept"},
                    join_keys=[
                        JoinKey(mapping_col="source_channel", metrics_col="channel_name"),
                        JoinKey(mapping_col="map_dk", metrics_col="metrics_dk"),
                    ],
                ),
            ),
        )
        query = key_value_store_alias_db.query
        engine_speed = query.channel_with_alias(channel_alias="engine_speed").alias("engine_speed")

        pdf = query.select(engine_speed).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)
        assert pdf["container_id"].tolist() == [1, 2, 3]
        assert all(length > 0 for length in pdf["engine_speed"].map(len))

    def test_tag_kwarg_must_match_post_rename_name(
        self, spark: SparkSession, key_value_store_alias_db: MeasurementDB
    ):
        # When channel_metrics.channel_name is renamed via column_name_mapping
        # to a non-default internal name, the direct selector's kwarg must use
        # the renamed name AND the override `join_keys` must reference it.
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                channel_metrics=TableConfig(column_name_mapping={"channel_name": "chan"}),
                channel_mapping=ChannelMappingConfig(
                    filters={"toolbox_id": "container_concept"},
                    join_keys=[
                        JoinKey(mapping_col="source_channel", metrics_col="chan"),
                        JoinKey(mapping_col="data_key", metrics_col="data_key"),
                    ],
                ),
            ),
        )
        query = key_value_store_alias_db.query
        # Direct selector — kwarg `chan` must match the renamed column name.
        engine_rpm = query.channel(chan="Engine RPM", data_key="TM").alias("engine_rpm")

        pdf = query.select(engine_rpm).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)
        # Direct selector — containers with no matching channel drop out.
        # Containers 1 and 2 have "Engine RPM"/data_key="TM"; container 3
        # only carries it under "EngSpd"/"ProjSpecREC_10Hz" (no match).
        assert sorted(pdf["container_id"].tolist()) == [1, 2]
        assert all(length > 0 for length in pdf["engine_rpm"].map(len))
