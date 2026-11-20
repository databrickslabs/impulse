# pylint: disable=missing-function-docstring
"""DefaultSolver channel resolution when several channels of a container match a selection.

Every selection resolves independently to one channel per container: a direct selection
to the lowest matching ``channel_id``, an alias to the best alias priority, then the
lowest ``channel_id``.  Selections matching the same channel all resolve to it.
"""

import os

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pytest

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import spark

_ALIAS_SCHEMA = "spark_catalog.silver_key_value_store_alias"
# unit="C" matches the ambient and intake temperatures: channels 6 and 9 in containers 1
# and 2, channels 2 and 8 in container 3.  The lowest ids are the ambient temperature.
_CELSIUS_CHANNELS = {1: [6], 2: [6], 3: [2]}


def _alias_config() -> SolverConfig:
    return SolverConfig(
        project_id="SAMPLE_PROJECT",
        container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
        channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
    )


def _resolved(spark, solver, query, selector) -> dict:
    """``{container_id: [channel_id, ...]}`` the filter pipeline resolves *selector* to."""
    matches = query.select(selector)._run_filter_pipeline(spark, solver, None)
    rows = matches.where(
        F.exists("selector_ids", lambda sid: sid.cast("long") == selector.selector_id)
    ).collect()
    resolved: dict = {}
    for row in rows:
        resolved.setdefault(row["container_id"], []).append(row["channel_id"])
    return {cid: sorted(channels) for cid, channels in resolved.items()}


def _channel_max(container_id, channel_id) -> float:
    """Max of one channel's samples in the fixture channel data."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    data = pd.read_csv(f"{base_path}/tests/unit/data/basic_narrow_csv/channel_data.csv")
    rows = data[(data.container_id == container_id) & (data.channel_id == channel_id)]
    return float(np.nanmax(rows.value))


def _assert_both_resolve(spark, solver, q, first, second, expected) -> None:
    """*first* and *second* both resolve to the channels of *expected* and solve on them."""
    assert first.selector_id != second.selector_id
    matches = q.select(first, second)._run_filter_pipeline(spark, solver, None)
    rows = {
        (r["container_id"], r["channel_id"]): set(r["selector_ids"]) for r in matches.collect()
    }
    assert rows == {
        (cid, ch): {first.selector_id, second.selector_id}
        for cid, channels in expected.items()
        for ch in channels
    }

    result = q.select(first.max().alias("a"), second.max().alias("b")).solve(spark, solver=solver)
    actual = {r["container_id"]: (r["a"], r["b"]) for r in result.collect()}
    assert actual.keys() == expected.keys()
    for cid, (channel,) in expected.items():
        assert actual[cid] == (pytest.approx(_channel_max(cid, channel)),) * 2


def _alias_tables(spark) -> dict:
    names = (
        "container_tags",
        "container_metrics",
        "channel_metrics",
        "channels",
        "channel_mapping",
        "channel_tags",
    )
    return {name: spark.read.table(f"{_ALIAS_SCHEMA}.{name}") for name in names}


def _alias_db(tables, ws, with_channel_tags=False) -> MeasurementDB:
    tables = dict(tables)
    if not with_channel_tags:
        tables.pop("channel_tags")
    cfg = MeasurementDBConfig.for_debug(tables)
    cfg.channel_mapping_table = "channel_mapping"
    return MeasurementDB(cfg, ws=ws)


def _with_channel(channel_metrics, container_id, channel_id, **columns):
    """*channel_metrics* plus a copy of container 1's Engine RPM row as a new channel."""
    template = channel_metrics.where(
        (F.col("container_id") == 1) & (F.col("channel_name") == "Engine RPM")
    )
    row = template.withColumn("container_id", F.lit(container_id).cast("long")).withColumn(
        "channel_id", F.lit(channel_id).cast(channel_metrics.schema["channel_id"].dataType)
    )
    for name, value in columns.items():
        row = row.withColumn(name, F.lit(value))
    return channel_metrics.unionByName(row.select(*channel_metrics.columns))


def _basic_db(basic_narrow_db, ws, **tables) -> MeasurementDB:
    """``basic_narrow_db`` with some of its tables replaced."""
    debug_tables = {**basic_narrow_db.config.debug_tables, **tables}
    return MeasurementDB(MeasurementDBConfig.for_debug(debug_tables), ws=ws)


class TestDirectSelectors:
    def test_wide_mode_keeps_the_lowest_channel_id(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        celsius = q.channel(unit="C")
        assert _resolved(spark, DefaultSolver(spark), q, celsius) == _CELSIUS_CHANNELS

    def test_wide_mode_solves_on_the_lowest_channel_id(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        result = q.select(q.channel(unit="C").max().alias("t_max")).solve(
            spark, solver=DefaultSolver(spark)
        )
        actual = {r["container_id"]: r["t_max"] for r in result.collect()}
        assert actual.keys() == _CELSIUS_CHANNELS.keys()
        for cid, (channel,) in _CELSIUS_CHANNELS.items():
            assert np.isfinite(actual[cid])
            assert actual[cid] == pytest.approx(_channel_max(cid, channel))

    def test_wide_mode_selectors_matching_the_same_channel_both_resolve(
        self, spark, basic_narrow_db
    ):
        q = basic_narrow_db.query
        rpm = q.channel(channel_name="Engine RPM")
        rpm_unit = q.channel(channel_name="Engine RPM", unit="RPM")
        _assert_both_resolve(
            spark, DefaultSolver(spark), q, rpm, rpm_unit, {1: [5], 2: [5], 3: [5]}
        )

    def test_wide_mode_ambiguous_and_unique_selector_of_one_channel(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        celsius = q.channel(unit="C")
        ambient = q.channel(channel_name="Ambient Air Temperature")
        _assert_both_resolve(spark, DefaultSolver(spark), q, celsius, ambient, _CELSIUS_CHANNELS)

    def test_eav_mode_selectors_matching_the_same_channel_both_resolve(
        self, spark, setup_key_value_store_alias_db, mock_workspace_client
    ):
        db = _alias_db(_alias_tables(spark), mock_workspace_client, with_channel_tags=True)
        q = db.query
        rpm = q.channel(channel_name="Engine RPM")
        rpm_or_speed = TimeSeriesSelector(
            (TagSelector("channel_name") == "Engine RPM")
            | (TagSelector("channel_name") == "Vehicle Speed Sensor")
        )
        solver = DefaultSolver(spark, config=_alias_config())
        _assert_both_resolve(spark, solver, q, rpm, rpm_or_speed, {1: [5], 2: [5]})

    def test_null_channel_id_is_never_resolved(
        self, spark, basic_narrow_db, mock_workspace_client
    ):
        metrics = basic_narrow_db.config.debug_tables["channel_metrics"]
        metrics = _with_channel(metrics, 1, None)
        metrics = _with_channel(metrics, 1, None, channel_name="Ghost")
        db = _basic_db(basic_narrow_db, mock_workspace_client, channel_metrics=metrics)
        q = db.query
        rpm = q.channel(channel_name="Engine RPM")
        solver = DefaultSolver(spark)
        assert _resolved(spark, solver, q, rpm) == {1: [5], 2: [5], 3: [5]}
        assert _resolved(spark, solver, q, q.channel(channel_name="Ghost")) == {}

        result = q.select(rpm.max().alias("rpm_max")).solve(spark, solver=solver)
        actual = {r["container_id"]: r["rpm_max"] for r in result.collect()}
        assert actual[1] == pytest.approx(_channel_max(1, 5))

    def test_string_channel_ids_compare_as_text(
        self, spark, basic_narrow_db, mock_workspace_client
    ):
        # Channel 9 becomes "10", which sorts before "6" as text.
        def as_text(df):
            ch = F.col("channel_id")
            return df.withColumn(
                "channel_id", F.when(ch == 9, F.lit("10")).otherwise(ch.cast("string"))
            )

        tables = basic_narrow_db.config.debug_tables
        db = _basic_db(
            basic_narrow_db,
            mock_workspace_client,
            channel_metrics=as_text(tables["channel_metrics"]),
            channels=as_text(tables["channels"]),
        )
        q = db.query
        celsius = q.channel(unit="C")
        solver = DefaultSolver(spark)
        assert _resolved(spark, solver, q, celsius) == {1: ["10"], 2: ["10"], 3: ["2"]}

        result = q.select(celsius.max().alias("t_max")).solve(spark, solver=solver)
        actual = {r["container_id"]: r["t_max"] for r in result.collect()}
        for cid in (1, 2):
            assert actual[cid] == pytest.approx(_channel_max(cid, 9))

    def test_eav_mode_keeps_the_lowest_channel_id(
        self, spark, setup_key_value_store_alias_db, mock_workspace_client
    ):
        # Re-tag channel 9 of container 1 and channels 2 and 8 of container 3 as
        # "Engine RPM", next to channel 5 of containers 1 and 2.
        tables = _alias_tables(spark)
        tags = tables["channel_tags"]
        retagged = [(1, 9), (3, 2), (3, 8)]
        keep = ~F.lit(False)
        for cid, ch in retagged:
            keep = keep & ~((F.col("container_id") == cid) & (F.col("channel_id") == ch))
        extra = spark.createDataFrame(
            [(cid, ch, "channel_name", "Engine RPM") for cid, ch in retagged],
            schema=tags.schema,
        )
        tables["channel_tags"] = tags.where(keep).unionByName(extra)
        db = _alias_db(tables, mock_workspace_client, with_channel_tags=True)

        q = db.query
        rpm = q.channel(channel_name="Engine RPM")
        assert _resolved(spark, DefaultSolver(spark, config=_alias_config()), q, rpm) == {
            1: [5],
            2: [5],
            3: [2],
        }


class TestAliasedSelectors:
    @pytest.mark.parametrize(("extra_channel", "expected"), [(2, 2), (8, 5), (None, 5)])
    def test_priority_tie_keeps_lowest_channel_id(
        self, spark, setup_key_value_store_alias_db, mock_workspace_client, extra_channel, expected
    ):
        # Container 1 gets a second engine_speed candidate (EngSpd); both candidates
        # have no priority, so the lower channel_id wins.
        tables = _alias_tables(spark)
        tables["channel_metrics"] = _with_channel(
            tables["channel_metrics"],
            1,
            extra_channel,
            channel_name="EngSpd",
            data_key="ProjSpecREC_10Hz",
        )
        db = _alias_db(tables, mock_workspace_client)
        q = db.query
        engine_speed = q.channel_with_alias(channel_alias="engine_speed")
        solver = DefaultSolver(spark, config=_alias_config())
        assert _resolved(spark, solver, q, engine_speed)[1] == [expected]

    @pytest.mark.parametrize(
        ("priorities", "expected"),
        [({}, 5), ({"engine_speed": 2, "vehicle_speed": 1}, 7)],
    )
    def test_selector_matching_several_aliases_resolves_one_channel(
        self, spark, setup_key_value_store_alias_db, mock_workspace_client, priorities, expected
    ):
        # engine_speed resolves to channel 5 and vehicle_speed to channel 7 in every
        # container; without priorities the lower channel_id wins.
        tables = _alias_tables(spark)
        mapping = tables["channel_mapping"]
        priority = F.col("priority")
        for alias, value in priorities.items():
            value = F.lit(value).cast(mapping.schema["priority"].dataType)
            priority = F.when(F.col("channel_alias") == alias, value).otherwise(priority)
        tables["channel_mapping"] = mapping.withColumn("priority", priority)
        q = _alias_db(tables, mock_workspace_client).query
        both = TimeSeriesSelector(
            (TagSelector("channel_alias") == "engine_speed")
            | (TagSelector("channel_alias") == "vehicle_speed"),
            uses_alias=True,
        )
        solver = DefaultSolver(spark, config=_alias_config())
        assert _resolved(spark, solver, q, both) == {cid: [expected] for cid in (1, 2, 3)}

    def test_selectors_matching_the_same_alias_both_resolve(self, spark, key_value_store_alias_db):
        # Both selectors match the engine_speed alias (channel 5); the second one
        # also matches vehicle_speed (channel 7), which it does not resolve to.
        q = key_value_store_alias_db.query
        engine_speed = q.channel_with_alias(channel_alias="engine_speed")
        speeds = TimeSeriesSelector(
            (TagSelector("channel_alias") == "engine_speed")
            | (TagSelector("channel_alias") == "vehicle_speed"),
            uses_alias=True,
        )
        solver = DefaultSolver(spark, config=_alias_config())
        _assert_both_resolve(spark, solver, q, engine_speed, speeds, {1: [5], 2: [5], 3: [5]})

        containers = solver.filter_container_metrics(
            spark, q, solver.filter_container_tags(spark, q)
        )
        aliases = solver.filter_aliased_channel_metrics(
            spark, key_value_store_alias_db, containers, [engine_speed, speeds]
        ).collect()
        assert sorted(
            (r["container_id"], r["channel_id"], r["channel_alias"], sorted(r["selector_ids"]))
            for r in aliases
        ) == [
            (cid, 5, "engine_speed", sorted([engine_speed.selector_id, speeds.selector_id]))
            for cid in (1, 2, 3)
        ]
