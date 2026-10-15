# pylint: disable=missing-function-docstring
"""DefaultSolver resolves every selector to at most one channel per container.

When several channels of a container match a selection, the lowest ``channel_id``
wins (after the alias priority for aliased selections), independent of row order.
"""

import os
import zlib

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pytest

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import (
    SeriesType,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import spark

_ALIAS_SCHEMA = "spark_catalog.silver_key_value_store_alias"


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


class TestDirectSelectors:
    def test_wide_mode_keeps_lowest_channel_id(self, spark, basic_narrow_db):
        # unit="C" matches the ambient and intake temperatures: channels 6 and 9 in
        # containers 1 and 2, channels 2 and 8 in container 3.
        q = basic_narrow_db.query
        celsius = q.channel(unit="C")
        assert _resolved(spark, DefaultSolver(spark), q, celsius) == {1: [6], 2: [6], 3: [2]}

    def test_wide_mode_solves_on_the_resolved_channel(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        result = q.select(q.channel(unit="C").max().alias("t_max")).solve(
            spark, solver=DefaultSolver(spark)
        )
        actual = {r["container_id"]: r["t_max"] for r in result.collect()}

        base_path = os.path.dirname(os.path.abspath(__file__))
        base_path = base_path[: base_path.find("tests")]
        data = pd.read_csv(f"{base_path}/tests/unit/data/basic_narrow_csv/channel_data.csv")
        expected = {
            cid: np.nanmax(data[(data.container_id == cid) & (data.channel_id == ch)].value)
            for cid, ch in {1: 6, 2: 6, 3: 2}.items()
        }
        assert actual == pytest.approx(expected)
        assert all(np.isfinite(v) for v in actual.values())

    def test_eav_mode_keeps_lowest_channel_id(
        self, spark, setup_key_value_store_alias_db, mock_workspace_client
    ):
        # Re-tag channel 9 of container 1 and channels 2 and 8 of container 3 as
        # "Engine RPM": container 1 keeps 5 (the original), container 3 keeps 2.
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
    @pytest.mark.parametrize(("extra_channel", "expected"), [(2, 2), (8, 5)])
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

    def test_selector_matching_several_aliases_resolves_one_channel(
        self, spark, key_value_store_alias_db
    ):
        q = key_value_store_alias_db.query
        solver = DefaultSolver(spark, config=_alias_config())
        per_alias = [
            _resolved(spark, solver, q, q.channel_with_alias(channel_alias=alias))
            for alias in ("engine_speed", "vehicle_speed")
        ]
        both = TimeSeriesSelector(
            (TagSelector("channel_alias") == "engine_speed")
            | (TagSelector("channel_alias") == "vehicle_speed"),
            uses_alias=True,
        )
        resolved = _resolved(spark, solver, q, both)
        assert resolved
        for cid, channels in resolved.items():
            candidates = [ch for one in per_alias for ch in one.get(cid, [])]
            assert channels == [min(candidates)]


def test_aliased_and_direct_selectors_on_the_same_tags_have_distinct_ids():
    expr = TagSelector("channel_name") == "Engine RPM"
    direct = TimeSeriesSelector(expr)
    aliased = TimeSeriesSelector(expr, uses_alias=True)
    assert direct.selector_id == zlib.crc32(str(expr).encode())
    assert aliased.selector_id != direct.selector_id


def test_aliased_and_direct_poi_selectors_on_the_same_tags_have_distinct_ids():
    expr = TagSelector("channel_name") == "Fault Code"
    direct = TimeSeriesSelector(expr, series_type=SeriesType.POINTS_IN_TIME)
    aliased = TimeSeriesSelector(expr, uses_alias=True, series_type=SeriesType.POINTS_IN_TIME)
    expected_direct = f"{SeriesType.POINTS_IN_TIME}|{expr}|{direct._value_type}"
    assert direct.selector_id == zlib.crc32(expected_direct.encode())
    assert aliased.selector_id != direct.selector_id
