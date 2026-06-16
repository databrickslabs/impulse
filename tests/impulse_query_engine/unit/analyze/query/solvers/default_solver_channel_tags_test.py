# pylint: disable=missing-function-docstring
"""
Tests for DefaultSolver's EAV ``channel_tags`` channel-selection branch and its
interaction with the wide features inherited from the former KeyValueStoreSolver
(container-tag filtering and channel-alias resolution).

The per-table column-mapping behaviour of the EAV branch is covered in
``default_solver_eav_column_mapping_test.py`` (which now runs DefaultSolver).  This
file focuses on:

- the empty-selector edge case in EAV mode (regression guard for a latent bug
  in the old DeltaSolver, which referenced ``selector_id`` on an empty frame);
- a container_tags + channel_tags combined query with default names;
- EAV channel_tags selection coexisting with channel_mapping alias resolution in
  a single query, which exercises the QueryBuilder change that feeds the aliased
  path the full tag-filtered container set rather than the direct-narrowed one.
"""

from unittest.mock import create_autospec

import pandas as pd
import pyspark.sql.types as T
import pytest
from databricks.sdk import WorkspaceClient

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import spark  # noqa: F401  (shared session-scoped fixture)

# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _container_tags_df(spark, rows):
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("key", T.StringType()),
            T.StructField("value", T.StringType()),
        ]
    )
    return spark.createDataFrame(
        pd.DataFrame(rows, columns=["container_id", "key", "value"]), schema
    )


def _container_metrics_df(spark, rows):
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("start_ts", T.LongType()),
            T.StructField("stop_ts", T.LongType()),
        ]
    )
    return spark.createDataFrame(
        pd.DataFrame(rows, columns=["container_id", "start_ts", "stop_ts"]), schema
    )


def _channel_tags_df(spark, rows):
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("channel_id", T.IntegerType()),
            T.StructField("key", T.StringType()),
            T.StructField("value", T.StringType()),
        ]
    )
    return spark.createDataFrame(
        pd.DataFrame(rows, columns=["container_id", "channel_id", "key", "value"]), schema
    )


def _channel_metrics_df(spark, rows, columns):
    type_by_col = {
        "container_id": T.LongType(),
        "channel_id": T.IntegerType(),
        "sample_count": T.IntegerType(),
        "channel_name": T.StringType(),
        "data_key": T.StringType(),
    }
    schema = T.StructType([T.StructField(c, type_by_col[c]) for c in columns])
    return spark.createDataFrame(pd.DataFrame(rows, columns=columns), schema)


def _channels_df(spark, rows):
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("channel_id", T.IntegerType()),
            T.StructField("tstart", T.LongType()),
            T.StructField("tend", T.LongType()),
            T.StructField("value", T.DoubleType()),
        ]
    )
    return spark.createDataFrame(
        pd.DataFrame(rows, columns=["container_id", "channel_id", "tstart", "tend", "value"]),
        schema,
    )


def _channel_mapping_df(spark, rows):
    schema = T.StructType(
        [
            T.StructField("channel_alias", T.StringType()),
            T.StructField("source_channel", T.StringType()),
            T.StructField("data_key", T.StringType()),
            T.StructField("priority", T.IntegerType()),
        ]
    )
    return spark.createDataFrame(
        pd.DataFrame(rows, columns=["channel_alias", "source_channel", "data_key", "priority"]),
        schema,
    )


def _make_db(tables: dict) -> MeasurementDB:
    cfg = MeasurementDBConfig.for_debug(tables)
    return MeasurementDB(cfg, ws=create_autospec(WorkspaceClient))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def eav_db(spark):
    """EAV channel_tags + container_tags; three containers each with Engine RPM."""
    return _make_db(
        {
            "container_tags": _container_tags_df(
                spark,
                [
                    (1, "model", "Leon"),
                    (2, "model", "Ibiza"),
                    (3, "model", "Ateca"),
                ],
            ),
            "container_metrics": _container_metrics_df(
                spark, [(1, 1000, 3000), (2, 1000, 3000), (3, 1000, 3000)]
            ),
            "channel_tags": _channel_tags_df(
                spark,
                [
                    (1, 1, "channel_name", "Engine RPM"),
                    (2, 1, "channel_name", "Engine RPM"),
                    (3, 1, "channel_name", "Engine RPM"),
                ],
            ),
            "channel_metrics": _channel_metrics_df(
                spark,
                [(1, 1, 100), (2, 1, 100), (3, 1, 100)],
                columns=["container_id", "channel_id", "sample_count"],
            ),
            "channels": _channels_df(
                spark,
                [
                    (1, 1, 1000, 2000, 1500.0),
                    (1, 1, 2000, 3000, 1600.0),
                    (2, 1, 1000, 2000, 1400.0),
                    (3, 1, 1000, 2000, 1800.0),
                ],
            ),
        }
    )


# ---------------------------------------------------------------------------
# Empty-selector edge case (regression guard)
# ---------------------------------------------------------------------------


class TestEmptySelectorsEAV:
    def _container_df(self, spark, solver, db):
        query = db.query
        tags_df = solver.filter_container_tags(spark, query)
        return solver.filter_container_metrics(spark, query, tags_df)

    def test_filter_channel_tags_empty_selectors_returns_empty(self, spark, eav_db):
        solver = DefaultSolver(spark)
        container_df = self._container_df(spark, solver, eav_db)
        result = solver.filter_channel_tags(spark, eav_db, container_df, [])
        assert {"container_id", "channel_id", "selector_ids"}.issubset(set(result.columns))
        assert result.count() == 0

    def test_filter_channel_metrics_empty_selectors_returns_empty(self, spark, eav_db):
        solver = DefaultSolver(spark)
        container_df = self._container_df(spark, solver, eav_db)
        # In EAV mode the upstream stage would also be empty for no selectors.
        channel_df = solver.filter_channel_tags(spark, eav_db, container_df, [])
        result = solver.filter_channel_metrics(spark, eav_db, channel_df, [])
        assert {"container_id", "channel_id", "selector_ids"}.issubset(set(result.columns))
        assert result.count() == 0


# ---------------------------------------------------------------------------
# container_tags + channel_tags combined (default names, end-to-end)
# ---------------------------------------------------------------------------


def test_container_tag_and_channel_tag_filter_combined(spark, eav_db):
    solver = DefaultSolver(spark)
    query = eav_db.query
    query.where(TagSelector("model") == "Ateca")  # narrows to container 3
    eng_rpm = query.channel(channel_name="Engine RPM")  # EAV channel selection
    result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark, solver=solver)

    rows = {row.container_id: row.rpm_mean for row in result.collect()}
    assert set(rows.keys()) == {3}
    assert rows[3] == pytest.approx(1800.0)


def test_channel_tags_eav_end_to_end_mean(spark, eav_db):
    solver = DefaultSolver(spark)
    query = eav_db.query
    eng_rpm = query.channel(channel_name="Engine RPM")
    result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark, solver=solver)

    rows = {row.container_id: row.rpm_mean for row in result.collect()}
    assert set(rows.keys()) == {1, 2, 3}
    assert rows[1] == pytest.approx(1550.0)  # mean(1500, 1600) over equal intervals
    assert rows[2] == pytest.approx(1400.0)
    assert rows[3] == pytest.approx(1800.0)


# ---------------------------------------------------------------------------
# EAV channel_tags + channel_mapping alias coexistence (QueryBuilder fix guard)
# ---------------------------------------------------------------------------


@pytest.fixture
def eav_plus_alias_db(spark):
    """Both an EAV channel_tags table (for direct selection) and a channel_mapping
    table (for alias resolution).  channel_metrics carries channel_name / data_key
    so the default alias join keys resolve against it.

    "Engine RPM" exists (in channel_tags) only in container 1, while the alias
    target "Spd" exists in all three containers.  This makes the aliased side
    strictly broader than the direct EAV match, so it fails if the aliased path
    is fed the direct-narrowed frame instead of the full container set.
    """
    return _make_db(
        {
            "container_metrics": _container_metrics_df(
                spark, [(1, 1000, 3000), (2, 1000, 3000), (3, 1000, 3000)]
            ),
            "channel_tags": _channel_tags_df(
                spark,
                [
                    (1, 1, "channel_name", "Engine RPM"),
                    (1, 5, "channel_name", "Spd"),
                    (2, 5, "channel_name", "Spd"),
                    (3, 5, "channel_name", "Spd"),
                ],
            ),
            "channel_metrics": _channel_metrics_df(
                spark,
                [
                    (1, 1, "Engine RPM", "TM", 100),
                    (1, 5, "Spd", "TM", 100),
                    (2, 5, "Spd", "TM", 100),
                    (3, 5, "Spd", "TM", 100),
                ],
                columns=["container_id", "channel_id", "channel_name", "data_key", "sample_count"],
            ),
            "channels": _channels_df(
                spark,
                [
                    (1, 1, 1000, 2000, 1500.0),
                    (1, 5, 1000, 2000, 30.0),
                    (2, 5, 1000, 2000, 40.0),
                    (3, 5, 1000, 2000, 50.0),
                ],
            ),
            "channel_mapping": _channel_mapping_df(spark, [("vehicle_speed", "Spd", "TM", 1)]),
        }
    )


def test_eav_direct_and_alias_coexist(spark, eav_plus_alias_db):
    solver = DefaultSolver(spark)
    query = eav_plus_alias_db.query

    direct = query.channel(channel_name="Engine RPM").mean().alias("rpm_mean")
    aliased = query.channel_with_alias(channel_alias="vehicle_speed").mean().alias("spd_mean")

    result = query.select(direct, aliased).solve(spark, solver=solver)
    rows = {row.container_id: row for row in result.collect()}

    # Direct EAV selection matches Engine RPM only in container 1.
    assert rows[1].rpm_mean == pytest.approx(1500.0)
    # The alias resolves in all three containers — including 2 and 3, which the
    # direct EAV selector did NOT match. This is the QueryBuilder fix: the
    # aliased path runs against the full tag-filtered container set.
    assert rows[1].spd_mean == pytest.approx(30.0)
    assert rows[2].spd_mean == pytest.approx(40.0)
    assert rows[3].spd_mean == pytest.approx(50.0)


def test_alias_resolves_against_full_container_set(spark, eav_plus_alias_db):
    """Stage-level guard: the aliased resolution covers all containers regardless
    of how narrowly the direct EAV selector matched."""
    solver = DefaultSolver(spark)
    query = eav_plus_alias_db.query
    query.select(
        query.channel(channel_name="Engine RPM"),
        query.channel_with_alias(channel_alias="vehicle_speed"),
    )

    direct_selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=False)
    aliased_selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=True)

    tags_df = solver.filter_container_tags(spark, query)
    metrics_df = solver.filter_container_metrics(spark, query, tags_df)

    channel_tags_df = solver.filter_channel_tags(spark, query.db, metrics_df, direct_selectors)
    # Direct EAV match is narrowed to container 1 only.
    assert {row.container_id for row in channel_tags_df.collect()} == {1}

    # Aliased resolution against the full container set still covers 1, 2, 3.
    aliased_df = solver.filter_aliased_channel_metrics(
        spark, query.db, metrics_df, aliased_selectors
    )
    assert {row.container_id for row in aliased_df.collect()} == {1, 2, 3}
