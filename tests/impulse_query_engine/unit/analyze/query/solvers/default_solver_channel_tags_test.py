# pylint: disable=missing-function-docstring
"""
DefaultSolver coverage that is unique to this file:

- the **EAV-mode empty-selector** edge case (regression guard for a latent bug in
  the old DeltaSolver, which referenced ``selector_id`` on an empty frame). Uses
  the shared ``narrow_db`` fixture, which has a ``channel_tags`` table configured.
- EAV ``channel_tags`` selection **coexisting with ``channel_mapping`` alias
  resolution** in a single query, exercising the QueryBuilder change that feeds the
  aliased path the full tag-filtered container set rather than the direct-narrowed
  one. No shared fixture carries both tables, so a small one is built here.

Out of scope here (covered elsewhere):
- EAV channel selection, its column mapping, and end-to-end EAV solves —
  ``default_solver_eav_column_mapping_test.py``.
- The wide-mode empty-selector case — ``default_solver_container_filters_test.py``.
"""

from unittest.mock import create_autospec

import pandas as pd
import pyspark.sql.types as T
import pytest
from databricks.sdk import WorkspaceClient

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import spark  # noqa: F401  (shared session-scoped fixture)

# ---------------------------------------------------------------------------
# Empty-selector edge case in EAV mode (regression guard) — reuses narrow_db
# ---------------------------------------------------------------------------


class TestEmptySelectorsEAV:
    """With a ``channel_tags`` table configured, the channel stages must return an
    empty ``(container_id, channel_id, selector_ids)`` frame when there are no
    direct selectors — without raising. The old DeltaSolver referenced a
    non-existent ``selector_id`` column on the empty frame (analysis-time error)."""

    def _container_df(self, spark, solver, db):
        query = db.query
        tags_df = solver.filter_container_tags(spark, query)
        return solver.filter_container_metrics(spark, query, tags_df)

    def test_filter_channel_tags_empty_selectors_returns_empty(self, spark, narrow_db):
        solver = DefaultSolver(spark)
        container_df = self._container_df(spark, solver, narrow_db)
        result = solver.filter_channel_tags(spark, narrow_db, container_df, [])
        assert {"container_id", "channel_id", "selector_ids"}.issubset(set(result.columns))
        assert result.count() == 0

    def test_filter_channel_metrics_empty_selectors_returns_empty(self, spark, narrow_db):
        solver = DefaultSolver(spark)
        container_df = self._container_df(spark, solver, narrow_db)
        channel_df = solver.filter_channel_tags(spark, narrow_db, container_df, [])
        result = solver.filter_channel_metrics(spark, narrow_db, channel_df, [])
        assert {"container_id", "channel_id", "selector_ids"}.issubset(set(result.columns))
        assert result.count() == 0


# ---------------------------------------------------------------------------
# EAV channel_tags + channel_mapping alias coexistence (QueryBuilder fix guard)
# ---------------------------------------------------------------------------
# No shared fixture carries both a channel_tags table (EAV direct selection) and a
# channel_mapping table (alias resolution), so this small dataset is built locally.


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


def _channel_metrics_df(spark, rows):
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("channel_id", T.IntegerType()),
            T.StructField("channel_name", T.StringType()),
            T.StructField("data_key", T.StringType()),
            T.StructField("sample_count", T.IntegerType()),
        ]
    )
    return spark.createDataFrame(
        pd.DataFrame(
            rows,
            columns=["container_id", "channel_id", "channel_name", "data_key", "sample_count"],
        ),
        schema,
    )


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


@pytest.fixture
def eav_plus_alias_db(spark):
    """A DB with both an EAV ``channel_tags`` table (direct selection) and a
    ``channel_mapping`` table (alias resolution); ``channel_metrics`` carries
    ``channel_name`` / ``data_key`` so the default alias join keys resolve.

    "Engine RPM" exists (in channel_tags) only in container 1, while the alias
    target "Spd" exists in all three containers — so the aliased side is strictly
    broader than the direct EAV match, and fails if the aliased path is fed the
    direct-narrowed frame instead of the full container set.
    """
    cfg = MeasurementDBConfig.for_debug(
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
    return MeasurementDB(cfg, ws=create_autospec(WorkspaceClient))


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
