# pylint: disable=missing-function-docstring
"""
DefaultSolver coverage that is unique to this file:

- the **EAV-mode empty-selector** edge case (regression guard for a latent bug in
  the old DeltaSolver, which referenced ``selector_id`` on an empty frame). Uses
  the shared ``narrow_db`` fixture, which has a ``channel_tags`` table configured.
- EAV ``channel_tags`` selection **coexisting with ``channel_mapping`` alias
  resolution** in a single query, exercising the QueryBuilder change that feeds the
  aliased path the full tag-filtered container set rather than the direct-narrowed
  one. Uses the shared ``key_value_store_alias_with_channel_tags_db`` fixture (the
  only fixture carrying both a ``channel_tags`` and a ``channel_mapping`` table).

Out of scope here (covered elsewhere):
- EAV channel selection, its column mapping, and end-to-end EAV solves —
  ``default_solver_eav_column_mapping_test.py``.
- The wide-mode empty-selector case — ``default_solver_container_filters_test.py``.
- Alias resolution / unit conversion on the wide model —
  ``default_solver_alias_test.py`` / ``default_solver_unit_conversion_test.py``.
"""

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    SolverConfig,
    TableConfig,
)
from tests.conftest import spark  # noqa: F401  (shared session-scoped fixture)


def _alias_config() -> SolverConfig:
    """The SolverConfig the alias CSV fixtures expect (see default_solver_alias_test)."""
    return SolverConfig(
        project_id="SAMPLE_PROJECT",
        container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
        channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
    )


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
# Uses key_value_store_alias_with_channel_tags_db, whose channel_tags table mirrors
# channel_metrics.channel_name. "Engine RPM" exists only in containers 1 & 2, while
# the alias "engine_speed" (via "EngSpd" in container 3) covers all three — so the
# aliased side is strictly broader than the direct EAV match, which fails if the
# aliased path is fed the direct-narrowed frame instead of the full container set.


def test_alias_resolves_against_full_container_set(
    spark, key_value_store_alias_with_channel_tags_db
):
    """Stage-level guard: the aliased resolution covers all containers regardless of
    how narrowly the direct EAV selector matched."""
    solver = DefaultSolver(spark, config=_alias_config())
    query = key_value_store_alias_with_channel_tags_db.query
    query.select(
        query.channel(channel_name="Engine RPM"),
        query.channel_with_alias(channel_alias="engine_speed"),
    )

    direct_selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=False)
    aliased_selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=True)

    tags_df = solver.filter_container_tags(spark, query)
    metrics_df = solver.filter_container_metrics(spark, query, tags_df)

    # Direct EAV match: "Engine RPM" tagged only in containers 1 and 2.
    channel_tags_df = solver.filter_channel_tags(spark, query.db, metrics_df, direct_selectors)
    assert {row.container_id for row in channel_tags_df.collect()} == {1, 2}

    # Aliased resolution against the full container set covers 1, 2, AND 3
    # (container 3 maps via "EngSpd"). This is the QueryBuilder fix.
    aliased_df = solver.filter_aliased_channel_metrics(
        spark, query.db, metrics_df, aliased_selectors
    )
    assert {row.container_id for row in aliased_df.collect()} == {1, 2, 3}


def test_eav_direct_and_alias_coexist(spark, key_value_store_alias_with_channel_tags_db):
    """End-to-end through QueryBuilder.solve: the direct EAV selector and the alias
    coexist; the alias resolves in every container, including container 3, which the
    direct EAV selector never matched."""
    solver = DefaultSolver(spark, config=_alias_config())
    query = key_value_store_alias_with_channel_tags_db.query

    direct = query.channel(channel_name="Engine RPM").mean().alias("rpm_mean")
    aliased = query.channel_with_alias(channel_alias="engine_speed").mean().alias("eng_speed_mean")

    result = query.select(direct, aliased).solve(spark, solver=solver)
    rows = {row.container_id: row for row in result.collect()}

    assert set(rows.keys()) == {1, 2, 3}
    # Direct EAV selection matched Engine RPM in containers 1 and 2.
    assert rows[1].rpm_mean > 0
    assert rows[2].rpm_mean > 0
    # The alias resolves a real channel in all three containers — including 3, which
    # the direct selector missed. Without the QueryBuilder fix it would be absent.
    assert rows[1].eng_speed_mean > 0
    assert rows[2].eng_speed_mean > 0
    assert rows[3].eng_speed_mean > 0
