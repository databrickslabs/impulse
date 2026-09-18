# pylint: disable=missing-function-docstring
"""End-to-end tests for ``SolverConfig.channels.filters``.

The channels table has no standalone ``filter_*`` stage — its per-table
equality filters are applied inside ``DefaultSolver._prepare_channels_join``
when the channel-data table is read (after ``column_name_mapping``, before the
UDF-column projection).  These tests exercise the filter through a real
``solve_calculated_channels`` run against the wide-only ``basic_narrow_db``
fixture and assert on the resulting computed values, not just row counts.

Covers:
- A string filter that removes rows: only the retained sample survives, and it
  carries the correct scaled value.
- No filter configured: behaviour is unchanged (full data comes through).
- A non-matching filter: zero result rows.
- A boolean channels column: string ``"true"`` matches the ``true`` rows via
  Spark's literal coercion (the shared ``F.col(col) == value`` mechanism).
"""

import pyspark.sql.functions as F
import pytest

from impulse_query_engine.analyze.query.channels.calculated_channel import (
    CalculatedChannel,
)
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import basic_narrow_db, spark  # noqa: F401  (pytest fixtures)

# Known datum from tests/unit/data/basic_narrow_csv/channel_data.csv:
# container 1, channel 5 (Engine RPM), second RLE row.
_C1_RPM_TSTART = 1499929245761999
_C1_RPM_VALUE = 1081.0


def _clone_with_channel_col(db: MeasurementDB, col_name: str, expr) -> MeasurementDB:
    """Clone a ``for_debug`` db, adding *col_name* = *expr* to the channels table only."""
    tables = dict(db.config.debug_tables)
    tables["channels"] = tables["channels"].withColumn(col_name, expr)
    return MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=db.ws)


def _only_row_marker(marked_value, other_value):
    """Expression tagging just the single (container 1, _C1_RPM_TSTART) channels row.

    That row gets *marked_value*; every other channels row gets *other_value*.
    """
    is_target = (F.col("container_id") == 1) & (F.col("tstart") == _C1_RPM_TSTART)
    return F.when(is_target, F.lit(marked_value)).otherwise(F.lit(other_value))


def _rpm_x2(query):
    """A calculated channel: Engine RPM * 2 (real, predictable per-interval values)."""
    return CalculatedChannel(
        query.channel(channel_name="Engine RPM") * 2,
        {"channel_name": "rpm_x2", "data_key": "CALC"},
    )


class TestChannelsFilter:
    """DefaultSolver applies ``config.channels.filters`` when reading channels."""

    def test_string_filter_removes_rows_and_keeps_real_value(self, spark, basic_narrow_db):
        """Only the ``source == 'live'`` sample survives, with its correct scaled value."""
        db = _clone_with_channel_col(
            basic_narrow_db, "source", _only_row_marker("live", "archive")
        )
        query = db.query
        cfg = SolverConfig(channels=TableConfig(filters={"source": "live"}))
        result = query.select(_rpm_x2(query)).solve_calculated_channels(
            spark, solver=DefaultSolver(spark, config=cfg)
        )

        rows = result.select("container_id", "tstart", "value").collect()
        # Every channels row except the one tagged "live" was filtered out, so the
        # whole solve collapses to that single retained sample.
        assert len(rows) == 1
        assert rows[0]["container_id"] == 1
        assert rows[0]["tstart"] == _C1_RPM_TSTART
        assert rows[0]["value"] == pytest.approx(_C1_RPM_VALUE * 2)

    def test_no_filter_leaves_behavior_unchanged(self, spark, basic_narrow_db):
        """Default SolverConfig (no channels filters) returns the full, unfiltered data."""
        query = basic_narrow_db.query
        result = query.select(_rpm_x2(query)).solve_calculated_channels(
            spark, solver=DefaultSolver(spark)
        )

        c1 = result.filter(F.col("container_id") == 1)
        # The known datum is still present and correct...
        target = c1.filter(F.col("tstart") == _C1_RPM_TSTART).select("value").collect()
        assert len(target) == 1
        assert target[0]["value"] == pytest.approx(_C1_RPM_VALUE * 2)
        # ...alongside the many other samples the string-filter test removed.
        assert c1.count() > 1

    def test_non_matching_filter_returns_empty(self, spark, basic_narrow_db):
        """A filter value matching no channels rows yields zero results."""
        db = _clone_with_channel_col(
            basic_narrow_db, "source", _only_row_marker("live", "archive")
        )
        query = db.query
        cfg = SolverConfig(channels=TableConfig(filters={"source": "does_not_exist"}))
        result = query.select(_rpm_x2(query)).solve_calculated_channels(
            spark, solver=DefaultSolver(spark, config=cfg)
        )
        assert result.count() == 0

    def test_boolean_column_filter_matches_true_rows(self, spark, basic_narrow_db):
        """A boolean channels column filters on the string ``"true"`` via literal coercion.

        ``TableConfig.filters`` values are always strings; Spark casts the string
        literal to the column's boolean type (the column is never stringified),
        exactly as the other tables' filters already behave.
        """
        db = _clone_with_channel_col(basic_narrow_db, "is_valid", _only_row_marker(True, False))
        query = db.query
        cfg = SolverConfig(channels=TableConfig(filters={"is_valid": "true"}))
        result = query.select(_rpm_x2(query)).solve_calculated_channels(
            spark, solver=DefaultSolver(spark, config=cfg)
        )

        rows = result.select("container_id", "tstart", "value").collect()
        assert len(rows) == 1
        assert rows[0]["container_id"] == 1
        assert rows[0]["tstart"] == _C1_RPM_TSTART
        assert rows[0]["value"] == pytest.approx(_C1_RPM_VALUE * 2)
