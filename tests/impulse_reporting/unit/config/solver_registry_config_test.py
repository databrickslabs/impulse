# pylint: disable=missing-function-docstring
"""Unit tests for solver selection + extended-config validation in QueryEngine.

Verifies the key requirement: when a report config selects a custom solver by
name, its ``solver_config`` block is validated through the *registered* config
class (a SolverConfig subclass), so extra/required fields are enforced at parse
time and the subclass instance type is preserved.

Also verifies the StrEnum migration keeps existing enum-based comparisons and
the deprecated-alias names working.
"""

import pytest
from pydantic import ValidationError
from pyspark.sql import DataFrame

from impulse_query_engine.analyze.query.solvers import registry
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_query_engine.analyze.query.solvers.registry import register_solver
from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig
from impulse_reporting.config.config_parser import QueryEngine, Solvers


class _RegConfig(SolverConfig):
    """SolverConfig subclass with one required and one optional extra field."""

    raw_signal_table: str  # required
    gps_signal_name: str = "GPS_LAT"  # optional with default


class _RegSolver(QuerySolver):
    def filter_container_tags(self, spark, query) -> DataFrame:  # noqa: D102
        raise NotImplementedError

    def filter_container_metrics(
        self, spark, query, container_df, pre_filtered_containers_df=None
    ) -> DataFrame:  # noqa: D102
        raise NotImplementedError

    def filter_channel_tags(self, spark, db, container_df, selectors) -> DataFrame:  # noqa: D102
        raise NotImplementedError

    def filter_channel_metrics(self, spark, db, channel_df, selectors) -> DataFrame:  # noqa: D102
        raise NotImplementedError

    def solve(self, query, channels_df, selections, dtypes):  # noqa: D102
        raise NotImplementedError


@pytest.fixture
def registered_custom_solver():
    """Register a custom solver+config for the duration of a test."""
    saved = dict(registry._REGISTRY)
    register_solver("RegSolver", _RegConfig)(_RegSolver)
    try:
        yield
    finally:
        registry._REGISTRY.clear()
        registry._REGISTRY.update(saved)


class TestExtendedConfigValidation:
    def test_extended_config_validated_and_typed(self, registered_custom_solver):
        qe = QueryEngine.model_validate(
            {
                "solver": "RegSolver",
                "solver_config": {"raw_signal_table": "cat.sch.raw"},
            }
        )
        # The subclass instance type is preserved on the base-typed field.
        assert isinstance(qe.solver_config, _RegConfig)
        assert qe.solver_config.raw_signal_table == "cat.sch.raw"
        assert qe.solver_config.gps_signal_name == "GPS_LAT"

    def test_missing_required_field_raises_at_parse_time(self, registered_custom_solver):
        with pytest.raises(ValidationError, match="raw_signal_table"):
            QueryEngine.model_validate({"solver": "RegSolver", "solver_config": {}})

    def test_unknown_solver_name_raises_with_config(self):
        # An unknown name is rejected at parse time (surfaced as ValidationError).
        with pytest.raises(ValidationError):
            QueryEngine.model_validate({"solver": "NopeSolver", "solver_config": {"foo": "bar"}})

    def test_unknown_solver_name_raises_without_config(self):
        # Fail-fast even when no solver_config block is present.
        with pytest.raises(ValidationError):
            QueryEngine.model_validate({"solver": "NopeSolver"})


class TestDefaultAndAliasBehavior:
    def test_default_solver_uses_base_config(self):
        qe = QueryEngine.model_validate(
            {"solver": "DefaultSolver", "solver_config": {"project_id": "P"}}
        )
        assert type(qe.solver_config) is SolverConfig
        assert qe.solver_config.project_id == "P"

    def test_default_when_solver_omitted(self):
        qe = QueryEngine.model_validate({})
        assert qe.solver == Solvers.DEFAULT_SOLVER

    def test_no_solver_config_stays_none(self):
        qe = QueryEngine.model_validate({"solver": "DefaultSolver"})
        assert qe.solver_config is None

    @pytest.mark.parametrize(
        "name,enum_member",
        [
            ("DefaultSolver", Solvers.DEFAULT_SOLVER),
            ("DeltaSolver", Solvers.DELTA_SOLVER),
            ("KeyValueStoreSolver", Solvers.KEY_VALUE_STORE_SOLVER),
        ],
    )
    def test_strenum_regression_equality_holds(self, name, enum_member):
        # After widening solver -> str, a StrEnum member still compares equal
        # to the stored string, so existing `== Solvers.X` assertions pass.
        qe = QueryEngine.model_validate({"solver": name})
        assert qe.solver == enum_member
