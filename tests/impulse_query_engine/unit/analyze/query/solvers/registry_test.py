# pylint: disable=missing-function-docstring
"""Unit tests for the solver registry.

Covers ``register_solver`` (name + aliases + config_cls + overwrite),
``resolve_registration`` (name/alias lookup and unknown-name error),
``is_registered`` and ``registered_names``.

The registry is process-global, so each test registers under unique names
and restores the registry via the ``clean_registry`` fixture to avoid
cross-test contamination.
"""

import pytest
from pyspark.sql import DataFrame

from impulse_query_engine.analyze.query.solvers import registry
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_query_engine.analyze.query.solvers.registry import (
    SolverRegistration,
    is_registered,
    register_solver,
    registered_names,
    resolve_registration,
)
from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig


class _StubSolver(QuerySolver):
    """Minimal concrete QuerySolver for registration tests."""

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


class _OtherStubSolver(_StubSolver):
    """A distinct subclass used for conflict/overwrite tests."""


class _StubConfig(SolverConfig):
    """A SolverConfig subclass carrying an extra field."""

    extra_table: str = "default_table"


@pytest.fixture
def clean_registry():
    """Snapshot and restore the process-global registry around each test."""
    saved = dict(registry._REGISTRY)
    try:
        yield
    finally:
        registry._REGISTRY.clear()
        registry._REGISTRY.update(saved)


class TestRegisterSolver:
    def test_registers_under_name_with_default_config(self, clean_registry):
        register_solver("StubA")(_StubSolver)

        reg = resolve_registration("StubA")
        assert isinstance(reg, SolverRegistration)
        assert reg.solver_cls is _StubSolver
        assert reg.config_cls is SolverConfig

    def test_registers_with_custom_config_cls(self, clean_registry):
        register_solver("StubB", _StubConfig)(_StubSolver)

        reg = resolve_registration("StubB")
        assert reg.solver_cls is _StubSolver
        assert reg.config_cls is _StubConfig

    def test_decorator_returns_class_unchanged(self, clean_registry):
        returned = register_solver("StubC")(_StubSolver)
        assert returned is _StubSolver

    def test_aliases_resolve_to_same_registration(self, clean_registry):
        register_solver("StubD", aliases=("StubDeprecated", "StubLegacy"))(_StubSolver)

        primary = resolve_registration("StubD")
        assert resolve_registration("StubDeprecated") == primary
        assert resolve_registration("StubLegacy") == primary

    def test_duplicate_same_class_is_idempotent(self, clean_registry):
        register_solver("StubE")(_StubSolver)
        # Re-registering the SAME class under the same name must not raise
        # (e.g. a module re-imported in a notebook).
        register_solver("StubE")(_StubSolver)
        assert resolve_registration("StubE").solver_cls is _StubSolver

    def test_conflicting_duplicate_raises(self, clean_registry):
        register_solver("StubF")(_StubSolver)
        with pytest.raises(ValueError, match="already registered"):
            register_solver("StubF")(_OtherStubSolver)

    def test_overwrite_replaces_registration(self, clean_registry):
        register_solver("StubG")(_StubSolver)
        register_solver("StubG", overwrite=True)(_OtherStubSolver)
        assert resolve_registration("StubG").solver_cls is _OtherStubSolver

    def test_non_subclass_raises_type_error(self, clean_registry):
        class NotASolver:
            pass

        with pytest.raises(TypeError):
            register_solver("StubH")(NotASolver)


class TestResolveRegistration:
    def test_unknown_name_raises_keyerror_listing_names(self, clean_registry):
        register_solver("StubKnown")(_StubSolver)
        with pytest.raises(KeyError) as exc:
            resolve_registration("DoesNotExist")
        # The error message should help the user by listing known names.
        assert "StubKnown" in str(exc.value)


class TestIntrospection:
    def test_is_registered(self, clean_registry):
        assert not is_registered("StubI")
        register_solver("StubI")(_StubSolver)
        assert is_registered("StubI")

    def test_registered_names_sorted_and_includes_aliases(self, clean_registry):
        register_solver("StubJ", aliases=("StubJAlias",))(_StubSolver)
        names = registered_names()
        assert "StubJ" in names
        assert "StubJAlias" in names
        assert names == sorted(names)


class TestBuiltinRegistration:
    def test_default_solver_is_registered(self):
        # DefaultSolver self-registers at import time under "DefaultSolver"
        # with the deprecated aliases.
        from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver

        assert resolve_registration("DefaultSolver").solver_cls is DefaultSolver
        assert resolve_registration("DeltaSolver").solver_cls is DefaultSolver
        assert resolve_registration("KeyValueStoreSolver").solver_cls is DefaultSolver
