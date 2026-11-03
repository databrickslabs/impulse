# pylint: disable=missing-function-docstring
"""Tests for the solver registry wiring and the ``from_config`` build hook.

The registry mechanics (aliases, last registration wins, the unknown-name message) are covered
in ``registry_test.py``.
"""

from unittest.mock import sentinel

import pytest

from impulse_query_engine.analyze.query.solvers import (
    DefaultSolver,
    QuerySolver,
    SolverBuildContext,
    SolverConfig,
    register_solver,
    resolve_solver,
)
from impulse_query_engine.analyze.query.solvers.solver_config import RawEncoder


@pytest.mark.parametrize("name", ["DefaultSolver", "DeltaSolver", "KeyValueStoreSolver"])
def test_default_solver_is_registered_under_its_name_and_aliases(name):
    registration = resolve_solver(name)

    assert registration.impl_cls is DefaultSolver
    assert registration.config_cls is SolverConfig


def test_register_rejects_non_solver_class():
    with pytest.raises(TypeError, match="must subclass QuerySolver"):
        register_solver("Bad")(object)


def test_register_rejects_non_solver_config_class():
    with pytest.raises(TypeError, match="must subclass SolverConfig"):
        register_solver("Bad", dict)


def test_unknown_name_error_names_the_solver_kind():
    with pytest.raises(KeyError, match=r"Unknown solver 'Missing'"):
        resolve_solver("Missing")


def test_base_from_config_passes_only_the_solver_config():
    class ConfigOnlySolver(QuerySolver):
        # QuerySolver's five pipeline stages are abstract; this test only builds the solver.
        def filter_container_tags(self, spark, query): ...

        def filter_container_metrics(
            self, spark, query, container_df, pre_filtered_containers_df=None
        ): ...

        def filter_channel_tags(self, spark, db, container_df, selectors): ...

        def filter_channel_metrics(self, spark, db, channel_df, selectors): ...

        def solve(self, query, channels_df, selections, dtypes): ...

    config = SolverConfig(project_id="p1")
    solver = ConfigOnlySolver.from_config(
        SolverBuildContext(spark=sentinel.spark, solver_config=config, is_raw_data=True)
    )

    assert type(solver) is ConfigOnlySolver
    assert solver.config is config


def test_default_solver_from_config_wires_spark_and_raw_flags():
    config = SolverConfig()
    solver = DefaultSolver.from_config(
        SolverBuildContext(
            spark=sentinel.spark,
            solver_config=config,
            is_raw_data=True,
            drop_implausible_data=True,
            raw_encoder=RawEncoder.INTERVAL,
        )
    )

    assert solver.spark is sentinel.spark
    assert solver.config is config
    assert solver.is_raw_data is True
    assert solver.drop_implausible_data is True
    assert solver.raw_encoder is RawEncoder.INTERVAL
