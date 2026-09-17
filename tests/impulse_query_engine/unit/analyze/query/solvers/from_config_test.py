# pylint: disable=missing-function-docstring
"""Unit tests for the ``from_config`` uniform-instantiation hook.

``QuerySolver.from_config`` builds a config-only solver; ``DefaultSolver``
overrides it to wire the SparkSession and the raw-data flags.  These tests
do not need a live SparkSession — ``DefaultSolver.__init__`` only stores the
handle — so a sentinel object stands in for spark.
"""

from pyspark.sql import DataFrame

from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_query_engine.analyze.query.solvers.solver_config import RawEncoder, SolverConfig
from impulse_query_engine.analyze.query.solvers.solver_context import SolverBuildContext


class _ConfigOnlySolver(QuerySolver):
    """A solver that keeps the base single-arg constructor."""

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


class TestQuerySolverFromConfig:
    def test_base_hook_passes_config_only(self):
        cfg = SolverConfig(project_id="P1")
        ctx = SolverBuildContext(spark=object(), solver_config=cfg)

        solver = _ConfigOnlySolver.from_config(ctx)

        assert isinstance(solver, _ConfigOnlySolver)
        assert solver.config is cfg

    def test_base_hook_defaults_config_when_none(self):
        ctx = SolverBuildContext(spark=object(), solver_config=None)
        solver = _ConfigOnlySolver.from_config(ctx)
        assert isinstance(solver.config, SolverConfig)


class TestDefaultSolverFromConfig:
    def test_wires_spark_config_and_flags(self):
        sentinel_spark = object()
        cfg = SolverConfig(project_id="P2")
        ctx = SolverBuildContext(
            spark=sentinel_spark,
            solver_config=cfg,
            is_raw_data=True,
            drop_implausible_data=True,
            raw_encoder=RawEncoder.INTERVAL,
        )

        solver = DefaultSolver.from_config(ctx)

        assert isinstance(solver, DefaultSolver)
        assert solver.spark is sentinel_spark
        assert solver.config is cfg
        assert solver.is_raw_data is True
        assert solver.drop_implausible_data is True
        assert solver.raw_encoder is RawEncoder.INTERVAL

    def test_defaults_match_constructor_defaults(self):
        ctx = SolverBuildContext(spark=object())
        solver = DefaultSolver.from_config(ctx)
        assert solver.is_raw_data is False
        assert solver.drop_implausible_data is False
        assert solver.raw_encoder is RawEncoder.RLE
        assert isinstance(solver.config, SolverConfig)
