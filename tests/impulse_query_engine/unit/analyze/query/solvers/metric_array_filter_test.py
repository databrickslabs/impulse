# pylint: disable=missing-function-docstring, redefined-outer-name
"""Spark-evaluating tests for array-membership MetricExpression operators.

``.contains`` / ``.contains_any`` / ``.contains_all`` build a boolean predicate
that runs in ``filter_container_metrics`` (Spark ``.where``). These tests assert
the *filtering semantics* — which containers survive — against an in-memory
``container_metrics`` table carrying an ``array<string>`` column, which no CSV
fixture can express. The pure builder-structure tests live in
``unit/model/expressions/metric_expression_test.py``.

Set fixture (``poi_defect_values`` per container):
    c1 → [A, B]   c2 → [A]   c3 → [C]   c4 → [] (empty)
"""

from unittest.mock import create_autospec

import pytest
import pyspark.sql.types as T
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.metadata.metric_expression import MetricSelector
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import spark


@pytest.fixture
def array_metric_db(spark: SparkSession) -> MeasurementDB:
    """Wide-only in-memory DB whose container_metrics has an array<string> column."""
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("poi_defect_values", T.ArrayType(T.StringType())),
        ]
    )
    container_metrics = spark.createDataFrame(
        [
            (1, ["A", "B"]),
            (2, ["A"]),
            (3, ["C"]),
            (4, []),
        ],
        schema=schema,
    )
    cfg = MeasurementDBConfig.for_debug({"container_metrics": container_metrics})
    return MeasurementDB(cfg, ws=create_autospec(WorkspaceClient))


def _survivors(db: MeasurementDB, spark: SparkSession, predicate) -> set:
    """Apply *predicate* through filter_container_metrics and return surviving ids."""
    solver = DefaultSolver(spark, config=SolverConfig())  # wide-only, no project_id
    query = db.query
    query.where(predicate)
    tags_df = solver.filter_container_tags(spark, query)  # empty in wide-only mode
    result = solver.filter_container_metrics(spark, query, tags_df)
    return {row.container_id for row in result.collect()}


class TestContains:
    def test_contains_single_value(self, spark: SparkSession, array_metric_db: MeasurementDB):
        """`.contains("A")` keeps containers whose set includes A → c1, c2."""
        got = _survivors(array_metric_db, spark, MetricSelector("poi_defect_values").contains("A"))
        assert got == {1, 2}

    def test_contains_absent_value_keeps_none(
        self, spark: SparkSession, array_metric_db: MeasurementDB
    ):
        got = _survivors(array_metric_db, spark, MetricSelector("poi_defect_values").contains("Z"))
        assert got == set()


class TestContainsAny:
    def test_contains_any_is_union(self, spark: SparkSession, array_metric_db: MeasurementDB):
        """`.contains_any([A, C])` keeps any container with A or C → c1, c2, c3."""
        pred = MetricSelector("poi_defect_values").contains_any(["A", "C"])
        assert _survivors(array_metric_db, spark, pred) == {1, 2, 3}

    def test_contains_any_single_matches_like_contains(
        self, spark: SparkSession, array_metric_db: MeasurementDB
    ):
        pred = MetricSelector("poi_defect_values").contains_any(["B"])
        assert _survivors(array_metric_db, spark, pred) == {1}

    def test_contains_any_none_present(self, spark: SparkSession, array_metric_db: MeasurementDB):
        pred = MetricSelector("poi_defect_values").contains_any(["Y", "Z"])
        assert _survivors(array_metric_db, spark, pred) == set()

    def test_contains_any_empty_list_matches_nothing(
        self, spark: SparkSession, array_metric_db: MeasurementDB
    ):
        """An empty candidate set overlaps nothing → no survivors (incl. the empty-set c4)."""
        pred = MetricSelector("poi_defect_values").contains_any([])
        assert _survivors(array_metric_db, spark, pred) == set()


class TestContainsAll:
    def test_contains_all_requires_every_value(
        self, spark: SparkSession, array_metric_db: MeasurementDB
    ):
        """`.contains_all([A, B])` keeps only the set containing both → c1."""
        pred = MetricSelector("poi_defect_values").contains_all(["A", "B"])
        assert _survivors(array_metric_db, spark, pred) == {1}

    def test_contains_all_single_value(self, spark: SparkSession, array_metric_db: MeasurementDB):
        pred = MetricSelector("poi_defect_values").contains_all(["A"])
        assert _survivors(array_metric_db, spark, pred) == {1, 2}

    def test_contains_all_partial_match_excluded(
        self, spark: SparkSession, array_metric_db: MeasurementDB
    ):
        """c2 has A but not C, so `.contains_all([A, C])` excludes it → none."""
        pred = MetricSelector("poi_defect_values").contains_all(["A", "C"])
        assert _survivors(array_metric_db, spark, pred) == set()

    def test_contains_all_empty_list_matches_all(
        self, spark: SparkSession, array_metric_db: MeasurementDB
    ):
        """Vacuously true: an empty required set is satisfied by every container."""
        pred = MetricSelector("poi_defect_values").contains_all([])
        assert _survivors(array_metric_db, spark, pred) == {1, 2, 3, 4}


class TestComposition:
    def test_contains_any_ands_with_scalar_metric(
        self, spark: SparkSession, array_metric_db: MeasurementDB
    ):
        """Array op composes with a scalar comparison via `&`."""
        pred = (MetricSelector("poi_defect_values").contains_any(["A", "C"])) & (
            MetricSelector("container_id") <= 2
        )
        # A-or-C → {1,2,3}; container_id <= 2 → {1,2}; AND → {1,2}
        assert _survivors(array_metric_db, spark, pred) == {1, 2}
