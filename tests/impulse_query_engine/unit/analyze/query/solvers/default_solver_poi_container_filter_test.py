# pylint: disable=missing-function-docstring
"""End-to-end tests for POI **container filtering** via the augmented
``filter_container_metrics`` path.

When ``poi_table`` + ``poi_types`` are configured, ``filter_container_metrics``
left-joins the ``PoiContainerTransformer`` rollup onto the container-metrics
frame, so a POI predicate runs through the *same* ``MetricExpression`` machinery
as any other metric — no separate stage, no new selector type:

- scalar count: ``q.metric("poi_defect_count") >= 5``
- value set:    ``q.metric("poi_defect_values").contains("U05B5-81")``

Wide-only model (no ``container_tags_table``) so ``filter_container_metrics``
applies the predicate and returns the surviving containers directly.
"""

import datetime

import pyspark.sql.types as T
import pytest
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.metadata.metric_expression import MetricSelector
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import PoiConfig, SolverConfig
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import mock_workspace_client, spark

# ---------------------------------------------------------------------------
# Inline data: container_metrics (wide) + poi (occurrence log), 3 containers.
#   c1: 3 defect occurrences, codes {A, B}
#   c2: 1 defect occurrence,  code  {A}
#   c3: no defect rows at all
# ---------------------------------------------------------------------------

_CONTAINER_METRICS_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.StringType()),
        T.StructField("duration_ms", T.LongType()),
    ]
)
_CONTAINER_METRICS_ROWS = [("c1", 5000), ("c2", 2000), ("c3", 500)]

_POI_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.StringType()),
        T.StructField("poi_type", T.StringType()),
        T.StructField("timestamp_abs", T.TimestampType()),
        T.StructField("value", T.StringType()),
    ]
)


def _ts(sec: int):
    return datetime.datetime.fromtimestamp(sec, tz=datetime.timezone.utc)


_POI_ROWS = [
    ("c1", "defect", _ts(10), "A"),
    ("c1", "defect", _ts(20), "B"),
    ("c1", "defect", _ts(30), "A"),  # c1: 3 rows, distinct {A, B}
    ("c2", "defect", _ts(15), "A"),  # c2: 1 row, {A}
    # c3: no defect rows
]


@pytest.fixture
def poi_filter_db(spark: SparkSession, mock_workspace_client) -> MeasurementDB:
    tables = {
        "container_metrics": spark.createDataFrame(
            _CONTAINER_METRICS_ROWS, schema=_CONTAINER_METRICS_SCHEMA
        ),
        "poi": spark.createDataFrame(_POI_ROWS, schema=_POI_SCHEMA),
    }
    cfg = MeasurementDBConfig.for_debug(tables)
    return MeasurementDB(cfg, ws=mock_workspace_client)


def _cfg() -> SolverConfig:
    # wide-only (no container_tags mapping / project_id); POI surfaces 'defect'.
    return SolverConfig(poi=PoiConfig(poi_types=["defect"]))


def _survivors(result) -> set[str]:
    return {row.container_id for row in result.collect()}


def test_count_threshold_filters_containers(spark: SparkSession, poi_filter_db: MeasurementDB):
    """`poi_defect_count >= 3` keeps only c1 (3 defects); c2 (1) and c3 (0) drop."""
    solver = DefaultSolver(spark, config=_cfg())
    query = poi_filter_db.query
    query.where(MetricSelector("poi_defect_count") >= 3)

    result = solver.filter_container_metrics(spark, query, None)
    assert _survivors(result) == {"c1"}


def test_count_ge_one_keeps_any_with_defect(spark: SparkSession, poi_filter_db: MeasurementDB):
    """`poi_defect_count >= 1` keeps c1 and c2; c3 (count 0, not null) is excluded."""
    solver = DefaultSolver(spark, config=_cfg())
    query = poi_filter_db.query
    query.where(MetricSelector("poi_defect_count") >= 1)

    result = solver.filter_container_metrics(spark, query, None)
    assert _survivors(result) == {"c1", "c2"}


def test_contains_filters_by_value(spark: SparkSession, poi_filter_db: MeasurementDB):
    """`poi_defect_values.contains("B")` keeps only c1 (c2 has only A)."""
    solver = DefaultSolver(spark, config=_cfg())
    query = poi_filter_db.query
    query.where(MetricSelector("poi_defect_values").contains("B"))

    result = solver.filter_container_metrics(spark, query, None)
    assert _survivors(result) == {"c1"}


def test_poi_composes_with_scalar_metric(spark: SparkSession, poi_filter_db: MeasurementDB):
    """POI count ANDs with an ordinary container-metric filter in one expression."""
    solver = DefaultSolver(spark, config=_cfg())
    query = poi_filter_db.query
    query.where((MetricSelector("duration_ms") > 3000) & (MetricSelector("poi_defect_count") >= 1))

    result = solver.filter_container_metrics(spark, query, None)
    # c1: duration 5000 > 3000 AND 3 defects → kept; c2: 2000 !> 3000 → dropped
    assert _survivors(result) == {"c1"}


def test_no_poi_config_leaves_metrics_unchanged(spark: SparkSession, poi_filter_db: MeasurementDB):
    """With poi_types empty, no POI columns are joined; a plain metric filter still works
    and referencing a POI column would fail — proving the augment is config-gated."""
    solver = DefaultSolver(spark, config=SolverConfig())  # no poi_types
    query = poi_filter_db.query
    query.where(MetricSelector("duration_ms") > 1000)

    result = solver.filter_container_metrics(spark, query, None)
    # c1 (5000), c2 (2000) pass; c3 (500) drops — POI never consulted
    assert _survivors(result) == {"c1", "c2"}
