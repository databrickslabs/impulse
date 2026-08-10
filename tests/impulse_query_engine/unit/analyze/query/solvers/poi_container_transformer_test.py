# pylint: disable=missing-function-docstring
"""Unit tests for ``PoiTransformer.to_container_granularity``.

Rolls the wide ``poi`` occurrence log (N rows per container) up to one row per
``container_id`` with, for each configured ``poi_type``, a ``poi_<type>_values``
set column and a ``poi_<type>_count`` total. Uses a real sample (defect POIs with
**string** value codes) loaded from CSV.
"""

import os

from pyspark.sql import SparkSession

from impulse_query_engine.analyze.query.solvers.poi_transformer import (
    PoiTransformer,
)
from impulse_query_engine.analyze.query.solvers.solver_config import PoiConfig, SolverConfig
from tests.conftest import spark

_CONTAINER = "000dc8fa03f65cfca5070a5eda174691fbfb067530e82c376809d0ead870c382"


def _poi_df(spark: SparkSession):
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    path = f"{base_path}/tests/unit/data/poi_container_rollup_csv/poi.csv"
    return spark.read.options(header="True", delimiter=",", inferSchema="True").csv(path)


def test_rollup_produces_values_set_and_total_count(spark: SparkSession):
    """One container, 7 defect rows → distinct value set + COUNT(*) of rows.

    Rows: B1024-43 (x2), B12CA-83, U0046-13, U05B5-81 (x2), U1213-81
      → 5 distinct codes, count = 7 (one per row; occurrences are NOT
      pre-aggregated, so the rollup counts rows itself).
    """
    cfg = SolverConfig(poi=PoiConfig(poi_types=["defect"]))
    rolled = PoiTransformer(cfg).to_container_granularity(_poi_df(spark))

    rows = rolled.collect()
    assert len(rows) == 1  # one container
    row = rows[0]
    assert row["container_id"] == _CONTAINER

    # poi_defect_values: distinct codes, sorted (collect_set + array_sort)
    assert list(row["poi_defect_values"]) == [
        "B1024-43",
        "B12CA-83",
        "U0046-13",
        "U05B5-81",
        "U1213-81",
    ]
    # poi_defect_count: COUNT(*) of the 7 defect rows
    assert row["poi_defect_count"] == 7


def test_unconfigured_type_is_absent(spark: SparkSession):
    """Only configured poi_types get columns; the data is all 'defect', so a
    configured 'aeb' type yields an empty value set + count 0 for the container."""
    cfg = SolverConfig(poi=PoiConfig(poi_types=["defect", "aeb"]))
    rolled = PoiTransformer(cfg).to_container_granularity(_poi_df(spark))

    row = rolled.collect()[0]
    # defect present
    assert row["poi_defect_count"] == 7
    # aeb configured but no rows → empty set, count 0 (never null)
    assert list(row["poi_aeb_values"]) == []
    assert row["poi_aeb_count"] == 0


def test_only_configured_columns_emitted(spark: SparkSession):
    """The rollup emits exactly the (values, count) pair per configured type,
    plus container_id — no stray columns."""
    cfg = SolverConfig(poi=PoiConfig(poi_types=["defect"]))
    rolled = PoiTransformer(cfg).to_container_granularity(_poi_df(spark))

    assert set(rolled.columns) == {"container_id", "poi_defect_values", "poi_defect_count"}
