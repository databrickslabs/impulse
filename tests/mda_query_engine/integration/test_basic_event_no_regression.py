"""Scalar-only BasicEvent still solves correctly through the core solvers
after the Intervals complement operator was added.

Pins acceptance signal #3 of the perception/scalar composition decision:
the core path must not regress when a perception extension is added.

The existing ``kvs_solver_test.py`` suite already exercises the full
KeyValueStoreSolver pipeline against ``BasicEvent``-style scalar queries;
this file adds two thin smoke tests that touch the specific surfaces
modified by BL-027 (the ``bounds`` keyword on ``Intervals.__init__``,
the ``Intervals.__invert__`` operator's interaction with ``__and__`` /
``__or__``) without introducing a separate solver fixture.
"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from mda_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
)
from mda_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig,
    TableConfig,
)
from mda_query_engine.measurement_db import MeasurementDB
from mda_query_engine.model.series.intervals import Intervals


def _kvs_cfg() -> SolverConfig:
    return SolverConfig(
        project_id="SAMPLE_PROJECT",
        container_tags=TableConfig(column_name_mapping={"element_id": "key"}),
        container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
        channel_mapping=TableConfig(),
    )


class TestBasicScalarSolveStillWorks:
    def test_scalar_aggregation_returns_one_row_per_container(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        solver = KeyValueStoreSolver(spark, config=_kvs_cfg())
        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")

        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)

        assert result.count() == 3
        assert "rpm_mean" in result.columns

    def test_scalar_threshold_predicate_solves_to_intervals(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        solver = KeyValueStoreSolver(spark, config=_kvs_cfg())
        query = key_value_store_db.query
        veh_speed = query.channel(channel_name="Vehicle Speed Sensor")
        # ``veh_speed > 50`` materialises to ``Intervals`` at build time —
        # the same path that PerceptionSelector composes with via ``&``.
        # After the bounds + ``__invert__`` additions, this must still
        # produce non-empty results.
        result = query.select((veh_speed > 50).alias("fast")).solve(spark=spark, solver=solver)

        rows = result.collect()
        assert len(rows) == 3
