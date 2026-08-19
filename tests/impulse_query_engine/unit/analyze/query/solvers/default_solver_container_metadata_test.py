# pylint: disable=missing-function-docstring, redefined-outer-name
"""End-to-end tests for container-level tags/metrics injected into solve UDFs.

A ``TimeSeriesUDF`` declares the container tag keys / metric columns it needs;
the engine threads exactly those columns through the filter cascade so they
reach the per-container solve UDF, where they are injected as ``container_tags``
/ ``container_metrics`` keyword arguments.

Covers both silver-layer shapes:
- wide model (``basic_narrow_db``, no ``container_tags_table``) -> metrics work,
  container tags are unsupported;
- EAV model (``narrow_db``, has ``container_tags``) -> tags work; a missing
  metric column raises.
"""

import pytest
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.query.aggregations.aggregation import Aggregation
from impulse_query_engine.analyze.query.channels.calculated_channel import CalculatedChannel
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB
from tests.conftest import (
    basic_narrow_db,
    key_value_store_alias_with_channel_tags_db,
    narrow_db,
    spark,
)


class _MetricProbe(Aggregation):
    """Test-local aggregation that returns a declared container metric from build().

    Proves an ``Aggregation`` subclass can declare container metrics and read
    them via :meth:`resolve_container_metadata` inside ``build()``.
    """

    def __init__(self, selection, metric):
        self.selection = selection
        self._metric = metric
        self._set_container_metadata(container_metrics=[metric])

    def _container_metadata_children(self):
        return (self.selection,)

    def dtype(self):
        return T.DoubleType()

    def build(self, cache):
        self.selection.build(cache)  # ensure the channel is selected/threaded
        _, metrics = self.resolve_container_metadata(cache)
        value = metrics.get(self._metric)
        return float(value) if value is not None else -1.0

    def required_tags(self):
        return self.selection.required_tags()

    def get_required_tag_exprs(self):
        return self.selection.get_required_tag_exprs()

    def get_selector_expr(self):
        return self.selection.get_selector_expr()

    def get_selectors(self):
        return self.selection.get_selectors()

    def __str__(self):
        return f"<_MetricProbe metric={self._metric}>"


def _alias_config() -> SolverConfig:
    """SolverConfig the alias CSV fixtures expect (see default_solver_alias_test)."""
    return SolverConfig(
        project_id="SAMPLE_PROJECT",
        container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
        channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
    )


def test_wide_model_injects_container_metric(spark: SparkSession, basic_narrow_db: MeasurementDB):
    """A declared container metric reaches the UDF for each container."""

    def grab_metric(ts, container_metrics):
        value = container_metrics["num_channels"]
        return float(value) if value is not None else -1.0

    query = basic_narrow_db.query
    result = query.select(
        query.channel(channel_name="Engine RPM")
        .apply(grab_metric, container_metrics=["num_channels"])
        .alias("nc")
    ).solve(spark, solver=DefaultSolver(spark))

    rows = {row.container_id: row.nc for row in result.collect()}
    assert rows, "expected at least one matched container"
    # Every container in basic_narrow_csv has num_channels == 11.
    assert all(value == 11.0 for value in rows.values()), rows


def test_wide_model_container_tags_unsupported(
    spark: SparkSession, basic_narrow_db: MeasurementDB
):
    """Requesting container tags without a container_tags_table raises."""

    def use_tag(ts, container_tags):
        return 0.0

    query = basic_narrow_db.query
    prepared = query.select(
        query.channel(channel_name="Engine RPM")
        .apply(use_tag, container_tags=["brand"])
        .alias("x")
    )
    with pytest.raises(ValueError, match="container_tags_table"):
        prepared.solve(spark, solver=DefaultSolver(spark))


def test_eav_model_injects_container_tag(spark: SparkSession, narrow_db: MeasurementDB):
    """A declared container tag reaches the UDF (EAV container_tags table)."""

    def tag_matches(ts, container_tags):
        return 1.0 if container_tags["name"] == "test" else 0.0

    query = narrow_db.query
    result = query.select(
        query.channel(seed="0").apply(tag_matches, container_tags=["name"]).alias("m")
    ).solve(spark, solver=DefaultSolver(spark))

    rows = {row.container_id: row.m for row in result.collect()}
    assert rows, "expected at least one matched container"
    # container 1 carries the tag name=test.
    assert rows.get(1) == 1.0, rows


def test_eav_model_missing_metric_raises(spark: SparkSession, narrow_db: MeasurementDB):
    """Requesting a nonexistent container metric column raises a clear error."""

    def use_metric(ts, container_metrics):
        return 0.0

    query = narrow_db.query
    prepared = query.select(
        query.channel(seed="0").apply(use_metric, container_metrics=["does_not_exist"]).alias("x")
    )
    with pytest.raises(ValueError, match="does_not_exist"):
        prepared.solve(spark, solver=DefaultSolver(spark))


def test_aliased_path_threads_container_metric(
    spark: SparkSession, key_value_store_alias_with_channel_tags_db: MeasurementDB
):
    """A container metric threads through the aliased resolution + union path.

    Uses a purely aliased selection so the metadata column must survive
    ``filter_aliased_channel_metrics`` and ``resolve_channel_selections``
    (union of the empty direct side with the aliased side + group-by).
    """

    def grab_metric(ts, container_metrics):
        value = container_metrics["num_channels"]
        return float(value) if value is not None else -1.0

    solver = DefaultSolver(spark, config=_alias_config())
    query = key_value_store_alias_with_channel_tags_db.query
    result = query.select(
        query.channel_with_alias(channel_alias="engine_speed")
        .apply(grab_metric, container_metrics=["num_channels"])
        .alias("nc")
    ).solve(spark, solver=solver)

    rows = {row.container_id: row.nc for row in result.collect()}
    # The alias resolves a real channel in all three containers.
    assert set(rows.keys()) == {1, 2, 3}, rows
    # Every container in basic_narrow_csv has num_channels == 11.
    assert all(value == 11.0 for value in rows.values()), rows


def test_aggregation_reads_container_metric(spark: SparkSession, basic_narrow_db: MeasurementDB):
    """An Aggregation subclass declares a metric and reads it in build()."""
    query = basic_narrow_db.query
    probe = _MetricProbe(query.channel(channel_name="Engine RPM"), "num_channels").alias("nc")
    result = query.select(probe).solve(spark, solver=DefaultSolver(spark))

    rows = {row.container_id: row.nc for row in result.collect()}
    assert rows, "expected at least one matched container"
    assert all(value == 11.0 for value in rows.values()), rows


def test_calculated_channel_threads_container_metric(
    spark: SparkSession, basic_narrow_db: MeasurementDB
):
    """A CalculatedChannel wrapping a metadata UDF gets the metric via the calc path.

    The scaled channel multiplies each sample by ``num_channels`` (== 11); the raw
    channel does not.  Proves the calculated-channels UDF cache is wired with the
    container metadata (it previously was not).
    """

    def scale_by_metric(ts, container_metrics):
        factor = container_metrics["num_channels"]
        return ts if factor is None else ts * factor

    query = basic_narrow_db.query
    raw = CalculatedChannel(
        query.channel(channel_name="Engine RPM"),
        {"channel_name": "raw_rpm", "data_key": "CALC"},
    )
    scaled = CalculatedChannel(
        query.channel(channel_name="Engine RPM").apply(
            scale_by_metric, container_metrics=["num_channels"]
        ),
        {"channel_name": "scaled_rpm", "data_key": "CALC"},
    )
    result = query.select(raw, scaled).solve_calculated_channels(
        spark, solver=DefaultSolver(spark)
    )

    totals = {
        row.channel_id: row["sum(value)"]
        for row in result.groupBy("channel_id").agg({"value": "sum"}).collect()
    }
    raw_total = totals.get(raw.channel_id)
    scaled_total = totals.get(scaled.channel_id)
    assert raw_total not in (None, 0), totals
    # Every sample scaled by num_channels == 11.
    assert scaled_total == pytest.approx(11.0 * raw_total), totals
