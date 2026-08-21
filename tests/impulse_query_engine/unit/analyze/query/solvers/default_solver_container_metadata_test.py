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
from pyspark.sql import SparkSession

import impulse_query_engine.schema as S
from impulse_query_engine.analyze.query.channels.calculated_channel import CalculatedChannel
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import (
    basic_narrow_db,
    key_value_store_alias_with_channel_tags_db,
    narrow_db,
    spark,
)


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


def test_required_tag_does_not_drop_containers_lacking_it(
    spark: SparkSession, narrow_db: MeasurementDB
):
    """Requesting a container tag must not filter out containers that lack it.

    A container present in ``container_tags`` (via some other key) but without a
    row for the requested key stays in the result with that column null — a
    requested tag is injected, not used as a filter.
    """
    # Clone narrow_db (container 1 has name=test) and add a container 2 that has
    # a tag row but no ``name`` row.
    tables = dict(narrow_db.config.debug_tables)
    extra = spark.createDataFrame([(2, "other", "x")], schema=S.CONTAINER_TAGS)
    tables["container_tags"] = tables["container_tags"].unionByName(extra)
    db = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=narrow_db.ws)

    result = DefaultSolver(spark).filter_container_tags(
        spark, db.query, required_container_tags=["name"]
    )
    rows = {row.container_id: row["name"] for row in result.collect()}
    assert rows == {1: "test", 2: None}, rows


def test_eav_model_injects_tag_and_metric_together(spark: SparkSession, narrow_db: MeasurementDB):
    """A UDF requesting both a container tag and a metric gets both.

    Exercises threading a container tag (from the EAV ``container_tags`` pivot in
    ``filter_container_tags``) alongside a container metric (from
    ``filter_container_metrics``) through the whole cascade — narrow_db carries
    both the ``name`` tag and ``num_channels``.
    """

    def grab_both(ts, container_tags, container_metrics):
        tag_ok = container_tags["name"] == "test"
        metric = container_metrics["num_channels"]
        return float(metric) if tag_ok and metric is not None else -1.0

    query = narrow_db.query
    result = query.select(
        query.channel(seed="0")
        .apply(grab_both, container_tags=["name"], container_metrics=["num_channels"])
        .alias("both")
    ).solve(spark, solver=DefaultSolver(spark))

    rows = {row.container_id: row.both for row in result.collect()}
    assert rows, "expected at least one matched container"
    # container 1 carries name=test and num_channels=1 -> both arrived.
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


def test_aggregation_propagates_container_metric(
    spark: SparkSession, basic_narrow_db: MeasurementDB
):
    """A metadata UDF wrapped in an aggregation propagates + is fed the metric.

    The UDF maps the series to a constant equal to ``num_channels`` (== 11); the
    surrounding histogram then bins that constant.  All mass must land in the
    ``[10, 20]`` bin, proving the metric reached the UDF inside the aggregation.
    """

    def const_from_metric(ts, container_metrics):
        factor = container_metrics["num_channels"]
        return ts * 0 + (factor if factor is not None else 0)

    query = basic_narrow_db.query
    hist = (
        query.channel(channel_name="Engine RPM")
        .apply(const_from_metric, container_metrics=["num_channels"])
        .histogram([0.0, 10.0, 20.0])
        .alias("h")
    )
    result = query.select(hist).solve(spark, solver=DefaultSolver(spark))

    rows = [row.h for row in result.collect()]
    assert rows, "expected at least one matched container"
    for hist_value in rows:
        counts = hist_value["H"]  # duration per bin: [ [0,10), [10,20) ]
        assert counts[0] == 0.0, counts  # nothing below 10
        assert counts[1] > 0.0, counts  # constant 11 lands in [10, 20)


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


def test_pre_filtered_containers_scopes_metric_injection(
    spark: SparkSession, basic_narrow_db: MeasurementDB
):
    """The metric injection honors ``pre_filtered_containers_df``.

    Because the container-metric columns are threaded from
    ``filter_container_metrics`` (which reads ``pre_filtered_containers_df`` when
    provided) rather than an independent full read, a subset restricted to a
    single container (a) processes only that container and (b) still injects its
    metric value correctly.
    """

    def grab_metric(ts, container_metrics):
        value = container_metrics["num_channels"]
        return float(value) if value is not None else -1.0

    query = basic_narrow_db.query
    # Incremental subset: container 1 only. Default config has no column mapping,
    # so the physical container_metrics frame is a valid pre-filtered input.
    subset = basic_narrow_db.container_metrics(spark).where("container_id = 1")
    result = query.select(
        query.channel(channel_name="Engine RPM")
        .apply(grab_metric, container_metrics=["num_channels"])
        .alias("nc")
    ).solve(spark, solver=DefaultSolver(spark), pre_filtered_containers_df=subset)

    rows = {row.container_id: row.nc for row in result.collect()}
    assert set(rows.keys()) == {1}, rows  # only the pre-filtered subset is processed
    assert rows[1] == 11.0, rows  # injected metric value is correct
