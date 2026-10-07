"""Integration tests for TimeWindowEvent with end-to-end Report usage."""

import math
from unittest.mock import create_autospec

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from databricks.sdk import WorkspaceClient
from pyspark.sql import Window

from impulse_query_engine.analyze.query.solvers.solver_config import RawEncoder, SolverConfig
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.config.config_parser import (
    Comparator,
    ContainerFilters,
    DataType,
    ImpulseConfig,
    IncrementalConfig,
    MetricFilter,
    QueryEngine,
    Solvers,
    Source,
    UnitySink,
)
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.container_event import ContainerEvent
from impulse_reporting.events.time_window_event import TimeWindowEvent
from tests.conftest import setup_basic_db, spark  # noqa: F401  (pytest fixtures)

# Container boundaries (epoch ms) for the Seat_Leon measurements in
# container_metrics.csv, as documented in container_event_test.py.
# c1: span 107545 ms, c2: 108752 ms, c3: 110083 ms
EXPECTED_CONTAINERS = {
    1: {"start_ts": 1751528502708, "stop_ts": 1751528610253},
    2: {"start_ts": 1751528501483, "stop_ts": 1751528610235},
    3: {"start_ts": 1751528500169, "stop_ts": 1751528610252},
}
WINDOW_LENGTH = 10_000  # 10 seconds, in epoch-ms units


def _config(table_prefix: str) -> ImpulseConfig:
    return ImpulseConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver.container_metrics",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(
            catalog="spark_catalog",
            schema="gold",
            table_prefix=table_prefix,
        ),
        container_filters=ContainerFilters(
            metric_filters=[
                [
                    MetricFilter(
                        column_name="vehicle_key", comparator=Comparator.EQ, value="Seat_Leon"
                    ),
                    MetricFilter(
                        column_name="start_dt",
                        comparator=Comparator.GE,
                        value="2025-07-03T07:00:00.000Z",
                    ),
                ]
            ]
        ),
        query_engine=QueryEngine(solver=Solvers.KEY_VALUE_STORE_SOLVER),
        measurement_dimensions=["container_id", "start_ts", "stop_ts"],
    )


def _expected_window_count(container_id: int) -> int:
    span = (
        EXPECTED_CONTAINERS[container_id]["stop_ts"]
        - EXPECTED_CONTAINERS[container_id]["start_ts"]
    )
    return -(-span // WINDOW_LENGTH)  # ceil division


def test_time_window_event_in_report(spark, basic_narrow_db):
    """A TimeWindowEvent (alongside a scoped aggregation) tiles each container into windows."""
    my_report = Report(
        name="time_window_event_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config("time_window_event_test")),
    )

    window_evt = TimeWindowEvent(
        name="ten_sec", window_length=WINDOW_LENGTH, desc="Ten second windows"
    )
    my_report.add_event(window_evt)

    query = my_report.get_db().query
    page = Page(page_number=1)
    my_report.add_page(page)
    page.add_aggregation(
        StatsAggregator(
            name="rpm_stats_per_window",
            input_expressions=[query.channel(channel_name="Engine RPM")],
            channel_names=["Engine RPM"],
            statistics=["min", "max", "mean"],
            event=window_evt,
            desc="Engine RPM stats per window",
        )
    )

    my_report.determine_report()

    event_dfs = my_report.event_dfs
    assert "TIME_WINDOW_EVENT" in event_dfs
    rows = event_dfs["TIME_WINDOW_EVENT"]["changed"].collect()

    total_expected = sum(_expected_window_count(cid) for cid in EXPECTED_CONTAINERS)
    assert len(rows) == total_expected

    for container_id, expected in EXPECTED_CONTAINERS.items():
        windows = sorted(
            ((r.start_ts, r.end_ts) for r in rows if r.container_id == container_id),
            key=lambda w: w[0],
        )
        assert len(windows) == _expected_window_count(container_id)
        # First window starts at the container start.
        assert windows[0][0] == expected["start_ts"]
        # Windows are contiguous: each end equals the next start.
        for (_, end), (nxt_start, _) in zip(windows, windows[1:], strict=False):
            assert end == nxt_start
        # Final window is clamped to the container stop.
        assert windows[-1][1] == expected["stop_ts"]
        # Every window is a valid, non-empty interval.
        assert all(start < end for start, end in windows)
        # Per-window instances are distinct (unlike ContainerEvent's single id).
        instance_ids = [r.event_instance_id for r in rows if r.container_id == container_id]
        assert len(set(instance_ids)) == len(instance_ids)

    dim_rows = my_report.event_metadata_dfs["TIME_WINDOW_EVENT"].collect()
    assert len(dim_rows) == 1
    assert dim_rows[0].event_type == "TIME_WINDOW_EVENT"
    assert dim_rows[0].attributes["window_length"] == str(float(WINDOW_LENGTH))


# Window length (channel-sample time unit, µs) for the aligned-boundary test.
# Per-container sample spans are ~3.9–5.4e9 µs, so 600s (=6e8 µs) yields several
# windows per container, each overlapping RPM samples.
ALIGNED_WINDOW_LENGTH = 600_000_000
_ALIGNED_SCHEMA = "spark_catalog.silver_tw_aligned"


# Customer-shaped time bases for the id-join test, all derived from the basic db's µs epochs.
# Each entry: (transform for channel tstart/tend, transform for container start_ts/stop_ts,
# window length in the samples' unit, SolverConfig.epoch_unit).
#   us:     the native µs epochs (< 2^53, every boundary exactly representable).
#   ns:     ns epochs (~1.5e18, beyond 2^53) with a window that is NOT a multiple of the
#           256 ns double spacing there, so the boundaries round.
#   sec:    seconds as doubles with a fractional window, so the boundaries round.
#   sec_ts: samples as seconds-as-double, container boundaries as TIMESTAMP (converted to
#           epoch seconds via epoch_unit="s").
#   us_ts:  native µs samples, container boundaries as TIMESTAMP (converted to epoch µs via
#           epoch_unit="us", the long path of the conversion).
def _to_seconds(c):
    return c.cast("double") / F.lit(1e6)


def _to_ns(c):
    return c.cast("long") * F.lit(1000)


_TIME_BASES = {
    "us": (lambda c: c, lambda c: c, ALIGNED_WINDOW_LENGTH, None),
    "ns": (_to_ns, _to_ns, 600_000_000_007, None),
    "sec": (_to_seconds, _to_seconds, 600.3, None),
    "sec_ts": (_to_seconds, lambda c: F.timestamp_micros(c.cast("long")), 600.3, "s"),
    "us_ts": (
        lambda c: c,
        lambda c: F.timestamp_micros(c.cast("long")),
        ALIGNED_WINDOW_LENGTH,
        "us",
    ),
}


def _clone_aligned_silver(
    spark, schema: str, to_time_base=lambda c: c, boundaries_to_time_base=None
) -> None:
    """Clone the basic silver tables into *schema* with container_metrics start_ts / stop_ts
    recomputed from each container's channel-sample range (so the container boundaries,
    and thus the windows, share the samples' time base). Channel timestamps are then mapped
    through *to_time_base* and the container boundaries through *boundaries_to_time_base*
    (default: the same transform)."""
    boundaries_to_time_base = boundaries_to_time_base or to_time_base
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    channels = spark.read.table("spark_catalog.silver.channels")
    bounds = channels.groupBy("container_id").agg(
        F.min("tstart").alias("_agg_start"),
        F.max("tend").alias("_agg_stop"),
    )
    container_metrics = spark.read.table("spark_catalog.silver.container_metrics")
    start_type = container_metrics.schema["start_ts"].dataType
    stop_type = container_metrics.schema["stop_ts"].dataType
    aligned_cm = (
        container_metrics.join(bounds, on="container_id", how="left")
        .withColumn("start_ts", F.coalesce("_agg_start", "start_ts").cast(start_type))
        .withColumn("stop_ts", F.coalesce("_agg_stop", "stop_ts").cast(stop_type))
        .drop("_agg_start", "_agg_stop")
        .withColumn("start_ts", boundaries_to_time_base(F.col("start_ts")))
        .withColumn("stop_ts", boundaries_to_time_base(F.col("stop_ts")))
    )
    aligned_cm.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{schema}.container_metrics")
    channels.withColumn("tstart", to_time_base(F.col("tstart"))).withColumn(
        "tend", to_time_base(F.col("tend"))
    ).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{schema}.channels"
    )
    spark.read.table("spark_catalog.silver.channel_metrics").write.format("delta").mode(
        "overwrite"
    ).saveAsTable(f"{schema}.channel_metrics")


@pytest.fixture
def setup_tw_aligned_db(spark, setup_basic_db, request):  # noqa: F811
    """Aligned silver clone in the time base given by ``request.param`` (default ``us``).

    Yields ``(schema, window_length, epoch_unit)``.
    """
    time_base = getattr(request, "param", "us")
    to_time_base, boundaries_to_time_base, window_length, epoch_unit = _TIME_BASES[time_base]
    schema = f"{_ALIGNED_SCHEMA}_{time_base}"
    _clone_aligned_silver(spark, schema, to_time_base, boundaries_to_time_base)
    yield schema, window_length, epoch_unit
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _aligned_config(
    schema: str,
    table_prefix: str,
    epoch_unit=None,
    raw_encoder: RawEncoder | None = None,
    channels_table: str = "channels",
    **extra,
) -> dict:
    """Report config over the aligned clone; a *raw_encoder* switches to ``data_type=RAW``."""
    return dict(
        ImpulseConfig(
            source=Source(
                container_metrics_table=f"{schema}.container_metrics",
                channel_metrics_table=f"{schema}.channel_metrics",
                channels_uri=f"{schema}.{channels_table}",
            ),
            unity_sink=UnitySink(
                catalog="spark_catalog", schema="gold", table_prefix=table_prefix
            ),
            container_filters=ContainerFilters(
                metric_filters=[
                    [
                        MetricFilter(
                            column_name="vehicle_key", comparator=Comparator.EQ, value="Seat_Leon"
                        )
                    ]
                ]
            ),
            query_engine=QueryEngine(
                solver=Solvers.KEY_VALUE_STORE_SOLVER,
                solver_config=SolverConfig(epoch_unit=epoch_unit) if epoch_unit else None,
                data_type=DataType.RAW if raw_encoder else DataType.RLE,
                raw_encoder=raw_encoder,
            ),
            measurement_dimensions=["container_id", "start_ts", "stop_ts"],
            **extra,
        )
    )


def _rpm_stats(report: Report, event: TimeWindowEvent, statistics=("min", "max", "mean")):
    query = report.get_db().query
    return StatsAggregator(
        name="rpm_stats_per_window",
        input_expressions=[query.channel(channel_name="Engine RPM")],
        channel_names=["Engine RPM"],
        statistics=list(statistics),
        event=event,
        desc="Engine RPM stats per window",
    )


def _assert_ids_join(spark, table_prefix: str) -> tuple[set, set]:  # noqa: F811
    """Assert every stats event_instance_id exists in event_instance_fact, with real values.

    Returns ``(stats_event_ids, event_ids)`` for further checks.
    """
    stats_fact = spark.read.table(f"spark_catalog.gold.{table_prefix}_stats_aggregator_fact")
    event_instance_fact = spark.read.table(
        f"spark_catalog.gold.{table_prefix}_event_instance_fact"
    )

    # Real computed values: with aligned boundaries every window overlaps RPM samples,
    # so the windows produce a positive max.
    max_values = [
        r.statistic_value
        for r in stats_fact.filter(F.col("aggregation_label") == "max").collect()
        if r.statistic_value is not None
    ]
    assert len(max_values) > 0
    assert any(v > 0 for v in max_values)

    stats_event_ids = {
        r.event_instance_id
        for r in stats_fact.filter(F.col("event_instance_id").isNotNull())
        .select("event_instance_id")
        .distinct()
        .collect()
    }
    event_ids = {
        r.event_instance_id
        for r in event_instance_fact.select("event_instance_id").distinct().collect()
    }
    assert len(stats_event_ids) > 0
    # Every per-window stats instance must map to a materialized window instance.
    assert stats_event_ids.issubset(
        event_ids
    ), f"stats event_instance_ids not in event_instance_fact: {stats_event_ids - event_ids}"
    return stats_event_ids, event_ids


def _assert_window_stats_match_samples(
    spark, schema: str, table_prefix: str, channels_table: str = "channels"  # noqa: F811
):
    """Each window's RPM min / max equal those of the silver samples overlapping that window,
    and windows without RPM samples carry no value (RPM only covers each container's first
    minute, so most windows are empty).

    The ids hash the window's position, so a position that drifted between the event fact
    and the solve would attach a neighbouring window's values; this pins every stats row to
    the window whose boundaries event_instance_fact stores.
    """
    rpm_channels = (
        spark.read.table(f"{schema}.channel_metrics")
        .filter(F.col("channel_name") == "Engine RPM")
        .select("container_id", "channel_id")
    )
    channels = spark.read.table(f"{schema}.{channels_table}")
    if "tend" not in channels.columns:
        # RAW points: each sample is valid until the next one, the last one only at its own
        # timestamp (the documented raw->interval rule of both encoders).
        by_time = Window.partitionBy("container_id", "channel_id").orderBy("tstart")
        channels = channels.withColumnRenamed("timestamp", "tstart").withColumn(
            "tend", F.coalesce(F.lead("tstart").over(by_time), F.col("tstart"))
        )
    samples = channels.join(rpm_channels, ["container_id", "channel_id"]).select(
        "container_id",
        F.col("tstart").cast("double").alias("tstart"),
        F.col("tend").cast("double").alias("tend"),
        F.col("value").cast("double").alias("value"),
    )
    windows = spark.read.table(f"spark_catalog.gold.{table_prefix}_event_instance_fact")
    expected = (
        windows.join(samples, "container_id")
        .filter((F.col("tstart") < F.col("end_ts")) & (F.col("tend") > F.col("start_ts")))
        .groupBy("container_id", "event_instance_id")
        .agg(F.min("value").alias("expected_min"), F.max("value").alias("expected_max"))
    )
    actual = (
        spark.read.table(f"spark_catalog.gold.{table_prefix}_stats_aggregator_fact")
        .groupBy("container_id", "event_instance_id")
        .pivot("aggregation_label", ["min", "max"])
        .agg(F.first("statistic_value"))
    )
    rows = actual.join(expected, ["container_id", "event_instance_id"], "left").collect()

    def _is_missing(value) -> bool:
        return value is None or math.isnan(value)

    with_samples = [r for r in rows if r.expected_max is not None]
    assert with_samples and len(with_samples) < len(rows)
    mismatches = [
        r
        for r in rows
        if (
            (r["min"], r["max"]) != (r.expected_min, r.expected_max)
            if r.expected_max is not None
            else not (_is_missing(r["min"]) and _is_missing(r["max"]))
        )
    ]
    assert not mismatches, mismatches[:5]


@pytest.mark.parametrize(
    "setup_tw_aligned_db", ["us", "ns", "sec", "sec_ts", "us_ts"], indirect=True
)
def test_time_window_event_aggregation_join(spark, setup_tw_aligned_db):
    """Stats scoped to a TimeWindowEvent yield per-window values whose event_instance_id
    joins to the natively computed event fact, for µs, ns and seconds-as-double time bases,
    and for TIMESTAMP container boundaries converted via epoch_unit."""
    schema, window_length, epoch_unit = setup_tw_aligned_db
    table_prefix = f"time_window_join_test_{schema.removeprefix(_ALIGNED_SCHEMA + '_')}"
    my_report = Report(
        name="time_window_join_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=_aligned_config(schema, table_prefix, epoch_unit=epoch_unit),
    )

    window_evt = TimeWindowEvent(name="ten_min", window_length=window_length)
    my_report.add_event(window_evt)

    page = Page(page_number=1)
    my_report.add_page(page)
    page.add_aggregation(_rpm_stats(my_report, window_evt))

    my_report.determine_report()
    my_report.persist_results()

    _assert_ids_join(spark, table_prefix)
    _assert_window_stats_match_samples(spark, schema, table_prefix)


def _write_raw_channels(spark, schema: str) -> str:  # noqa: F811
    """Write the aligned channels in the raw format (one ``timestamp`` per sample, no
    ``tend``) next to the RLE table, and return the new table's name."""
    table = "channels_raw"
    spark.read.table(f"{schema}.channels").select(
        "container_id", "channel_id", F.col("tstart").alias("timestamp"), "value"
    ).write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{table}")
    return table


@pytest.mark.parametrize("raw_encoder", [RawEncoder.RLE, RawEncoder.INTERVAL])
@pytest.mark.parametrize("setup_tw_aligned_db", ["us", "us_ts"], indirect=True)
def test_time_window_event_aggregation_join_raw(spark, setup_tw_aligned_db, raw_encoder):
    """With data_type=RAW both encoders derive [tstart, tend) from the raw ``timestamp``
    column without changing its unit, so windows over numeric or TIMESTAMP (epoch_unit="us")
    container boundaries line up with the samples exactly as for RLE silver data."""
    schema, window_length, epoch_unit = setup_tw_aligned_db
    channels_table = _write_raw_channels(spark, schema)
    time_base = schema.removeprefix(_ALIGNED_SCHEMA + "_")
    table_prefix = f"time_window_raw_test_{time_base}_{raw_encoder.value.lower()}"
    my_report = Report(
        name="time_window_raw_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=_aligned_config(
            schema,
            table_prefix,
            epoch_unit=epoch_unit,
            raw_encoder=raw_encoder,
            channels_table=channels_table,
        ),
    )

    window_evt = TimeWindowEvent(name="ten_min", window_length=window_length)
    my_report.add_event(window_evt)
    page = Page(page_number=1)
    my_report.add_page(page)
    page.add_aggregation(_rpm_stats(my_report, window_evt))

    my_report.determine_report()
    my_report.persist_results()

    _assert_ids_join(spark, table_prefix)
    _assert_window_stats_match_samples(spark, schema, table_prefix, channels_table)


def test_multiple_time_window_events_coexist(spark, basic_narrow_db):
    """Two TimeWindowEvents with different windows are allowed and both materialize."""
    my_report = Report(
        name="time_window_multi_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config("time_window_multi_test")),
    )

    evt_10s = TimeWindowEvent(name="ten_sec", window_length=WINDOW_LENGTH)
    evt_30s = TimeWindowEvent(name="thirty_sec", window_length=3 * WINDOW_LENGTH)
    my_report.add_event(evt_10s)
    my_report.add_event(evt_30s)

    query = my_report.get_db().query
    page = Page(page_number=1)
    my_report.add_page(page)
    page.add_aggregation(
        StatsAggregator(
            name="rpm_stats",
            input_expressions=[query.channel(channel_name="Engine RPM")],
            channel_names=["Engine RPM"],
            statistics=["mean"],
            event=evt_10s,
            desc="Engine RPM stats per 10s window",
        )
    )

    my_report.determine_report()

    rows = my_report.event_dfs["TIME_WINDOW_EVENT"]["changed"].collect()
    names = {r.event_id for r in rows}
    # Two distinct events (distinct event_ids) share the shared fact table.
    assert names == {evt_10s.get_id(), evt_30s.get_id()}

    # The 10s event produces strictly more windows than the 30s event.
    count_10s = sum(1 for r in rows if r.event_id == evt_10s.get_id())
    count_30s = sum(1 for r in rows if r.event_id == evt_30s.get_id())
    assert count_10s > count_30s > 0

    dim_rows = my_report.event_metadata_dfs["TIME_WINDOW_EVENT"].collect()
    assert {d.event_name for d in dim_rows} == {"ten_sec", "thirty_sec"}


# ---------------------------------------------------------------------------
# Coverage: windows for every filtered container, independent of channel data
# ---------------------------------------------------------------------------
_PARTIAL_SCHEMA = "spark_catalog.silver_tw_partial"
_RPM_CHANNEL_ID = 5


@pytest.fixture
def setup_tw_partial_db(spark, setup_basic_db):  # noqa: F811
    """Basic silver clone where container 3 has no Engine RPM channel (metrics or data)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_PARTIAL_SCHEMA}")
    no_rpm_on_3 = ~((F.col("container_id") == 3) & (F.col("channel_id") == _RPM_CHANNEL_ID))
    spark.read.table("spark_catalog.silver.container_metrics").write.format("delta").mode(
        "overwrite"
    ).saveAsTable(f"{_PARTIAL_SCHEMA}.container_metrics")
    for table in ("channel_metrics", "channels"):
        spark.read.table(f"spark_catalog.silver.{table}").filter(no_rpm_on_3).write.format(
            "delta"
        ).mode("overwrite").saveAsTable(f"{_PARTIAL_SCHEMA}.{table}")
    yield
    spark.sql(f"DROP SCHEMA IF EXISTS {_PARTIAL_SCHEMA} CASCADE")


def _assert_windows_for_all_containers(rows) -> None:
    for container_id in EXPECTED_CONTAINERS:
        count = sum(1 for r in rows if r.container_id == container_id)
        assert count == _expected_window_count(container_id), (
            f"container {container_id}: expected {_expected_window_count(container_id)} "
            f"windows, got {count}"
        )


def test_time_window_event_covers_containers_without_aggregated_channel(
    spark, setup_tw_partial_db
):
    """Windows exist for every filtered container even when the scoped aggregation's
    channel is missing on some of them and the solve is split into single-channel batches.

    Previously the windows came from the channel solve, so container 3 (no Engine RPM)
    got none whenever the window expression landed in the RPM batch."""
    config = _config("time_window_partial_test")
    config.source = Source(
        container_metrics_table=f"{_PARTIAL_SCHEMA}.container_metrics",
        channel_metrics_table=f"{_PARTIAL_SCHEMA}.channel_metrics",
        channels_uri=f"{_PARTIAL_SCHEMA}.channels",
    )
    config.query_engine = QueryEngine(
        solver=Solvers.KEY_VALUE_STORE_SOLVER, max_channels_per_batch=1
    )
    my_report = Report(
        name="time_window_partial_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(config),
    )
    window_evt = TimeWindowEvent(name="ten_sec", window_length=WINDOW_LENGTH)
    my_report.add_event(window_evt)
    page = Page(page_number=1)
    my_report.add_page(page)
    page.add_aggregation(_rpm_stats(my_report, window_evt))

    my_report.determine_report()

    rows = my_report.event_dfs["TIME_WINDOW_EVENT"]["changed"].collect()
    _assert_windows_for_all_containers(rows)


def test_standalone_time_window_event_covers_all_containers(spark, basic_narrow_db):
    """A TimeWindowEvent with no aggregation (nothing to solve) still materializes windows."""
    my_report = Report(
        name="time_window_standalone_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config("time_window_standalone_test")),
    )
    my_report.add_event(TimeWindowEvent(name="ten_sec", window_length=WINDOW_LENGTH))

    my_report.determine_report()

    rows = my_report.event_dfs["TIME_WINDOW_EVENT"]["changed"].collect()
    _assert_windows_for_all_containers(rows)
    assert all(r.start_ts < r.end_ts for r in rows)


# ---------------------------------------------------------------------------
# Incremental: ids still join when the aggregation and the event use different scopes
# ---------------------------------------------------------------------------
def test_time_window_event_ids_join_after_incremental_run(spark, setup_tw_aligned_db):
    """Run 1 (full) on containers 1-2; run 2 (incremental) adds container 3 and changes the
    aggregation's definition. The changed aggregation recomputes over all containers while
    the unchanged event only computes container 3, yet every stats id must still join."""
    schema, window_length, _ = setup_tw_aligned_db
    table_prefix = "time_window_inc_test"
    cm_run_1 = f"{schema}.container_metrics_run_1"
    cm_run_2 = f"{schema}.container_metrics_run_2"
    past = F.lit("2020-01-01 00:00:00").cast("timestamp")
    cm = spark.read.table(f"{schema}.container_metrics")
    cm.filter(F.col("container_id").isin([1, 2])).withColumn("timestamp", past).write.format(
        "delta"
    ).mode("overwrite").saveAsTable(cm_run_1)

    def _run(cm_table: str, is_incremental: bool, statistics) -> None:
        config = _aligned_config(
            schema,
            table_prefix,
            incremental=IncrementalConfig(
                enabled=is_incremental,
                silver_last_modified_column="timestamp",
                gold_last_modified_column="_created_at",
            ),
        )
        config["source"].container_metrics_table = cm_table
        report = Report(
            name="time_window_inc_report",
            spark=spark,
            workspace_client=create_autospec(WorkspaceClient),
            config=config,
        )
        window_evt = TimeWindowEvent(name="ten_min", window_length=window_length)
        report.add_event(window_evt)
        page = Page(page_number=1)
        report.add_page(page)
        page.add_aggregation(_rpm_stats(report, window_evt, statistics))
        report.determine_report()
        report.persist_results()

    _run(cm_run_1, is_incremental=False, statistics=("min", "max", "mean"))

    # Container 3 is new (recent timestamp); 1 and 2 are unchanged.
    cm.withColumn(
        "timestamp", F.when(F.col("container_id") == 3, F.current_timestamp()).otherwise(past)
    ).write.format("delta").mode("overwrite").saveAsTable(cm_run_2)
    # Adding a statistic changes the aggregation's definition hash (event unchanged).
    _run(cm_run_2, is_incremental=True, statistics=("min", "max", "mean", "median"))

    stats_event_ids, _ = _assert_ids_join(spark, table_prefix)

    event_fact = spark.read.table(f"spark_catalog.gold.{table_prefix}_event_instance_fact")
    stats_fact = spark.read.table(f"spark_catalog.gold.{table_prefix}_stats_aggregator_fact")
    assert {r.container_id for r in event_fact.select("container_id").distinct().collect()} == {
        1,
        2,
        3,
    }
    # The changed aggregation was recomputed for all containers (incl. the new one).
    assert {r.container_id for r in stats_fact.select("container_id").distinct().collect()} == {
        1,
        2,
        3,
    }
    assert stats_fact.filter(F.col("aggregation_label") == "median").count() > 0


# ---------------------------------------------------------------------------
# TIMESTAMP container boundaries: epoch_unit is opt-in, required only by TimeWindowEvent
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("setup_tw_aligned_db", ["sec_ts"], indirect=True)
def test_time_window_event_timestamp_boundaries_require_epoch_unit(spark, setup_tw_aligned_db):
    """A TimeWindowEvent over TIMESTAMP boundaries without epoch_unit fails fast and clearly."""
    schema, window_length, _ = setup_tw_aligned_db
    my_report = Report(
        name="time_window_no_unit_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=_aligned_config(schema, "time_window_no_unit_test"),
    )
    my_report.add_event(TimeWindowEvent(name="ten_min", window_length=window_length))

    with pytest.raises(ValueError, match=r"TimeWindowEvent.*epoch_unit"):
        my_report.determine_report()


@pytest.mark.parametrize("epoch_unit", [None, "s"])
@pytest.mark.parametrize("setup_tw_aligned_db", ["sec_ts"], indirect=True)
def test_container_event_timestamp_boundaries(spark, setup_tw_aligned_db, epoch_unit):
    """Backward compatibility: a ContainerEvent over TIMESTAMP boundaries runs without
    epoch_unit (as today) and yields epoch seconds; epoch_unit="s" gives identical values.
    measurement_dimension keeps the TIMESTAMP type either way."""
    schema, _, _ = setup_tw_aligned_db
    table_prefix = f"container_event_ts_test_{epoch_unit or 'unset'}"
    my_report = Report(
        name="container_event_ts_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=_aligned_config(schema, table_prefix, epoch_unit=epoch_unit),
    )
    my_report.add_event(ContainerEvent(name="full_container"))
    my_report.determine_report()
    my_report.persist_results()

    expected = {
        r.container_id: (r.s, r.e)
        for r in spark.read.table(f"{schema}.container_metrics")
        .select(
            "container_id",
            F.col("start_ts").cast("double").alias("s"),
            F.col("stop_ts").cast("double").alias("e"),
        )
        .collect()
    }
    event_fact = spark.read.table(f"spark_catalog.gold.{table_prefix}_event_instance_fact")
    actual = {r.container_id: (r.start_ts, r.end_ts) for r in event_fact.collect()}
    assert actual and all(actual[cid] == expected[cid] for cid in actual), (actual, expected)

    measurement_dim = spark.read.table(f"spark_catalog.gold.{table_prefix}_measurement_dimension")
    assert isinstance(measurement_dim.schema["start_ts"].dataType, T.TimestampType)


@pytest.mark.parametrize("setup_tw_aligned_db", ["sec_ts"], indirect=True)
def test_epoch_unit_change_recomputes_boundary_events(spark, setup_tw_aligned_db):
    """Changing epoch_unit between incremental runs moves the definition hashes of the
    TimeWindowEvent, the ContainerEvent and the aggregation scoped to the windows. They
    recompute over all containers, so the gold tables never mix units."""
    schema, window_length, epoch_unit = setup_tw_aligned_db
    assert epoch_unit == "s"
    table_prefix = "time_window_epoch_unit_test"
    cm_run_1 = f"{schema}.container_metrics_run_1"
    cm_run_2 = f"{schema}.container_metrics_run_2"
    past = F.lit("2020-01-01 00:00:00").cast("timestamp")
    cm = spark.read.table(f"{schema}.container_metrics")
    cm.filter(F.col("container_id").isin([1, 2])).withColumn("timestamp", past).write.format(
        "delta"
    ).mode("overwrite").saveAsTable(cm_run_1)
    # Container 3 is new in run 2 (recent timestamp); 1 and 2 are unchanged.
    cm.withColumn(
        "timestamp", F.when(F.col("container_id") == 3, F.current_timestamp()).otherwise(past)
    ).write.format("delta").mode("overwrite").saveAsTable(cm_run_2)

    def _run(cm_table: str, unit: str, is_incremental: bool):
        config = _aligned_config(
            schema,
            table_prefix,
            epoch_unit=unit,
            incremental=IncrementalConfig(
                enabled=is_incremental,
                silver_last_modified_column="timestamp",
                gold_last_modified_column="_created_at",
            ),
        )
        config["source"].container_metrics_table = cm_table
        report = Report(
            name="time_window_epoch_unit_report",
            spark=spark,
            workspace_client=create_autospec(WorkspaceClient),
            config=config,
        )
        window_evt = TimeWindowEvent(name="ten_min", window_length=window_length)
        container_evt = ContainerEvent(name="full_container")
        report.add_event(window_evt)
        report.add_event(container_evt)
        page = Page(page_number=1)
        report.add_page(page)
        stats = _rpm_stats(report, window_evt)
        page.add_aggregation(stats)
        report.determine_report()
        report.persist_results()
        return report, window_evt, container_evt, stats

    def _event_rows(event_id: int):
        return (
            spark.read.table(f"spark_catalog.gold.{table_prefix}_event_instance_fact")
            .filter(F.col("event_id") == event_id)
            .collect()
        )

    _, _, container_evt, _ = _run(cm_run_1, "s", is_incremental=False)
    starts_in_s = {r.container_id: r.start_ts for r in _event_rows(container_evt.get_id())}
    assert set(starts_in_s) == {1, 2}

    report, window_evt, container_evt, stats = _run(cm_run_2, "ms", is_incremental=True)

    changed_events = {i for ids in report._changed_event_ids.values() for i in ids}
    changed_aggs = {i for ids in report._changed_aggregation_ids.values() for i in ids}
    assert {window_evt.get_id(), container_evt.get_id()} <= changed_events
    assert stats.get_id() in changed_aggs

    # The unchanged containers 1 and 2 were rewritten in ms, not left in seconds.
    container_rows = _event_rows(container_evt.get_id())
    assert {r.container_id for r in container_rows} == {1, 2, 3}
    for r in container_rows:
        if r.container_id in starts_in_s:
            assert r.start_ts == pytest.approx(starts_in_s[r.container_id] * 1000)
    starts_in_ms = {r.container_id: r.start_ts for r in container_rows}

    # Every window tiles the container's ms span: none is left over from the seconds run.
    window_rows = _event_rows(window_evt.get_id())
    assert {r.container_id for r in window_rows} == {1, 2, 3}
    first_window = {}
    for r in window_rows:
        first_window[r.container_id] = min(
            first_window.get(r.container_id, r.start_ts), r.start_ts
        )
    assert first_window == starts_in_ms
