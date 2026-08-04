"""End-to-end report test for custom statistics (cross-channel and per-channel)."""

import math
import os
from unittest.mock import create_autospec

import numpy as np
import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from impulse_query_engine.analyze.query.aggregations.custom_statistic import (
    CrossChannelStatistic,
    PerChannelStatistic,
)
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent


def _value_spread(series, t_start, t_end):
    """Cross-channel: max - min over the values of all series."""
    values = [v for s in series for v in s.values if not np.isnan(v)]
    if not values:
        return [float("nan")]
    return [float(max(values) - min(values))]


def _count_above(series, t_start, t_end, threshold=0.0):
    """Cross-channel: number of samples above a configurable threshold."""
    return [float(sum((s.values > threshold).sum() for s in series))]


def _rms(series, t_start, t_end):
    """Per-channel: root mean square of the series values."""
    if len(series) == 0:
        return [float("nan")]
    return [float(np.sqrt(np.nanmean(series.values**2)))]


def _percentiles(series, t_start, t_end):
    """Per-channel multi-output: returns (p50, p90) of the series values."""
    if len(series) == 0:
        return (float("nan"), float("nan"))
    return (
        float(np.nanpercentile(series.values, 50)),
        float(np.nanpercentile(series.values, 90)),
    )


def _min_max(series, t_start, t_end):
    """Cross-channel multi-output: returns [min, max] over all series values."""
    values = [v for s in series for v in s.values if not np.isnan(v)]
    if not values:
        return [float("nan"), float("nan")]
    return [float(min(values)), float(max(values))]


def test_custom_statistics_report(spark):
    """Custom statistics flow end-to-end into the stats aggregator fact rows."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="custom_stats_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")

    rpm_event = BasicEvent(name="rpm_event", expr=c1 > 0, desc="Engine RPM > 0")
    my_report.add_event(rpm_event)

    page = Page(page_number=1)
    my_report.add_page(page)

    stats = StatsAggregator(
        name="custom_stats",
        input_expressions=[c1, c2],
        channel_names=["Engine RPM", "Vehicle Speed"],
        statistics=["min", "max"],
        event=rpm_event,
        cross_channel_custom_statistics=[
            CrossChannelStatistic(
                func=_value_spread,
                aggregation_labels=["spread"],
                inputs=["Engine RPM", "Vehicle Speed"],
                channel_name="rpm_speed_spread",
            ),
            CrossChannelStatistic(
                func=_count_above, aggregation_labels=["count"], params={"threshold": 0.0}
            ),
        ],
        per_channel_custom_statistics=[PerChannelStatistic(func=_rms, aggregation_labels=["rms"])],
    )
    page.add_aggregation(stats)

    my_report.determine_report()

    assert "STATS_AGGREGATOR" in my_report.aggregation_dfs
    stats_df = my_report.aggregation_dfs["STATS_AGGREGATOR"]["changed"]
    assert stats_df.count() > 0

    rows = stats_df.collect()
    by_label = {}
    for row in rows:
        by_label.setdefault(row["aggregation_label"], []).append(row)

    # cross-channel rows: descriptor channel_name and stat-name default
    assert {row["channel_name"] for row in by_label["spread"]} == {"rpm_speed_spread"}
    assert {row["channel_name"] for row in by_label["count"]} == {"count"}
    for row in by_label["spread"]:
        assert not math.isnan(row["statistic_value"])
        assert row["statistic_value"] > 0
    for row in by_label["count"]:
        assert row["statistic_value"] > 0

    # per-channel custom rows land under the real channel names, like built-ins
    assert {row["channel_name"] for row in by_label["rms"]} == {"Engine RPM", "Vehicle Speed"}
    assert {row["channel_name"] for row in by_label["min"]} == {"Engine RPM", "Vehicle Speed"}

    # rms is pivotable alongside the built-ins and lies within [min, max]
    # (non-negative sample values)
    pivoted = (
        stats_df.groupBy("container_id", "channel_name", "event_instance_id")
        .pivot("aggregation_label", ["min", "max", "rms"])
        .agg(F.first("statistic_value"))
        .where(F.col("rms").isNotNull() & ~F.isnan("rms"))
        .collect()
    )
    assert len(pivoted) > 0
    for row in pivoted:
        assert row["min"] - 1e-9 <= row["rms"] <= row["max"] + 1e-9

    # spread dominates each channel's own value range within the same interval
    per_channel_range = {}
    for row in pivoted:
        key = (row["container_id"], row["event_instance_id"])
        value_range = row["max"] - row["min"]
        per_channel_range[key] = max(per_channel_range.get(key, 0.0), value_range)
    spread_by_instance = {
        (row["container_id"], row["event_instance_id"]): row["statistic_value"]
        for row in by_label["spread"]
    }
    joinable = set(spread_by_instance) & set(per_channel_range)
    assert joinable, "cross-channel rows must join per-channel rows on event_instance_id"
    for key in joinable:
        assert spread_by_instance[key] >= per_channel_range[key] - 1e-9

    # metadata: custom statistic names are documented in the dimension row
    metadata_df = my_report.aggregation_metadata_dfs["STATS_AGGREGATOR"]
    metadata = metadata_df.collect()[0]
    assert metadata["statistics"] == ["min", "max", "rms", "spread", "count"]


def test_multi_output_custom_statistics_report(spark):
    """Multi-output custom statistics fan out to several fact rows end-to-end."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="multi_output_stats_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")

    rpm_event = BasicEvent(name="rpm_event", expr=c1 > 0, desc="Engine RPM > 0")
    my_report.add_event(rpm_event)

    page = Page(page_number=1)
    my_report.add_page(page)

    stats = StatsAggregator(
        name="multi_output_stats",
        input_expressions=[c1, c2],
        channel_names=["Engine RPM", "Vehicle Speed"],
        statistics=["min", "max"],
        event=rpm_event,
        per_channel_custom_statistics=[
            PerChannelStatistic(func=_percentiles, aggregation_labels=["p50", "p90"]),
        ],
        cross_channel_custom_statistics=[
            CrossChannelStatistic(
                func=_min_max, aggregation_labels=["lo", "hi"], channel_name="rpm_speed_bounds"
            ),
        ],
    )
    page.add_aggregation(stats)

    my_report.determine_report()

    stats_df = my_report.aggregation_dfs["STATS_AGGREGATOR"]["changed"]
    rows = stats_df.collect()
    by_label = {}
    for row in rows:
        by_label.setdefault(row["aggregation_label"], []).append(row)

    # per-channel multi-output labels land under the real channel names
    assert {row["channel_name"] for row in by_label["p50"]} == {"Engine RPM", "Vehicle Speed"}
    assert {row["channel_name"] for row in by_label["p90"]} == {"Engine RPM", "Vehicle Speed"}

    # cross-channel multi-output labels share the descriptor's channel_name
    assert {row["channel_name"] for row in by_label["lo"]} == {"rpm_speed_bounds"}
    assert {row["channel_name"] for row in by_label["hi"]} == {"rpm_speed_bounds"}

    # values are finite and hi >= lo for each interval
    for label in ("p50", "p90", "lo", "hi"):
        for row in by_label[label]:
            assert not math.isnan(row["statistic_value"])

    # per interval instance, hi >= lo
    lo_by_instance = {row["event_instance_id"]: row["statistic_value"] for row in by_label["lo"]}
    hi_by_instance = {row["event_instance_id"]: row["statistic_value"] for row in by_label["hi"]}
    for instance_id, lo in lo_by_instance.items():
        assert hi_by_instance[instance_id] >= lo - 1e-9

    # the label set is documented in the dimension row
    metadata = my_report.aggregation_metadata_dfs["STATS_AGGREGATOR"].collect()[0]
    assert metadata["statistics"] == ["min", "max", "p50", "p90", "lo", "hi"]
