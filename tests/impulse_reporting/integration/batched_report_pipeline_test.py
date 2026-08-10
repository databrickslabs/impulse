"""Integration coverage for the centralized batched report-solving flow."""

from __future__ import annotations

import types
from collections import Counter
from unittest.mock import create_autospec

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

import impulse_reporting.core.report_utils as report_utils_module
from impulse_reporting.aggregations.histogram import Histogram, HistogramDuration
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.config.config_parser import (
    ImpulseConfig,
    QueryEngine,
    Solvers,
    Source,
    UnitySink,
)
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent


def _build_batched_report(spark: SparkSession) -> tuple[Report, dict[str, object]]:
    """Create a report that exercises events, histograms, and stats aggregation."""
    config = ImpulseConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver.container_metrics",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(
            catalog="spark_catalog",
            schema="gold",
            table_prefix="evaluation",
        ),
        query_engine=QueryEngine(
            solver=Solvers.KEY_VALUE_STORE_SOLVER,
            batch_size=1,
        ),
    )

    report = Report(
        name="batched_pipeline_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(config),
    )

    query = report.get_db().query
    engine_rpm = query.channel(channel_name="Engine RPM")
    vehicle_speed = query.channel(channel_name="Vehicle Speed Sensor")

    moving_vehicle_event = BasicEvent(
        name="moving_vehicle_event",
        expr=vehicle_speed > 1,
        desc="Vehicle speed above 1 km/h",
    )
    engine_running_event = BasicEvent(
        name="engine_running_event",
        expr=engine_rpm > 500,
        desc="Engine RPM above idle",
    )

    report.add_event(moving_vehicle_event)
    report.add_event(engine_running_event)

    page = Page(page_number=1)
    report.add_page(page)

    rpm_histogram = HistogramDuration(
        name="engine_rpm_hist_batched",
        base_expr=engine_rpm,
        bins=[float(i) for i in range(0, 8000, 250)],
    )
    page.add_aggregation(rpm_histogram)

    speed_rpm_stats = StatsAggregator(
        name="batched_speed_rpm_stats",
        input_expressions=[engine_rpm, vehicle_speed],
        channel_names=["Engine RPM", "Vehicle Speed Sensor"],
        statistics=["min", "max", "mean"],
    )
    page.add_aggregation(speed_rpm_stats)

    tracked_expressions = {
        "moving_vehicle_event": moving_vehicle_event.get_expression(),
        "engine_running_event": engine_running_event.get_expression(),
        "engine_rpm_hist_batched": rpm_histogram.get_expression(),
        "batched_speed_rpm_stats": speed_rpm_stats.get_expression(),
    }
    return report, tracked_expressions


def _instrument_expression_selectors(
    tracked_expressions: dict[str, object],
    selector_calls: Counter,
) -> None:
    """Wrap get_selectors on each tracked expression and count invocations."""
    for label, expr in tracked_expressions.items():
        original_get_selectors = expr.get_selectors

        def _wrapped(self, _label=label, _original=original_get_selectors):
            selector_calls[_label] += 1
            return _original()

        expr.get_selectors = types.MethodType(_wrapped, expr)


def test_batched_pipeline_orchestrates_real_report_flow(spark, monkeypatch):
    instrumentation = {
        "create_sink_calls": 0,
        "cleanup_calls": 0,
        "solve_calls": [],
        "build_batches": [],
        "basic_event_calls": [],
        "histogram_calls": [],
        "stats_calls": [],
    }
    selector_calls: Counter = Counter()

    original_create_sink = Report.create_sink
    original_cleanup_temp_tables = Report._cleanup_temp_tables
    original_solve_expressions_batched = Report._solve_expressions_batched
    original_build_batches = report_utils_module.build_batches
    original_basic_event_determine_events = BasicEvent.determine_events.__func__
    original_histogram_determine_aggregations = Histogram.determine_aggregations.__func__
    original_stats_determine_aggregations = StatsAggregator.determine_aggregations.__func__

    def _create_sink_wrapper(config):
        instrumentation["create_sink_calls"] += 1
        return original_create_sink(config)

    def _cleanup_temp_tables_wrapper(self):
        instrumentation["cleanup_calls"] += 1
        return original_cleanup_temp_tables(self)

    def _build_batches_wrapper(expressions, batch_size):
        batches = original_build_batches(expressions, batch_size)
        instrumentation["build_batches"].append(
            {
                "batch_size": batch_size,
                "expression_count": len(expressions),
                "batch_count": len(batches),
                "batch_aliases": [
                    [getattr(expr, "_alias", expr.__class__.__name__) for expr in batch]
                    for batch in batches
                ],
            }
        )
        return batches

    def _solve_expressions_batched_wrapper(
        self,
        expressions,
        pre_filtered_containers_df=None,
    ):
        call_info = {
            "expression_count": len(expressions),
            "aliases": [getattr(expr, "_alias", expr.__class__.__name__) for expr in expressions],
            "has_pre_filtered_containers": pre_filtered_containers_df is not None,
        }
        result = original_solve_expressions_batched(
            self,
            expressions,
            pre_filtered_containers_df=pre_filtered_containers_df,
        )
        call_info["returned_none"] = result is None
        instrumentation["solve_calls"].append(call_info)
        return result

    def _basic_event_wrapper(
        cls,
        spark_session,
        events,
        *,
        solved_df=None,
        query=None,
        solver=None,
        pre_filtered_containers_df=None,
    ):
        instrumentation["basic_event_calls"].append(
            {
                "event_names": [event.get_name() for event in events],
                "has_solved_df": solved_df is not None,
            }
        )
        return original_basic_event_determine_events(
            cls,
            spark_session,
            events,
            solved_df=solved_df,
            query=query,
            solver=solver,
            pre_filtered_containers_df=pre_filtered_containers_df,
        )

    def _histogram_wrapper(
        cls,
        spark_session,
        aggregations,
        *,
        solved_df=None,
        query=None,
        solver=None,
        pre_filtered_containers_df=None,
    ):
        instrumentation["histogram_calls"].append(
            {
                "aggregation_names": [aggregation.get_name() for aggregation in aggregations],
                "has_solved_df": solved_df is not None,
            }
        )
        return original_histogram_determine_aggregations(
            cls,
            spark_session,
            aggregations,
            solved_df=solved_df,
            query=query,
            solver=solver,
            pre_filtered_containers_df=pre_filtered_containers_df,
        )

    def _stats_wrapper(
        cls,
        spark_session,
        aggregations,
        *,
        solved_df=None,
        query=None,
        solver=None,
        pre_filtered_containers_df=None,
    ):
        instrumentation["stats_calls"].append(
            {
                "aggregation_names": [aggregation.get_name() for aggregation in aggregations],
                "has_solved_df": solved_df is not None,
            }
        )
        return original_stats_determine_aggregations(
            cls,
            spark_session,
            aggregations,
            solved_df=solved_df,
            query=query,
            solver=solver,
            pre_filtered_containers_df=pre_filtered_containers_df,
        )

    monkeypatch.setattr(Report, "create_sink", staticmethod(_create_sink_wrapper))
    monkeypatch.setattr(Report, "_cleanup_temp_tables", _cleanup_temp_tables_wrapper)
    monkeypatch.setattr(Report, "_solve_expressions_batched", _solve_expressions_batched_wrapper)
    monkeypatch.setattr(report_utils_module, "build_batches", _build_batches_wrapper)
    monkeypatch.setattr(BasicEvent, "determine_events", classmethod(_basic_event_wrapper))
    monkeypatch.setattr(Histogram, "determine_aggregations", classmethod(_histogram_wrapper))
    monkeypatch.setattr(
        StatsAggregator,
        "determine_aggregations",
        classmethod(_stats_wrapper),
    )

    report, tracked_expressions = _build_batched_report(spark)
    _instrument_expression_selectors(tracked_expressions, selector_calls)

    spark.sql("DROP TABLE IF EXISTS spark_catalog.gold.__impulse_temp_stale_batch")
    spark.sql(
        "CREATE TABLE spark_catalog.gold.__impulse_temp_stale_batch USING DELTA AS "
        "SELECT 1 AS stale_id"
    )

    report.determine_report()

    temp_tables = {
        row.tableName
        for row in spark.sql("SHOW TABLES IN spark_catalog.gold LIKE '__impulse_temp_*'").collect()
    }

    assert instrumentation["create_sink_calls"] == 1
    assert report._has_sink is True

    assert instrumentation["cleanup_calls"] == 1
    assert "__impulse_temp_stale_batch" not in temp_tables
    assert temp_tables

    assert instrumentation["build_batches"]
    assert instrumentation["build_batches"][0]["batch_size"] == 1
    assert instrumentation["build_batches"][0]["batch_count"] >= 2

    assert len(instrumentation["solve_calls"]) == 2
    assert instrumentation["solve_calls"][0]["expression_count"] == len(tracked_expressions)
    assert instrumentation["solve_calls"][0]["returned_none"] is False
    assert instrumentation["solve_calls"][1]["expression_count"] == 0
    assert instrumentation["solve_calls"][1]["returned_none"] is True

    assert all(selector_calls[label] > 0 for label in tracked_expressions)

    assert len(instrumentation["basic_event_calls"]) == 1
    assert instrumentation["basic_event_calls"][0]["has_solved_df"] is True
    assert len(instrumentation["histogram_calls"]) == 1
    assert instrumentation["histogram_calls"][0]["has_solved_df"] is True
    assert len(instrumentation["stats_calls"]) == 1
    assert instrumentation["stats_calls"][0]["has_solved_df"] is True

    assert report.event_dfs["BASIC_EVENT"]["changed"].count() > 0
    assert report.event_metadata_dfs["BASIC_EVENT"].count() == 2

    histogram_df = report.aggregation_dfs["HISTOGRAM"]["changed"]
    stats_df = report.aggregation_dfs["STATS_AGGREGATOR"]["changed"]

    assert histogram_df.filter(F.col("hist_value") > 0).count() > 0
    assert stats_df.count() > 0
    assert report.aggregation_metadata_dfs["HISTOGRAM"].count() == 1
    assert report.aggregation_metadata_dfs["STATS_AGGREGATOR"].count() == 1
