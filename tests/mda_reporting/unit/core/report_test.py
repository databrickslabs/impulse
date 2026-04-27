import os
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from mda_reporting.aggregations.aggregation_types import AggregationType
from mda_reporting.aggregations.histogram import HistogramDuration
from mda_reporting.aggregations.histogram2d import Histogram2DDuration
from mda_reporting.aggregations.stats_aggregator import StatsAggregator
from mda_reporting.core.page import Page
from mda_reporting.core.report import Report
from mda_reporting.events.basic_event import BasicEvent
from mda_reporting.events.event_types import EventType
from mda_reporting.persist.report_storage import SinkConfig

DUMMY_CONFIG = {
    "source": {
        "container_metrics_table": "mda_demo.silver.container_metric",
        "channel_metrics_table": "mda_demo.silver.channel_metric",
        "channels_uri": "mda_demo.silver.channel_data",
    },
    "unity_sink": {
        "catalog": "test_catalog",
        "schema": "test_schema",
        "table_prefix": "test_prefix",
    },
    "container_filters": {
        "metric_filters": [
            [
                {"column_name": "uut_id", "comparator": "==", "value": "123"},
                {
                    "column_name": "start_ts",
                    "comparator": ">=",
                    "value": "2025-04-27T05:20:54.000Z",
                },
                {"column_name": "end_ts", "comparator": "<=", "value": "2025-04-27T05:21:00.000Z"},
            ]
        ]
    },
}

DUMMY_KEY_VALUE_STORE_CONFIG = {
    "source": {
        "container_tags_table": "mda_demo.silver.concept_entities",
        "container_metrics_table": "mda_demo.silver.container_metric",
        "channel_metrics_table": "mda_demo.silver.channel_metric",
        "channels_uri": "mda_demo.silver.channel_data",
    },
    "unity_sink": {
        "catalog": "test_catalog",
        "schema": "test_schema",
        "table_prefix": "test_prefix",
    },
    "query_engine": {
        "solver": "KeyValueStoreSolver",
        "project_id": "test_project",
    },
}

DUMMY_KEY_VALUE_STORE_CONFIG_WITH_SOLVER_CONFIG = {
    "source": {
        "container_tags_table": "mda_demo.silver.concept_entities",
        "container_metrics_table": "mda_demo.silver.container_metric",
        "channel_metrics_table": "mda_demo.silver.channel_metric",
        "channels_uri": "mda_demo.silver.channel_data",
    },
    "unity_sink": {
        "catalog": "test_catalog",
        "schema": "test_schema",
        "table_prefix": "test_prefix",
    },
    "query_engine": {
        "solver": "KeyValueStoreSolver",
        "project_id": "test_project",
        "solver_config": {
            "container_id_col": "measurement_id",
            "channel_id_cols": ["measurement_id", "signal_id"],
            "channel_data_mapping": {
                "tstart": "t_start",
                "tend": "t_stop",
                "value": "val",
            },
            "container_meta_data_mapping": {
                "project_id": "project",
            },
        },
    },
}

DUMMY_KEY_VALUE_STORE_CONFIG = {
    "source": {
        "container_tags_table": "mda_demo.silver.concept_entities",
        "container_metrics_table": "mda_demo.silver.container_metric",
        "channel_metrics_table": "mda_demo.silver.channel_metric",
        "channels_uri": "mda_demo.silver.channel_data",
    },
    "unity_sink": {
        "catalog": "test_catalog",
        "schema": "test_schema",
        "table_prefix": "test_prefix",
    },
    "query_engine": {
        "solver": "KeyValueStoreSolver",
        "project_id": "test_project",
    },
}

DUMMY_KEY_VALUE_STORE_CONFIG_WITH_SOLVER_CONFIG = {
    "source": {
        "container_tags_table": "mda_demo.silver.concept_entities",
        "container_metrics_table": "mda_demo.silver.container_metric",
        "channel_metrics_table": "mda_demo.silver.channel_metric",
        "channels_uri": "mda_demo.silver.channel_data",
    },
    "unity_sink": {
        "catalog": "test_catalog",
        "schema": "test_schema",
        "table_prefix": "test_prefix",
    },
    "query_engine": {
        "solver": "KeyValueStoreSolver",
        "project_id": "test_project",
        "solver_config": {
            "container_id_col": "measurement_id",
            "channel_id_cols": ["measurement_id", "signal_id"],
            "channel_data_mapping": {
                "tstart": "t_start",
                "tend": "t_stop",
                "value": "val",
            },
            "container_meta_data_mapping": {
                "project_id": "project",
            },
        },
    },
}


def test_report_init():
    """Test Report initialization"""
    report = Report(
        name="test_report",
        spark=None,
        workspace_client=create_autospec(WorkspaceClient),
        config=DUMMY_CONFIG,
    )

    assert report.name == "test_report"
    assert report.pages == []
    assert report.aggregation_dfs == {}


def test_set_config():
    """Test setting config with config path"""

    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    report: Report = Report(
        name="my_report",
        spark=None,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    # Verify config was loaded and is an MdaConfig instance
    report_config = report.get_sink_config()

    assert isinstance(report_config, SinkConfig)
    assert report_config is not None

    assert "evaluation" == report_config.table_prefix

    assert (
        "spark_catalog.gold.evaluation_histogram_fact"
        == report_config.get_output_uri_fact_table(AggregationType.HISTOGRAM)
    )
    assert (
        "spark_catalog.gold.evaluation_event_instance_fact"
        == report_config.get_output_uri_fact_table(EventType.BASIC_EVENT)
    )

    assert (
        "spark_catalog.gold.evaluation_histogram_dimension"
        == report_config.get_output_uri_dimension_table(AggregationType.HISTOGRAM)
    )
    assert (
        "spark_catalog.gold.evaluation_event_dimension"
        == report_config.get_output_uri_dimension_table(EventType.BASIC_EVENT)
    )


def test_add_page():
    """Test adding a page to report"""
    report: Report = Report(
        name="my_report",
        spark=None,
        workspace_client=create_autospec(WorkspaceClient),
        config=DUMMY_CONFIG,
    )
    page = Page(page_number=1)

    report.add_page(page)

    assert len(report.pages) == 1
    assert report.pages[0] == page


def test_add_multiple_pages():
    """Test adding multiple pages to report"""
    report: Report = Report(
        name="my_report",
        spark=None,
        workspace_client=create_autospec(WorkspaceClient),
        config=DUMMY_CONFIG,
    )
    page1 = Page(page_number=1)
    page2 = Page(page_number=2)
    page3 = Page(page_number=3)

    report.add_page(page1)
    report.add_page(page2)
    report.add_page(page3)

    assert len(report.pages) == 3
    assert report.pages[0] == page1
    assert report.pages[1] == page2
    assert report.pages[2] == page3


def test_create_solver_key_value_store_default_config():
    """KeyValueStoreSolver created without solver_config uses default SolverConfig."""
    from mda_query_engine.analyze.query.solvers.key_value_store_solver import (
        KeyValueStoreSolver,
    )

    report = Report(
        name="test_report",
        spark=None,
        workspace_client=create_autospec(WorkspaceClient),
        config=DUMMY_KEY_VALUE_STORE_CONFIG,
    )
    solver = report.get_solver()

    assert isinstance(solver, KeyValueStoreSolver)
    assert solver.config.container_id_col == "container_id"
    assert solver.config.project_id_col == "project_id"
    assert solver.config.value_col == "value"


def test_create_solver_key_value_store_with_solver_config():
    """KeyValueStoreSolver created with solver_config uses provided column mappings."""
    from mda_query_engine.analyze.query.solvers.key_value_store_solver import (
        KeyValueStoreSolver,
    )

    report = Report(
        name="test_report",
        spark=None,
        workspace_client=create_autospec(WorkspaceClient),
        config=DUMMY_KEY_VALUE_STORE_CONFIG_WITH_SOLVER_CONFIG,
    )
    solver = report.get_solver()

    assert isinstance(solver, KeyValueStoreSolver)
    assert solver.config.container_id_col == "measurement_id"
    assert solver.config.channel_id_cols == ["measurement_id", "signal_id"]
    assert solver.config.tstart_col == "t_start"
    assert solver.config.tend_col == "t_stop"
    assert solver.config.value_col == "val"
    assert solver.config.project_id_col == "project"


class TestValidateAggregationEvents:
    """Tests for _validate_aggregation_events method."""

    def test_statistics_event_not_registered_raises_error(self):
        """Test that using an event in Statistics without registering it raises ValueError."""
        report: Report = Report(
            name="my_report",
            spark=None,
            workspace_client=create_autospec(WorkspaceClient),
            config=DUMMY_CONFIG,
        )

        # Create a time series expression and event
        ts_expr = TimeSeriesSelector(TagSelector("name") == "test_signal")
        event = BasicEvent(name="test_event", expr=ts_expr > 0)

        # Create StatsAggregator with an event but don't add the event to the report
        stats = StatsAggregator(
            name="test_stats",
            input_expressions=[ts_expr],
            channel_names=["test_signal"],
            statistics=["min", "max"],
            event=event,
        )

        page = Page(page_number=1)
        page.add_aggregation(stats)
        report.add_page(page)

        # Should raise ValueError because event is not registered
        with pytest.raises(ValueError) as exc_info:
            report._validate_aggregation_events()

        assert "test_event" in str(exc_info.value)
        assert "test_stats" in str(exc_info.value)
        assert "not added to the report" in str(exc_info.value)

    def test_histogram_event_not_registered_raises_error(self):
        """Test that using an event in Histogram without registering it raises ValueError."""
        report: Report = Report(
            name="my_report",
            spark=None,
            workspace_client=create_autospec(WorkspaceClient),
            config=DUMMY_CONFIG,
        )

        # Create a time series expression and event
        ts_expr = TimeSeriesSelector(TagSelector("name") == "test_signal")
        event = BasicEvent(name="histogram_event", expr=ts_expr > 0)

        # Create Histogram with an event but don't add the event to the report
        hist = HistogramDuration(
            name="test_histogram",
            base_expr=ts_expr,
            bins=[0.0, 100.0, 200.0],
            event=event,
        )

        page = Page(page_number=1)
        page.add_aggregation(hist)
        report.add_page(page)

        # Should raise ValueError because event is not registered
        with pytest.raises(ValueError) as exc_info:
            report._validate_aggregation_events()

        assert "histogram_event" in str(exc_info.value)
        assert "test_histogram" in str(exc_info.value)

    def test_histogram2d_event_not_registered_raises_error(self):
        """Test that using an event in Histogram2D without registering it raises ValueError."""
        report: Report = Report(
            name="my_report",
            spark=None,
            workspace_client=create_autospec(WorkspaceClient),
            config=DUMMY_CONFIG,
        )

        # Create time series expressions and event
        x_expr = TimeSeriesSelector(TagSelector("name") == "x_signal")
        y_expr = TimeSeriesSelector(TagSelector("name") == "y_signal")
        event = BasicEvent(name="hist2d_event", expr=x_expr > 0)

        # Create Histogram2D with an event but don't add the event to the report
        hist2d = Histogram2DDuration(
            name="test_histogram2d",
            x_expr=x_expr,
            y_expr=y_expr,
            x_bins=[0.0, 100.0],
            y_bins=[0.0, 50.0],
            event=event,
        )

        page = Page(page_number=1)
        page.add_aggregation(hist2d)
        report.add_page(page)

        # Should raise ValueError because event is not registered
        with pytest.raises(ValueError) as exc_info:
            report._validate_aggregation_events()

        assert "hist2d_event" in str(exc_info.value)
        assert "test_histogram2d" in str(exc_info.value)

    def test_registered_event_passes_validation(self):
        """Test that properly registered events pass validation."""
        report: Report = Report(
            name="my_report",
            spark=None,
            workspace_client=create_autospec(WorkspaceClient),
            config=DUMMY_CONFIG,
        )

        # Create a time series expression and event
        ts_expr = TimeSeriesSelector(TagSelector("name") == "test_signal")
        event = BasicEvent(name="registered_event", expr=ts_expr > 0)

        # Add the event to the report
        report.add_event(event)

        # Create StatsAggregator with the registered event
        stats = StatsAggregator(
            name="test_stats",
            input_expressions=[ts_expr],
            channel_names=["test_signal"],
            statistics=["min", "max"],
            event=event,
        )

        page = Page(page_number=1)
        page.add_aggregation(stats)
        report.add_page(page)

        # Should not raise any error
        report._validate_aggregation_events()

    def test_histogram_without_event_passes_validation(self):
        """Test that Histogram without an event passes validation."""
        report: Report = Report(
            name="my_report",
            spark=None,
            workspace_client=create_autospec(WorkspaceClient),
            config=DUMMY_CONFIG,
        )

        # Create a time series expression
        ts_expr = TimeSeriesSelector(TagSelector("name") == "test_signal")

        # Create Histogram without an event
        hist = HistogramDuration(
            name="test_histogram",
            base_expr=ts_expr,
            bins=[0.0, 100.0, 200.0],
        )

        page = Page(page_number=1)
        page.add_aggregation(hist)
        report.add_page(page)

        # Should not raise any error
        report._validate_aggregation_events()

    def test_multiple_aggregations_with_unregistered_event_lists_all(self):
        """Test that error message lists all aggregations with unregistered events."""
        report: Report = Report(
            name="my_report",
            spark=None,
            workspace_client=create_autospec(WorkspaceClient),
            config=DUMMY_CONFIG,
        )

        # Create expressions and events
        ts_expr1 = TimeSeriesSelector(TagSelector("name") == "signal1")
        ts_expr2 = TimeSeriesSelector(TagSelector("name") == "signal2")
        event1 = BasicEvent(name="event1", expr=ts_expr1 > 0)
        event2 = BasicEvent(name="event2", expr=ts_expr2 > 0)

        # Create two StatsAggregators with different unregistered events
        stats1 = StatsAggregator(
            name="stats1",
            input_expressions=[ts_expr1],
            channel_names=["signal1"],
            statistics=["min"],
            event=event1,
        )
        stats2 = StatsAggregator(
            name="stats2",
            input_expressions=[ts_expr2],
            channel_names=["signal2"],
            statistics=["max"],
            event=event2,
        )

        page = Page(page_number=1)
        page.add_aggregation(stats1)
        page.add_aggregation(stats2)
        report.add_page(page)

        # Should raise ValueError listing both issues
        with pytest.raises(ValueError) as exc_info:
            report._validate_aggregation_events()

        error_msg = str(exc_info.value)
        assert "event1" in error_msg
        assert "event2" in error_msg
        assert "stats1" in error_msg
        assert "stats2" in error_msg

    def test_same_event_used_in_multiple_aggregations(self):
        """Test that the same event can be used in multiple aggregations when registered."""
        report: Report = Report(
            name="my_report",
            spark=None,
            workspace_client=create_autospec(WorkspaceClient),
            config=DUMMY_CONFIG,
        )

        # Create expressions and shared event
        ts_expr = TimeSeriesSelector(TagSelector("name") == "test_signal")
        shared_event = BasicEvent(name="shared_event", expr=ts_expr > 0)

        # Register the event
        report.add_event(shared_event)

        # Create multiple aggregations using the same event
        stats = StatsAggregator(
            name="test_stats",
            input_expressions=[ts_expr],
            channel_names=["test_signal"],
            statistics=["min", "max"],
            event=shared_event,
        )
        hist = HistogramDuration(
            name="test_histogram",
            base_expr=ts_expr,
            bins=[0.0, 100.0],
            event=shared_event,
        )

        page = Page(page_number=1)
        page.add_aggregation(stats)
        page.add_aggregation(hist)
        report.add_page(page)

        # Should not raise any error
        report._validate_aggregation_events()
