import os

from mda_reporting.aggregations.aggregation_types import AggregationType
from mda_reporting.core.page import Page
from mda_reporting.core.report import Report
from mda_reporting.events.event_types import EventType
from mda_reporting.persist.report_storage import SinkConfig

DUMMY_CONFIG = {
    "source": {
        "container_metrics_table": "avl_databricks_mvp.silver.container_metric",
        "channel_metrics_table": "avl_databricks_mvp.silver.channel_metric",
        "channels_uri": "avl_databricks_mvp.silver.channel_data",
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
                {
                    "column_name": "end_ts",
                    "comparator": "<=",
                    "value": "2025-04-27T05:21:00.000Z",
                },
            ]
        ]
    },
}

DUMMY_KEY_VALUE_STORE_CONFIG = {
    "source": {
        "container_tags_table": "avl_databricks_mvp.silver.concept_entities",
        "container_metrics_table": "avl_databricks_mvp.silver.container_metric",
        "channel_metrics_table": "avl_databricks_mvp.silver.channel_metric",
        "channels_uri": "avl_databricks_mvp.silver.channel_data",
    },
    "unity_sink": {
        "catalog": "test_catalog",
        "schema": "test_schema",
        "table_prefix": "test_prefix",
    },
    "query_engine": {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "test_project",
        },
    },
}

DUMMY_KEY_VALUE_STORE_CONFIG_WITH_SOLVER_CONFIG = {
    "source": {
        "container_tags_table": "avl_databricks_mvp.silver.concept_entities",
        "container_metrics_table": "avl_databricks_mvp.silver.container_metric",
        "channel_metrics_table": "avl_databricks_mvp.silver.channel_metric",
        "channels_uri": "avl_databricks_mvp.silver.channel_data",
    },
    "unity_sink": {
        "catalog": "test_catalog",
        "schema": "test_schema",
        "table_prefix": "test_prefix",
    },
    "query_engine": {
        "solver": "KeyValueStoreSolver",
        "solver_config": {
            "project_id": "test_project",
            "container_tags": {
                "column_name_mapping": {"project": "project_id"},
            },
            "channels": {
                "column_name_mapping": {
                    "measurement_id": "container_id",
                    "signal_id": "channel_id",
                    "t_start": "tstart",
                    "t_stop": "tend",
                    "val": "value",
                },
            },
        },
    },
}


def test_report_init():
    """Test Report initialization"""
    report = Report(name="test_report", spark=None, config=DUMMY_CONFIG)

    assert report.name == "test_report"
    assert report.pages == []
    assert report.aggregation_dfs == {}


def test_set_config():
    """Test setting config with config path"""

    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "unit", "data", "config", "config.json")

    report: Report = Report(name="my_report", spark=None, config_path=config_path)

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
    report: Report = Report(name="my_report", spark=None, config=DUMMY_CONFIG)
    page = Page(page_number=1)

    report.add_page(page)

    assert len(report.pages) == 1
    assert report.pages[0] == page


def test_add_multiple_pages():
    """Test adding multiple pages to report"""
    report: Report = Report(name="my_report", spark=None, config=DUMMY_CONFIG)
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

    report = Report(name="test_report", spark=None, config=DUMMY_KEY_VALUE_STORE_CONFIG)
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
        config=DUMMY_KEY_VALUE_STORE_CONFIG_WITH_SOLVER_CONFIG,
    )
    solver = report.get_solver()

    assert isinstance(solver, KeyValueStoreSolver)
    assert solver.config.container_id_col == "container_id"
    assert solver.config.channel_id_cols == ["container_id", "channel_id"]
    assert solver.config.tstart_col == "tstart"
    assert solver.config.tend_col == "tend"
    assert solver.config.value_col == "value"
    assert solver.config.project_id_col == "project_id"
