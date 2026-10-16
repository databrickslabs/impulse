# pylint: disable=missing-function-docstring
"""End-to-end report pipeline on Databricks: silver tables -> report -> gold tables."""

import pyspark.sql.functions as F
import pytest

from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent


@pytest.mark.e2e
def test_report_pipeline_end_to_end(spark, ws, e2e_schema, source_schema):
    schema = e2e_schema.full_name

    config = {
        "source": {
            "container_metrics_table": f"{source_schema}.container_metrics",
            "container_tags_table": f"{source_schema}.container_tags",
            "channel_metrics_table": f"{source_schema}.channel_metrics",
            "channel_tags_table": f"{source_schema}.channel_tags",
            "channels_uri": f"{source_schema}.channels",
        },
        "unity_sink": {
            "catalog": e2e_schema.catalog_name,
            "schema": e2e_schema.name,
            "table_prefix": "eval",
        },
        "query_engine": {"solver": "DefaultSolver", "data_type": "RAW"},
        "measurement_dimensions": ["container_id", "vehicle_key", "start_ts", "stop_ts"],
    }

    report = Report(name="e2e_report", spark=spark, workspace_client=ws, config=config)
    eng_rpm = report.get_db().query.channel(channel_name="Engine RPM", brand="Seat", model="Leon")

    high_rpm = BasicEvent(name="high_rpm", expr=eng_rpm > 2000, desc="Engine RPM above 2000")
    report.add_event(high_rpm)

    page = Page(page_number=1)
    page.add_aggregation(
        HistogramDuration(
            name="rpm_distribution",
            base_expr=eng_rpm,
            bins=[float(x) for x in range(0, 5001, 500)],
            event=high_rpm,
            channel_name="Engine RPM",
            bins_unit="rpm",
            values_unit="s",
        )
    )
    report.add_page(page)

    histogram_fact_table = f"{schema}.eval_histogram_fact"

    report.run()
    rows_after_first_run = spark.read.table(histogram_fact_table).count()

    # A second run must update the gold tables in place, not append.
    report.run()
    histogram_fact = spark.read.table(histogram_fact_table)

    assert rows_after_first_run > 0, "histogram fact table is empty"
    assert histogram_fact.count() == rows_after_first_run, "second run duplicated histogram rows"
    total_hist_value = histogram_fact.agg(F.sum("hist_value")).first()[0]
    assert total_hist_value is not None and total_hist_value > 0, total_hist_value
