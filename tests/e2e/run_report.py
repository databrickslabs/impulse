"""e2e step: run a report from the silver tables into the test schema."""

import argparse

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--source-schema", required=True)
args = parser.parse_args()

source = f"{args.catalog}.{args.source_schema}"
config = {
    "source": {
        "container_metrics_table": f"{source}.container_metrics",
        "container_tags_table": f"{source}.container_tags",
        "channel_metrics_table": f"{source}.channel_metrics",
        "channel_tags_table": f"{source}.channel_tags",
        "channels_uri": f"{source}.channels",
    },
    "unity_sink": {"catalog": args.catalog, "schema": args.schema, "table_prefix": "eval"},
    "query_engine": {"solver": "DefaultSolver", "data_type": "RLE"},
    "measurement_dimensions": ["container_id", "vehicle_key", "start_ts", "stop_ts"],
}

report = Report(
    name="e2e_report",
    spark=SparkSession.builder.getOrCreate(),
    workspace_client=WorkspaceClient(),
    config=config,
)
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

report.run()
