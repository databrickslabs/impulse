"""Shared helpers for the e2e notebooks."""

import io
import os
from types import SimpleNamespace

import pandas as pd
import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils
from databricks.sdk.service.workspace import ImportFormat
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent

SILVER_TABLES = [
    "container_metrics",
    "container_tags",
    "channel_metrics",
    "channel_tags",
    "channels",
]

spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()


def params():
    """Task parameters. `prefix` is the task name and names the test's tables and expected files."""
    names = ["catalog", "schema", "source_schema", "prefix", "expected_dir", "actual_dir"]
    p = SimpleNamespace(**{n: dbutils.widgets.get(n) for n in names})
    p.source = f"{p.catalog}.{p.source_schema}."
    return p


def config(p, source=None, **extra):
    """Report config reading `<source><table>` and writing `<schema>.<prefix>_*`."""
    source = source or p.source
    cfg = {
        "source": {
            "container_metrics_table": f"{source}container_metrics",
            "container_tags_table": f"{source}container_tags",
            "channel_metrics_table": f"{source}channel_metrics",
            "channel_tags_table": f"{source}channel_tags",
            "channels_uri": f"{source}channels",
        },
        "unity_sink": {"catalog": p.catalog, "schema": p.schema, "table_prefix": p.prefix},
        "query_engine": {"solver": "DefaultSolver", "data_type": "RLE"},
        "measurement_dimensions": ["container_id", "vehicle_key", "start_ts", "stop_ts"],
    }
    for key, value in extra.items():
        cfg[key] = {**cfg.get(key, {}), **value}
    return cfg


def new_report(cfg):
    report = Report(name="e2e_report", spark=spark, workspace_client=ws, config=cfg)
    # Tests run in parallel in one schema: don't drop the other runs' temp tables.
    report._cleanup_temp_tables = lambda: None
    return report


def channel(report, name):
    return report.get_db().query.channel(channel_name=name, brand="Seat", model="Leon")


def add_rpm_histogram(report):
    """Baseline report: an RPM > 2000 event and the RPM duration histogram within it."""
    eng_rpm = channel(report, "Engine RPM")
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
    return high_rpm


def gold(p, table):
    df = spark.read.table(f"{p.catalog}.{p.schema}.{p.prefix}_{table}")
    # Skip bookkeeping columns such as _created_at, which differ per run.
    return df.select([c for c in df.columns if not c.startswith("_")])


def expected(p, name, schema, test=None):
    """Rows of `expected/<test>/<name>.csv` (default test: this one), cast to `schema`."""
    path = os.path.join(p.expected_dir, p.prefix if test is None else test, f"{name}.csv")
    # Read as strings and cast in Spark, so nulls and types round-trip exactly.
    pdf = pd.read_csv(path, dtype=str, keep_default_na=False).replace({"": None})
    df = spark.createDataFrame(pdf[schema.fieldNames()])
    return df.select([F.col(f.name).cast(f.dataType) for f in schema.fields])


def save_actual(p, name, df):
    # The workspace API takes paths without the /Workspace mount prefix.
    folder = f"{p.actual_dir.removeprefix('/Workspace')}/{p.prefix}"
    ws.workspace.mkdirs(folder)
    csv = df.select([F.col(c).cast("string") for c in df.columns]).toPandas().to_csv(index=False)
    path = f"{folder}/{name}.csv"
    ws.workspace.upload(path, io.BytesIO(csv.encode()), format=ImportFormat.AUTO, overwrite=True)
    return path


def check(p, name, actual, expected_rows=None):
    """Compare `actual` with `expected_rows` (default: this test's expected file).

    On a mismatch the actual rows are uploaded as CSV, so the expected file can be refreshed.
    """
    try:
        if expected_rows is None:
            expected_rows = expected(p, name, actual.schema)
        assertDataFrameEqual(actual, expected_rows, rtol=1e-9, maxErrors=10)
    except (AssertionError, FileNotFoundError):
        print(f"{name}: actual rows saved to {save_actual(p, name, actual)}")
        raise
    print(f"{name}: OK ({actual.count()} rows)")
