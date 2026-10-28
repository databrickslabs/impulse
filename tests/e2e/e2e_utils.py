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
    names = [
        "catalog",
        "schema",
        "source_catalog",
        "source_schema",
        "prefix",
        "expected_dir",
        "actual_dir",
    ]
    p = SimpleNamespace(**{n: dbutils.widgets.get(n) for n in names})
    p.source = f"{p.source_catalog}.{p.source_schema}."
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
    # Tests run in parallel in one schema: don't drop the other runs' temp tables. Every run
    # drops them at its start, whatever `unity_sink.cleanup_temp_tables` says.
    report._cleanup_temp_tables = lambda: None
    return report


def channel(report, name):
    return report.get_db().query.channel(channel_name=name, brand="Seat", model="Leon")


def add_rpm_histogram(report, bin_width=500):
    """Baseline report: an RPM > 2000 event and the RPM duration histogram within it."""
    eng_rpm = channel(report, "Engine RPM")
    high_rpm = BasicEvent(name="high_rpm", expr=eng_rpm > 2000, desc="Engine RPM above 2000")
    report.add_event(high_rpm)
    page = Page(page_number=1)
    page.add_aggregation(
        HistogramDuration(
            name="rpm_distribution",
            base_expr=eng_rpm,
            bins=[float(x) for x in range(0, 5001, bin_width)],
            event=high_rpm,
            channel_name="Engine RPM",
            bins_unit="rpm",
            values_unit="s",
        )
    )
    report.add_page(page)
    return high_rpm


def copy_silver(p):
    """Empty copies of the silver tables, named `<prefix>_src_*`, with a `last_modified`
    column on container_metrics. Returns the source prefix for `config(p, source=...)`."""
    source = f"{p.catalog}.{p.schema}.{p.prefix}_src_"
    for table in SILVER_TABLES:
        extra = ", current_timestamp() AS last_modified" if table == "container_metrics" else ""
        spark.sql(
            f"CREATE TABLE {source}{table} AS SELECT *{extra} FROM {p.source}{table} WHERE false"
        )
    return source


def load_silver(p, source, where):
    """Copy the source rows matching `where` into the silver copy."""
    for table in SILVER_TABLES:
        extra = ", current_timestamp()" if table == "container_metrics" else ""
        spark.sql(
            f"INSERT INTO {source}{table} SELECT *{extra} FROM {p.source}{table} WHERE {where}"
        )


def versions(p, tables):
    """Latest Delta version of each of this test's gold tables."""
    return {
        t: spark.sql(f"DESCRIBE HISTORY {p.catalog}.{p.schema}.{p.prefix}_{t} LIMIT 1")
        .first()
        .version
        for t in tables
    }


# Commits by predictive optimization and other table maintenance.
MAINTENANCE_OPERATIONS = ["OPTIMIZE", "VACUUM START", "VACUUM END"]


def commits_since(p, table, version):
    """History of this test's gold `table` after `version`, without maintenance commits."""
    name = f"{p.catalog}.{p.schema}.{p.prefix}_{table}"
    return spark.sql(f"DESCRIBE HISTORY {name}").where(
        (F.col("version") > version) & ~F.col("operation").isin(MAINTENANCE_OPERATIONS)
    )


def assert_unchanged(p, before, where):
    """The rows matching `where` (incl. _created_at) equal those at the versions in `before`."""
    for table, version in before.items():
        name = f"{p.catalog}.{p.schema}.{p.prefix}_{table}"
        assertDataFrameEqual(
            spark.sql(f"SELECT * FROM {name} WHERE {where}"),
            spark.sql(f"SELECT * FROM {name} VERSION AS OF {version} WHERE {where}"),
        )


def assert_histogram_covers_events(histogram, events):
    """Per container, the RPM histogram adds up to the duration of its event's instances,
    and the bins up to the event threshold (2000 rpm) are empty."""
    assert (
        histogram.where("upper_bound <= 2000 AND hist_value != 0").count() == 0
    ), "histogram values below the event threshold"
    keys = ["container_id", "event_id"]
    totals = histogram.groupBy(keys).agg(F.sum("hist_value").alias("hist"))
    durations = (
        events.join(histogram.select("event_id").distinct(), "event_id", "semi")
        .groupBy(keys)
        .agg(F.sum(F.col("end_ts") - F.col("start_ts")).alias("dur"))
    )
    # Containers without events still have (empty) histogram rows.
    diff = totals.join(durations, keys, "full").where(
        F.abs(F.coalesce("hist", F.lit(0.0)) - F.coalesce("dur", F.lit(0.0))) > 1e-5
    )
    assert diff.count() == 0, f"histogram doesn't match event durations: {diff.take(5)}"


def event_ids(p):
    rows = gold(p, "event_dimension").select("event_name", "event_id").collect()
    return {r.event_name: r.event_id for r in rows}


def gold(p, table, prefix=None):
    df = spark.read.table(f"{p.catalog}.{p.schema}.{prefix or p.prefix}_{table}")
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


def check(p, name, actual, expected_rows=None, test=None, where=None):
    """Compare `actual` with `expected_rows` (default: the expected file of `test`, by default
    this one, filtered by the `where` condition if given).

    On a mismatch the actual rows are uploaded as CSV, so the expected file can be refreshed.
    """
    try:
        if expected_rows is None:
            expected_rows = expected(p, name, actual.schema, test)
            if where is not None:
                expected_rows = expected_rows.where(where)
        # Absolute tolerance only: a relative one would allow seconds of drift on epoch timestamps.
        assertDataFrameEqual(actual, expected_rows, rtol=0, atol=1e-6, maxErrors=10)
    except Exception:
        # Also on a missing or outdated expected file (e.g. a new column).
        print(f"{name}: actual rows saved to {save_actual(p, name, actual)}")
        raise
    print(f"{name}: OK ({actual.count()} rows)")
