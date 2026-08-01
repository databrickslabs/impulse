# pylint: disable=missing-function-docstring
"""End-to-end report pipeline against a real Databricks workspace.

Runs inside a serverless job (see ``databricks.yml``). Exercises the whole
library path on real Unity Catalog + serverless Spark that the local-Spark unit
suite can only mock:

1. Load the Seat Leon reporting demo CSVs into silver Delta tables in an
   **ephemeral** UC schema (auto-dropped after the test).
2. Build a ``Report`` with a ``UnitySink`` pointing at that schema, defining a
   ``BasicEvent`` (RPM > 2000) and a ``HistogramDuration`` over ``Engine RPM``
   — the same shape as ``demos/getting_started.ipynb``.
3. ``determine_report()`` → ``persist_results()`` **twice**, so the second write
   goes through the incremental Delta ``MERGE`` path on real serverless compute.
4. Assert the gold histogram-fact table holds real, non-zero values (not just
   row counts), mirroring
   ``tests/impulse_reporting/integration/string_container_id_report_test.py``.
"""

import pandas as pd
import pyspark.sql.functions as F
import pytest

from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent

# Silver tables loaded from demos/data/reporting. `channels` is RAW point data
# (container_id, channel_id, timestamp, value); metadata is narrow/EAV.
_SILVER_TABLES = (
    "container_metrics",
    "container_tags",
    "channel_metrics",
    "channel_tags",
    "channels",
)


def _load_silver(spark, schema_full_name: str, csv_dir: str) -> None:
    """Load the demo CSVs into ``<schema>.<table>`` Delta tables."""
    for table in _SILVER_TABLES:
        pdf = pd.read_csv(f"{csv_dir}/{table}.csv")
        (
            spark.createDataFrame(pdf)
            .write.format("delta")
            .mode("overwrite")
            .saveAsTable(f"{schema_full_name}.{table}")
        )


@pytest.mark.e2e
def test_report_pipeline_end_to_end(spark, ws, e2e_schema, reporting_demo_dir):
    schema = e2e_schema.full_name  # impulse_tests.<random>
    _load_silver(spark, schema, reporting_demo_dir)

    config = {
        "source": {
            "container_metrics_table": f"{schema}.container_metrics",
            "container_tags_table": f"{schema}.container_tags",
            "channel_metrics_table": f"{schema}.channel_metrics",
            "channel_tags_table": f"{schema}.channel_tags",
            "channels_uri": f"{schema}.channels",
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

    report.determine_report()
    report.persist_results()
    # Second persist merges into the existing gold tables (incremental Delta MERGE).
    report.persist_results()

    histogram_fact = spark.read.table(f"{schema}.eval_histogram_fact")

    assert histogram_fact.count() > 0, "histogram fact table is empty"
    total_hist_value = histogram_fact.agg(F.sum("hist_value")).first()[0]
    assert total_hist_value is not None and total_hist_value > 0, total_hist_value
