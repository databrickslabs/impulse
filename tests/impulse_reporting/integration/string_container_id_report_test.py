# pylint: disable=missing-function-docstring
"""End-to-end reporting test with a STRING-typed ``container_id``.

The query engine derives the ``container_id`` type from the source tables
instead of hardcoding ``LongType``.  The reporting persist path is
projection-based (``select(FACT_SCHEMA.fieldNames())``) and writes with
``overwriteSchema=True``, so a string ``container_id`` should flow unchanged
to the gold layer.  This reuses the basic silver tables, recasting
``container_id`` to string, then runs a full report and asserts the gold fact
and measurement-dimension tables keep the ``StringType`` ``container_id``.
"""

from unittest.mock import create_autospec

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from databricks.sdk import WorkspaceClient

from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.config.config_parser import (
    Comparator,
    ContainerFilters,
    ImpulseConfig,
    MetricFilter,
    QueryEngine,
    Solvers,
    Source,
    UnitySink,
)
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from tests.conftest import setup_basic_db, spark  # noqa: F401  (pytest fixtures)

_STRING_CID_SCHEMA = "spark_catalog.silver_string_cid"


@pytest.fixture
def setup_string_cid_db(spark, setup_basic_db):  # noqa: F811
    """Silver tables cloned from the basic db with ``container_id`` cast to string."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_STRING_CID_SCHEMA}")
    for table in ("container_metrics", "channel_metrics", "channels"):
        spark.read.table(f"spark_catalog.silver.{table}").withColumn(
            "container_id", F.col("container_id").cast("string")
        ).write.format("delta").mode("overwrite").saveAsTable(f"{_STRING_CID_SCHEMA}.{table}")
    yield
    spark.sql(f"DROP SCHEMA IF EXISTS {_STRING_CID_SCHEMA} CASCADE")


def test_persist_report_string_container_id(spark, setup_string_cid_db):
    config = dict(
        ImpulseConfig(
            source=Source(
                container_metrics_table=f"{_STRING_CID_SCHEMA}.container_metrics",
                channel_metrics_table=f"{_STRING_CID_SCHEMA}.channel_metrics",
                channels_uri=f"{_STRING_CID_SCHEMA}.channels",
            ),
            unity_sink=UnitySink(
                catalog="spark_catalog", schema="gold", table_prefix="evaluation"
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
            query_engine=QueryEngine(solver=Solvers.KEY_VALUE_STORE_SOLVER),
            # Include container_id so it lands in the measurement-dimension table too.
            measurement_dimensions=["container_id", "uut_id", "file_name", "file_path"],
        )
    )

    report = Report(
        name="string_cid_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=config,
    )
    query = report.get_db().query
    page = Page(page_number=1)
    report.add_page(page)
    page.add_aggregation(
        HistogramDuration(
            "rpm_hist",
            base_expr=query.channel(channel_name="Engine RPM"),
            bins=[float(i) for i in range(0, 8000, 250)],
        )
    )

    report.determine_report()
    report.persist_results()
    # Second persist merges string-keyed rows into the existing gold tables (Delta MERGE).
    report.persist_results()

    histogram_fact = spark.read.table("spark_catalog.gold.evaluation_histogram_fact")
    measurement_dimension = spark.read.table("spark_catalog.gold.evaluation_measurement_dimension")

    # The key assertion: container_id keeps StringType all the way to gold — it is NOT
    # coerced to the IntegerType declared in fact_schema.py.
    assert histogram_fact.schema["container_id"].dataType == T.StringType()
    assert measurement_dimension.schema["container_id"].dataType == T.StringType()

    # The fact table is actually populated with real, non-zero histogram values.
    assert histogram_fact.count() > 0, "histogram fact table is empty"
    total_hist_value = histogram_fact.agg(F.sum("hist_value")).first()[0]
    assert total_hist_value is not None and total_hist_value > 0, total_hist_value
