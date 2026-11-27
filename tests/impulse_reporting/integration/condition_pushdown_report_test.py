"""Outlier events (``channel op scalar``, also on a resampled channel) are solved in Spark
without the pandas UDF."""

from unittest.mock import create_autospec

import pandas as pd
from databricks.sdk import WorkspaceClient

from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_reporting.config.config_parser import (
    ImpulseConfig,
    QueryEngine,
    Solvers,
    Source,
    UnitySink,
)
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent
from tests.conftest import drop_gold_tables, spark  # noqa: F401  (pytest fixture)

_EVENT_FACT = "spark_catalog.gold.evaluation_event_instance_fact"
_EVENT_DIMENSION = "spark_catalog.gold.evaluation_event_dimension"


def _report(spark, enable_pushdown: bool) -> Report:
    config = ImpulseConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver.container_metrics",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(catalog="spark_catalog", schema="gold", table_prefix="evaluation"),
        query_engine=QueryEngine(
            solver=Solvers.KEY_VALUE_STORE_SOLVER, enable_pushdown=enable_pushdown
        ),
    )
    report = Report(
        name="outlier_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(config),
    )
    q = report.get_db().query
    rpm = q.channel(channel_name="Engine RPM")
    speed = q.channel(channel_name="Vehicle Speed Sensor")
    report.add_event(BasicEvent(name="rpm_high", expr=rpm > 2000, desc="RPM outlier"))
    report.add_event(BasicEvent(name="rpm_low", expr=400 > rpm, desc="RPM below 400"))
    report.add_event(BasicEvent(name="speed_high", expr=speed >= 50, desc="Speed outlier"))
    # On the 10 s grid; the comparison absorbs the resample.
    report.add_event(
        BasicEvent(
            name="speed_high_10s", expr=speed.resample(1e7) >= 50, desc="Speed outlier, 10 s"
        )
    )
    return report


def test_outlier_event_facts_are_identical_without_the_udf(spark, monkeypatch):
    udf_runs = []
    original = DefaultSolver._apply_grouped_map

    def _counting(self, *args, **kwargs):
        udf_runs[-1] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(DefaultSolver, "_apply_grouped_map", _counting)
    facts = []
    for enable_pushdown in (False, True):
        drop_gold_tables(spark)
        udf_runs.append(0)
        report = _report(spark, enable_pushdown)
        report.run()
        names = spark.read.table(_EVENT_DIMENSION).select("event_id", "event_name")
        facts.append(
            spark.read.table(_EVENT_FACT)
            .drop("_created_at")
            .join(names, on="event_id")
            .toPandas()
            .sort_values(["container_id", "event_id", "start_ts"])
            .reset_index(drop=True)
        )
    off, on = facts
    assert udf_runs[0] > 0 and udf_runs[1] == 0
    # The resampled comparison has instances, like the raw one.
    assert {"speed_high", "speed_high_10s"} <= set(off["event_name"])
    assert off["end_ts"].gt(off["start_ts"]).all()
    pd.testing.assert_frame_equal(on, off, check_exact=True)
