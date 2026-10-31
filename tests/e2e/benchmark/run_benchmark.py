# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark: run
# MAGIC Runs and measures the scenarios of this task (`TASKS` in `benchmark_report.py`, by task key),
# MAGIC each into the task's own schema:
# MAGIC
# MAGIC - `run`: `full`, the mixed report into an empty schema (all trips); `incremental_noop`, an
# MAGIC   incremental rerun with nothing to update; `incremental_delta`, an incremental rerun after 5
# MAGIC   trips were re-ingested.
# MAGIC - `run_wide`: `wide_report`, the wide report.
# MAGIC - `run_udfs`: `python_udfs`, the report of Python extensions.
# MAGIC
# MAGIC Appends one row per scenario to the results table. A failed scenario is recorded too,
# MAGIC with its error, and fails the task.

# COMMAND ----------

# Impulse is installed per notebook: as a cluster library, the bundle's wheel keeps the
# notebook from starting ("Hive metastore initialization failed").
import glob

whl = max(glob.glob(f"{dbutils.widgets.get('wheel_dir')}/databricks_impulse-*.whl"))

# COMMAND ----------

# MAGIC %pip install --quiet $whl

# COMMAND ----------

import datetime
import json

import benchmark_udfs
from bench_utils import RESULTS_SCHEMA, fingerprint, params, report_config, run_info, spark
from benchmark_metrics import Measurement, compute_kind
from benchmark_report import (
    DELTA_CONTAINERS,
    SCENARIO_CONFIG,
    SCENARIO_CONTAINERS,
    TASKS,
    build_report,
    build_udf_report,
    build_wide_report,
    wide_signal_stats,
)
from databricks.sdk import WorkspaceClient
from pyspark import cloudpickle

from impulse_reporting.core.report import Report

# The executors can't import the workspace file the UDFs are in: pickle them by value.
cloudpickle.register_pickle_by_value(benchmark_udfs)

p = params()
task_run_id = dbutils.widgets.get("task_run_id")
suffix, scenarios = TASKS[p.task_key]
p.schema += suffix
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {p.catalog}.{p.schema}")
containers = SCENARIO_CONTAINERS[compute_kind()]

# `incremental_delta` re-ingests trips in a copy of container_metrics, read by the task's scenarios.
silver_copy = None
if "incremental_delta" in scenarios:
    silver_copy = f"{p.catalog}.{p.schema}.silver_container_metrics"
    spark.sql(f"CREATE TABLE {silver_copy} AS SELECT * FROM {p.source}container_metrics")


def scenario_config(scenario):
    return report_config(p, silver_copy, containers.get(scenario), SCENARIO_CONFIG.get(scenario))


def builder(scenario, cfg):
    if scenario == "wide_report":
        signals = wide_signal_stats(spark, p.source)
        cap = cfg.get("query_engine", {}).get("max_channels_per_batch")
        return lambda report: build_wide_report(report, signals, cap)
    if scenario == "python_udfs":
        return build_udf_report
    return build_report


def prepare(scenario):
    """What happens in the silver layer before the scenario's run."""
    if scenario == "incremental_delta":
        ids = ", ".join(str(c) for c in DELTA_CONTAINERS)
        spark.sql(
            f"UPDATE {silver_copy} SET _ingested_at = current_timestamp() "
            f"WHERE container_id IN ({ids})"
        )


def record(scenario, status, metrics=None, error=None, prints=None):
    row = {
        **run_info(p),
        "ts": datetime.datetime.now(datetime.timezone.utc),
        "scenario": scenario,
        "status": status,
        "error": error,
        **(metrics or {}),
        "fingerprint": prints,
    }
    df = spark.createDataFrame([row], RESULTS_SCHEMA)
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(p.results)


# COMMAND ----------

summary = {}
for scenario in scenarios:
    measurement = Measurement(task_run_id)
    try:
        cfg = scenario_config(scenario)
        print(scenario, json.dumps(cfg, indent=2))
        build = builder(scenario, cfg)
        prepare(scenario)
        # A new report object per run, as a scheduled job would build it. Errors in the
        # definition or the config overrides are recorded like errors of the run.
        report = Report(
            name="benchmark", spark=spark, workspace_client=WorkspaceClient(), config=cfg
        )
        build(report)
        with measurement:
            report.run()
    except Exception as e:
        wall_s = getattr(measurement, "wall_s", None)
        record(scenario, "failed", {"wall_s": wall_s}, error=repr(e)[:4000])
        raise
    metrics = measurement.metrics()
    # The gold tables after each scenario that computes something.
    prints = fingerprint(p) if scenario != "incremental_noop" else None
    record(scenario, "success", metrics, prints=prints)
    summary[scenario] = metrics
    print(scenario, json.dumps(metrics, indent=2))

dbutils.notebook.exit(json.dumps(summary))
