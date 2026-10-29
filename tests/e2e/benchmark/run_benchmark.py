# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark: run
# MAGIC Runs the benchmark report (`benchmark_report.py`) on the full silver set and measures:
# MAGIC
# MAGIC - `full`: the first run, into an empty schema (all containers).
# MAGIC - `incremental_noop`: an incremental run right after it, with nothing to update.
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

from bench_utils import RESULTS_SCHEMA, fingerprint, params, report_config, run_info, spark
from benchmark_metrics import Measurement
from benchmark_report import build_report
from databricks.sdk import WorkspaceClient

from impulse_reporting.core.report import Report

p = params()
cfg = report_config(p)
print(json.dumps(cfg, indent=2))


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
for scenario in ["full", "incremental_noop"]:
    measurement = Measurement()
    try:
        # A new report object per run, as a scheduled job would build it. Errors in the
        # definition or the config overrides are recorded like errors of the run.
        report = Report(
            name="benchmark", spark=spark, workspace_client=WorkspaceClient(), config=cfg
        )
        build_report(report)
        with measurement:
            report.run()
    except Exception as e:
        wall_s = getattr(measurement, "wall_s", None)
        record(scenario, "failed", {"wall_s": wall_s}, error=repr(e)[:4000])
        raise
    metrics = measurement.metrics()
    prints = fingerprint(p) if scenario == "full" else None
    record(scenario, "success", metrics, prints=prints)
    summary[scenario] = metrics
    print(scenario, json.dumps(metrics, indent=2))

dbutils.notebook.exit(json.dumps(summary))
