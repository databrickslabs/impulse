# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark: teardown
# MAGIC Drops the run's schemas (one per run task), also after a failure: their gold tables are
# MAGIC large and the results table keeps what is needed to debug.

# COMMAND ----------

# The bundle's wheel, installed per notebook: as a cluster library it breaks the metastore init.
import glob

whl = max(glob.glob(f"{dbutils.widgets.get('wheel_dir')}/databricks_impulse-*.whl"))

# COMMAND ----------

# MAGIC %pip install --quiet $whl

# COMMAND ----------

from bench_utils import params, spark
from benchmark_report import TASKS

p = params()
if not p.schema.startswith("bench_"):
    raise ValueError(f"Refusing to drop non-benchmark schema {p.schema!r}")
for suffix, _ in TASKS.values():
    spark.sql(f"DROP SCHEMA IF EXISTS {p.catalog}.{p.schema}{suffix} CASCADE")
