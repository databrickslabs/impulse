# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark: teardown
# MAGIC Drops the run's schema, also after a failure: its gold tables are large and the results
# MAGIC table keeps what is needed to debug.

# COMMAND ----------

# Impulse is installed per notebook: as a cluster library, the bundle's wheel keeps the
# notebook from starting ("Hive metastore initialization failed").
import glob

whl = max(glob.glob(f"{dbutils.widgets.get('wheel_dir')}/databricks_impulse-*.whl"))

# COMMAND ----------

# MAGIC %pip install --quiet $whl

# COMMAND ----------

from bench_utils import params, spark

p = params()
if not p.schema.startswith("bench_"):
    raise ValueError(f"Refusing to drop non-benchmark schema {p.schema!r}")
spark.sql(f"DROP SCHEMA IF EXISTS {p.catalog}.{p.schema} CASCADE")
