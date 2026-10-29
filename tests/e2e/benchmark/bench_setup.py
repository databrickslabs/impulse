# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark: setup
# MAGIC Creates the results table and the run's schema, drops benchmark schemas left over from
# MAGIC cancelled runs, fills in the DBUs of earlier runs, and checks that the source data still
# MAGIC matches `expected/benchmark/containers.csv`.

# COMMAND ----------

# Impulse is installed per notebook: as a cluster library, the bundle's wheel keeps the
# notebook from starting ("Hive metastore initialization failed").
import glob

whl = max(glob.glob(f"{dbutils.widgets.get('wheel_dir')}/databricks_impulse-*.whl"))

# COMMAND ----------

# MAGIC %pip install --quiet $whl

# COMMAND ----------

import pyspark.sql.functions as F
from bench_utils import RESULTS_SCHEMA, params, spark
from e2e_utils import check

p = params()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {p.catalog}.{p.results_schema}")
columns = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in RESULTS_SCHEMA.fields)
spark.sql(f"CREATE TABLE IF NOT EXISTS {p.results} ({columns})")
spark.sql(f"CREATE SCHEMA {p.catalog}.{p.schema}")

# COMMAND ----------

# Only this job's own schemas: bench_<run id>, owned by the running principal, older than a day.
stale = spark.sql(
    f"SELECT schema_name FROM {p.catalog}.information_schema.schemata "
    "WHERE schema_name RLIKE '^bench_[0-9]+$' AND schema_owner = current_user() "
    "AND created < current_timestamp() - INTERVAL 1 DAY"
)
for row in stale.collect():
    spark.sql(f"DROP SCHEMA {p.catalog}.{row.schema_name} CASCADE")

# COMMAND ----------

# Billing data arrives hours after a run, so each run fills in the DBUs of earlier ones.
try:
    spark.sql(f"""
        MERGE INTO {p.results} r
        USING (
            SELECT usage_metadata.job_run_id AS job_run_id, sum(usage_quantity) AS dbus
            FROM system.billing.usage
            WHERE usage_metadata.job_id = '{p.job_id}' AND usage_unit = 'DBU'
            GROUP BY 1
        ) u
        ON r.job_run_id = u.job_run_id AND r.dbus IS NULL
        WHEN MATCHED THEN UPDATE SET r.dbus = u.dbus
        """)
except Exception as e:  # The principal may not read system tables.
    print(f"DBUs not updated: {e}")

# COMMAND ----------

containers = (
    spark.read.table(f"{p.source}container_metrics")
    .join(
        spark.read.table(f"{p.source}channel_metrics")
        .groupBy("container_id")
        .agg(F.count("*").alias("channels"), F.sum("sample_count").alias("samples")),
        "container_id",
    )
    .select("container_id", "vehicle_key", "trip_name", "channels", "samples")
)
check(p, "containers", containers, test="benchmark")
