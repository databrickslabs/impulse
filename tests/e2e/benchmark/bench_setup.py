# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark: setup
# MAGIC Creates the results table (or adds new columns to it) and the run's schema, drops
# MAGIC benchmark schemas left over from cancelled runs, and checks that the source data is still the
# MAGIC full dataset.

# COMMAND ----------

# Impulse is installed per notebook: as a cluster library, the bundle's wheel keeps the
# notebook from starting ("Hive metastore initialization failed").
import glob

whl = max(glob.glob(f"{dbutils.widgets.get('wheel_dir')}/databricks_impulse-*.whl"))

# COMMAND ----------

# MAGIC %pip install --quiet $whl

# COMMAND ----------

import hashlib

import pyspark.sql.functions as F
from bench_utils import RESULTS_SCHEMA, params, spark

p = params()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {p.catalog}.{p.results_schema}")
ddl = lambda fields: ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in fields)
spark.sql(f"CREATE TABLE IF NOT EXISTS {p.results} ({ddl(RESULTS_SCHEMA.fields)})")
# Metrics added since the table was created.
existing = set(spark.read.table(p.results).columns)
missing = [f for f in RESULTS_SCHEMA.fields if f.name not in existing]
if missing:
    spark.sql(f"ALTER TABLE {p.results} ADD COLUMNS ({ddl(missing)})")
spark.sql(f"CREATE SCHEMA {p.catalog}.{p.schema}")

# COMMAND ----------

# Only this job's own schemas: bench_<run id>[_<task>], owned by the running principal, older
# than a day.
stale = spark.sql(
    f"SELECT schema_name FROM {p.catalog}.information_schema.schemata "
    "WHERE schema_name RLIKE '^bench_[0-9]+(_[a-z]+)?$' AND schema_owner = current_user() "
    "AND created < current_timestamp() - INTERVAL 1 DAY"
)
for row in stale.collect():
    spark.sql(f"DROP SCHEMA {p.catalog}.{row.schema_name} CASCADE")

# COMMAND ----------

# The dataset as ingested by benchmark/ingest: 100 trips, their channel and sample counts. Only
# counts and a checksum, so the repository holds no data derived from the dataset.
EXPECTED_CONTAINERS = 100
EXPECTED_SAMPLES = 5_076_151_061
# SHA-256 of the sorted lines "<container_id>,<channels>,<samples>", joined by newlines.
EXPECTED_CHECKSUM = "d4e2d13aa60176a4fb6f27b7d3784c27556fff1c90adca9b2c8079448cbe160d"

containers = (
    spark.read.table(f"{p.source}container_metrics")
    .select("container_id")
    .join(
        spark.read.table(f"{p.source}channel_metrics")
        .groupBy("container_id")
        .agg(F.count("*").alias("channels"), F.sum("sample_count").alias("samples")),
        "container_id",
        "left",
    )
    .collect()
)
lines = sorted(f"{r.container_id},{r.channels},{r.samples}" for r in containers)
checksum = hashlib.sha256("\n".join(lines).encode()).hexdigest()
samples = sum(r.samples or 0 for r in containers)
if (len(containers), samples, checksum) != (
    EXPECTED_CONTAINERS,
    EXPECTED_SAMPLES,
    EXPECTED_CHECKSUM,
):
    print("container_id,channels,samples")
    print("\n".join(lines))
    raise AssertionError(
        f"source data isn't the benchmark dataset: {len(containers)} containers, {samples} samples, "
        f"checksum {checksum}; expected {EXPECTED_CONTAINERS}, {EXPECTED_SAMPLES}, "
        f"{EXPECTED_CHECKSUM}. The actual rows are printed above."
    )
print(f"source data OK: {len(containers)} containers, {samples} samples")
