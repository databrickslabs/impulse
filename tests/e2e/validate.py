# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: validate
# MAGIC Compares the report's gold tables with the expected rows in `tests/e2e/expected`.
# MAGIC On a mismatch the actual rows are uploaded as CSV to `actual_dir`, so the expected
# MAGIC files can be refreshed from them.

# COMMAND ----------

import io
import os

import pandas as pd
import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils
from databricks.sdk.service.workspace import ImportFormat
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
expected_dir = dbutils.widgets.get("expected_dir")
# The workspace API takes paths without the /Workspace mount prefix.
actual_dir = dbutils.widgets.get("actual_dir").removeprefix("/Workspace")

# Gold tables to compare, without the report's table prefix.
TABLES = ["histogram_fact", "event_instance_fact"]

spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()

# COMMAND ----------


def as_strings(df):
    return df.select([F.col(c).cast("string") for c in df.columns])


def load_expected(path, schema):
    # Read as strings and cast in Spark, so nulls and types round-trip exactly.
    pdf = pd.read_csv(path, dtype=str, keep_default_na=False).replace({"": None})
    df = spark.createDataFrame(pdf[schema.fieldNames()])
    return df.select([F.col(f.name).cast(f.dataType) for f in schema.fields])


def save_actual(name, df):
    ws.workspace.mkdirs(actual_dir)
    path = f"{actual_dir}/{name}.csv"
    csv = as_strings(df).toPandas().to_csv(index=False)
    ws.workspace.upload(path, io.BytesIO(csv.encode()), format=ImportFormat.AUTO, overwrite=True)
    return path


# COMMAND ----------

failures = []
for name in TABLES:
    actual = spark.read.table(f"`{catalog}`.`{schema}`.eval_{name}")
    # Skip bookkeeping columns such as _created_at, which differ per run.
    actual = actual.select([c for c in actual.columns if not c.startswith("_")])
    expected_path = os.path.join(expected_dir, f"{name}.csv")

    if not os.path.exists(expected_path):
        failures.append(
            f"{name}: no expected file; actual rows saved to {save_actual(name, actual)}"
        )
        continue

    # Column types follow the gold table, which inherits them from the source data.
    expected = load_expected(expected_path, actual.schema)
    try:
        assertDataFrameEqual(actual, expected, rtol=1e-9, maxErrors=10)
        print(f"{name}: OK ({actual.count()} rows)")
    except AssertionError as e:
        print(f"{name}: actual {actual.count()} rows, expected {expected.count()} rows\n{e}")
        failures.append(f"{name}: mismatch; actual rows saved to {save_actual(name, actual)}")

if failures:
    raise AssertionError("\n".join(failures))
