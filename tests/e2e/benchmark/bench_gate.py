# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark: gate
# MAGIC Fails the run if `compare` found a regression above `max_regression`.

# COMMAND ----------

import json

regressions = json.loads(dbutils.jobs.taskValues.get(taskKey="compare", key="regressions"))
if regressions:
    raise AssertionError(f"performance regressions: {regressions}")
print("no regressions")
