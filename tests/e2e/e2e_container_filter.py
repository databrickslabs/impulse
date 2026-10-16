# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: container filter
# MAGIC Runs the baseline report on the containers of `experiment_1` only. The result must equal
# MAGIC those containers' rows in `expected/rpm_histogram`.

# COMMAND ----------

import os

import pandas as pd
import pyspark.sql.functions as F
from e2e_utils import add_rpm_histogram, check, config, expected, gold, new_report, params

p = params()
experiment_filter = {"tag_name": "experiment_id", "comparator": "==", "value": "experiment_1"}
report = new_report(config(p, container_filters={"tag_filters": [[experiment_filter]]}))
add_rpm_histogram(report)
report.run()

# COMMAND ----------

containers = pd.read_csv(os.path.join(p.expected_dir, "containers.csv"))
ids = containers.loc[containers.experiment_id == "experiment_1", "container_id"].tolist()

for table in ["histogram_fact", "event_instance_fact"]:
    actual = gold(p, table)
    rows = expected(p, table, actual.schema, test="rpm_histogram")
    check(p, table, actual, rows.where(F.col("container_id").isin(ids)))
