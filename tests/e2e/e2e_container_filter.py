# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: container filter
# MAGIC Runs the baseline report on the containers of `experiment_1` or `experiment_2` that are
# MAGIC longer than 30 minutes (tag filter groups OR-combined, AND-combined with a metric filter).
# MAGIC The result must equal those containers' rows in `expected/rpm_histogram`.

# COMMAND ----------

import pyspark.sql.functions as F
from e2e_utils import add_rpm_histogram, check, config, gold, new_report, params, spark

p = params()
experiments = ["experiment_1", "experiment_2"]
filters = {
    "tag_filters": [
        [{"tag_name": "experiment_id", "comparator": "==", "value": e}] for e in experiments
    ],
    "metric_filters": [[{"column_name": "duration_s", "comparator": ">", "value": 1800.0}]],
}
report = new_report(config(p, container_filters=filters))
add_rpm_histogram(report)
report.run()

# COMMAND ----------

# The same selection in plain SQL.
ids = [
    r.container_id
    for r in spark.sql(
        f"SELECT m.container_id FROM {p.source}container_metrics m "
        f"JOIN {p.source}container_tags t ON t.container_id = m.container_id "
        f"WHERE t.key = 'experiment_id' AND t.value IN ('experiment_1', 'experiment_2') "
        "AND m.duration_s > 1800"
    ).collect()
]
all_ids = spark.read.table(f"{p.source}container_tags").select("container_id").distinct().count()
assert 0 < len(ids) < all_ids, f"filter selects {len(ids)} of {all_ids} containers"

for table in ["histogram_fact", "event_instance_fact"]:
    check(p, table, gold(p, table), test="rpm_histogram", where=F.col("container_id").isin(ids))
