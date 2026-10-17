# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: incremental updates
# MAGIC Runs the baseline report incrementally on a copy of the silver tables, then:
# MAGIC 1. reruns without changes: the fact tables must not be rewritten,
# MAGIC 2. removes half of container 5's samples: only that container is recomputed,
# MAGIC 3. changes the histogram bins: only the histogram is recomputed.
# MAGIC
# MAGIC The result must equal a full run of the final report on the final data.

# COMMAND ----------

from e2e_utils import (
    add_rpm_histogram,
    check,
    config,
    copy_silver,
    expected,
    gold,
    load_silver,
    new_report,
    params,
    spark,
    versions,
)

p = params()
silver = copy_silver(p)
load_silver(p, silver, "true")
incremental = {"enabled": True, "silver_last_modified_column": "last_modified"}

report = new_report(config(p, source=silver, incremental=incremental))
add_rpm_histogram(report)
report.run()

# COMMAND ----------

# 1. No changes: the fact tables must not be rewritten.
tables = ["histogram_fact", "event_instance_fact"]
before = versions(p, tables)
report.run()
assert versions(p, tables) == before, "a run without changes rewrote fact tables"

# COMMAND ----------

# 2. Container 5 is re-ingested with only the first half of its samples.
mid = spark.sql(
    f"SELECT (start_ts + stop_ts) / 2 FROM {silver}container_metrics WHERE container_id = 5"
).first()[0]
spark.sql(f"DELETE FROM {silver}channels WHERE container_id = 5 AND tstart > {mid}")
spark.sql(
    f"UPDATE {silver}container_metrics SET last_modified = current_timestamp() "
    "WHERE container_id = 5"
)
report.run()

# COMMAND ----------

# 3. New histogram bins; the event is unchanged.
report = new_report(config(p, source=silver, incremental=incremental))
add_rpm_histogram(report, bin_width=250)
before = versions(p, ["histogram_fact", "event_instance_fact"])
report.run()
after = versions(p, ["histogram_fact", "event_instance_fact"])
assert after["histogram_fact"] > before["histogram_fact"], "histogram was not recomputed"
assert after["event_instance_fact"] == before["event_instance_fact"], "event was rewritten"

# COMMAND ----------

# Reference: a full run of the same report on the same data.
ref = f"{p.prefix}_ref"
reference = new_report(config(p, source=silver, unity_sink={"table_prefix": ref}))
add_rpm_histogram(reference, bin_width=250)
reference.run()

for table in ["histogram_fact", "event_instance_fact"]:
    check(p, table, gold(p, table), gold(p, table, prefix=ref))

events = gold(p, "event_instance_fact").where("container_id = 5")
original = expected(p, "event_instance_fact", events.schema, test="rpm_histogram")
assert events.count() < original.where("container_id = 5").count(), "container 5 was not updated"
