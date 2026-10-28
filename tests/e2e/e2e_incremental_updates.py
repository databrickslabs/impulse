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

import pyspark.sql.functions as F
from e2e_utils import (
    add_rpm_histogram,
    assert_histogram_covers_events,
    assert_unchanged,
    check,
    commits_since,
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
from pyspark.testing import assertDataFrameEqual

p = params()
silver = copy_silver(p)
load_silver(p, silver, "true")
incremental = {"enabled": True, "silver_last_modified_column": "last_modified"}
tables = ["histogram_fact", "event_instance_fact"]


report = new_report(config(p, source=silver, incremental=incremental))
add_rpm_histogram(report)
report.run()

# COMMAND ----------

# 1. No changes: the fact tables must not be rewritten.
before = versions(p, tables)
report.run()
rewritten = [t for t in tables if commits_since(p, t, before[t]).count()]
assert not rewritten, f"a run without changes rewrote fact tables: {rewritten}"

# COMMAND ----------

# 2. Container 5 is re-ingested with only the first half of its samples.
before = versions(p, tables)
mid = spark.sql(
    f"SELECT (start_ts + stop_ts) / 2 FROM {silver}container_metrics WHERE container_id = 5"
).first()[0]
spark.sql(f"DELETE FROM {silver}channels WHERE container_id = 5 AND tstart > {mid}")
spark.sql(
    f"UPDATE {silver}container_metrics SET last_modified = current_timestamp() "
    "WHERE container_id = 5"
)
report.run()

# The other containers' rows must not be rewritten (same rows incl. _created_at), and
# container 5's histogram must match its new events. Step 3 keeps the events, so the full
# run at the end also checks them.
assert_unchanged(p, before, "container_id != 5")
assert_histogram_covers_events(gold(p, "histogram_fact"), gold(p, "event_instance_fact"))

events = gold(p, "event_instance_fact").where("container_id = 5")
original = expected(p, "event_instance_fact", events.schema, test="rpm_histogram")
assert events.count() < original.where("container_id = 5").count(), "container 5 was not updated"
assert events.where(f"start_ts > {mid}").count() == 0, "container 5 has events after its end"

# COMMAND ----------

# 3. New histogram bins; the event is unchanged.
report = new_report(config(p, source=silver, incremental=incremental))
add_rpm_histogram(report, bin_width=250)
before = versions(p, tables)
report.run()
commits = {t: commits_since(p, t, before[t]).count() for t in tables}
assert commits["histogram_fact"] > 0, "histogram was not recomputed"
assert commits["event_instance_fact"] == 0, "event was rewritten"

# Reference: a full run of the same report on the same data.
ref = f"{p.prefix}_ref"
reference = new_report(config(p, source=silver, unity_sink={"table_prefix": ref}))
add_rpm_histogram(reference, bin_width=250)
reference.run()

for table in tables:
    check(p, table, gold(p, table), gold(p, table, prefix=ref))

# The finer bins split the same durations: per container they add up to the events and,
# except for container 5, to the 500-rpm histogram of the original data.
histogram = gold(p, "histogram_fact")
assert_histogram_covers_events(histogram, gold(p, "event_instance_fact"))


def totals(df):
    return df.where("container_id != 5").groupBy("container_id").agg(F.sum("hist_value"))


original = expected(p, "histogram_fact", histogram.schema, test="rpm_histogram")
assertDataFrameEqual(totals(histogram), totals(original), rtol=0, atol=1e-5)
