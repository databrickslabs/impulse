# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: events
# MAGIC Points-in-time, sequence and time-window events with their aggregations. Checks their
# MAGIC structure (point events have zero length, windows tile each container, ...) and the
# MAGIC expected rows.

# COMMAND ----------

import pyspark.sql.functions as F
from e2e_utils import channel, check, config, event_ids, gold, new_report, params
from pyspark.sql import Window

from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.core.page import Page
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent
from impulse_reporting.events.sequence_of_events import SequenceOfEvents
from impulse_reporting.events.time_window_event import TimeWindowEvent

p = params()
report = new_report(config(p))
rpm, speed = channel(report, "Engine RPM"), channel(report, "Vehicle Speed Sensor")

slow_down = PointsInTimeEvent(name="slow_down", expr=(speed > 100).end_points())
fast_then_high_rpm = SequenceOfEvents(
    name="fast_then_high_rpm", expressions=[speed > 100, rpm > 3000]
)
five_min = TimeWindowEvent(name="five_min", window_length=300.0)
for event in [slow_down, fast_then_high_rpm, five_min]:
    report.add_event(event)

page = Page(page_number=1)
page.add_aggregation(
    PointValueAggregator(
        name="values_at_slow_down",
        input_expressions=[rpm, speed],
        channel_names=["Engine RPM", "Vehicle Speed Sensor"],
        event=slow_down,
    )
)
page.add_aggregation(
    StatsAggregator(
        name="five_min_stats",
        input_expressions=[rpm],
        channel_names=["Engine RPM"],
        statistics=["mean", "max"],
        event=five_min,
    )
)
report.add_page(page)
report.run()

# COMMAND ----------

ids = event_ids(p)
events = gold(p, "event_instance_fact").withColumn("length", F.col("end_ts") - F.col("start_ts"))


def instances(name):
    return events.where(F.col("event_id") == ids[name])


assert instances("slow_down").count() > 0, "no slow_down events"
assert instances("slow_down").where("length != 0").count() == 0, "point events with a length"
assert instances("fast_then_high_rpm").where("length <= 0").count() == 0, "empty sequences"
assert instances("five_min").where("length > 300.000001").count() == 0, "windows over 5 min"

# The windows tile each container: they start at its start, each starts where the previous
# ends, the last ends at its stop, and there are ceil(duration / 300) of them.
by_start = Window.partitionBy("container_id").orderBy("start_ts")
windows = instances("five_min").withColumn("prev_end", F.lag("end_ts").over(by_start))
gaps = windows.where(F.abs(F.col("start_ts") - F.col("prev_end")) > 1e-6)
assert gaps.count() == 0, f"gaps or overlaps between windows: {gaps.take(5)}"

containers = gold(p, "measurement_dimension").select("container_id", "start_ts", "stop_ts")
tiling = windows.groupBy("container_id").agg(
    F.min("start_ts").alias("first"), F.max("end_ts").alias("last"), F.count("*").alias("n")
)
bad = containers.join(tiling, "container_id", "left").where(
    F.col("n").isNull()
    | (F.abs(F.col("first") - F.col("start_ts")) > 1e-6)
    | (F.abs(F.col("last") - F.col("stop_ts")) > 1e-6)
    | (F.col("n") != F.ceil((F.col("stop_ts") - F.col("start_ts")) / 300))
)
assert bad.count() == 0, f"windows don't tile the containers: {bad.take(5)}"

# One value per point event and channel; speed has just dropped to 100 or below.
stats = gold(p, "stats_aggregator_fact")
values = stats.where(F.col("aggregation_label") == "value")
per_instance = (
    instances("slow_down")
    .join(values, ["container_id", "event_instance_id"], "left")
    .groupBy("event_instance_id", "channel_name")
    .count()
)
assert per_instance.where("count != 1 OR channel_name IS NULL").count() == 0, "point values"
assert values.count() == 2 * instances("slow_down").count(), "point values of other events"
too_fast = values.where("channel_name = 'Vehicle Speed Sensor' AND statistic_value > 100")
assert too_fast.count() == 0, f"speed above 100 at slow_down: {too_fast.take(5)}"

window_stats = (
    stats.join(instances("five_min").select("event_instance_id"), "event_instance_id", "semi")
    .groupBy("event_instance_id")
    .pivot("aggregation_label", ["mean", "max"])
    .agg(F.first("statistic_value"))
)
assert window_stats.where("mean > max + 1e-9").count() == 0, "window mean above its max"

# COMMAND ----------

for table in ["event_instance_fact", "stats_aggregator_fact"]:
    check(p, table, gold(p, table))
