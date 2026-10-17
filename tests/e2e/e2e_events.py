# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: events
# MAGIC Points-in-time, sequence and time-window events with their aggregations. Checks their
# MAGIC structure (point events have zero length, windows tile each container, ...) and the
# MAGIC expected rows.

# COMMAND ----------

import pyspark.sql.functions as F
from e2e_utils import channel, check, config, event_ids, gold, new_report, params

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

# The windows tile each container.
covered = instances("five_min").groupBy("container_id").agg(F.sum("length").alias("covered"))
durations = gold(p, "measurement_dimension").select(
    "container_id", (F.col("stop_ts") - F.col("start_ts")).alias("duration")
)
gaps = covered.join(durations, "container_id", "full").where(
    F.abs(F.coalesce("covered", F.lit(0.0)) - F.col("duration")) > 1e-6
)
assert gaps.count() == 0, "windows don't cover the containers"

# One value per point event and channel.
values = gold(p, "stats_aggregator_fact").where(F.col("aggregation_label") == "value")
assert values.count() == 2 * instances("slow_down").count(), "missing point values"

# COMMAND ----------

for table in ["event_instance_fact", "stats_aggregator_fact"]:
    check(p, table, gold(p, table))
