# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: pipeline
# MAGIC Adds a container event, per-container statistics, an RPM x speed heatmap and a
# MAGIC calculated channel to the baseline report. Statistics of the calculated channel's
# MAGIC expression must agree with and without an event, and with its persisted samples.

# COMMAND ----------

import pyspark.sql.functions as F
from e2e_utils import add_rpm_histogram, channel, check, config, gold, new_report, params
from pyspark.testing import assertDataFrameEqual

from impulse_reporting.aggregations.histogram2d import Histogram2DDuration
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.channels.calculated_channel import CalculatedChannel
from impulse_reporting.core.page import Page
from impulse_reporting.events.container_event import ContainerEvent

p = params()
report = new_report(config(p, calculated_channels={"emit_channel_metrics": True}))
high_rpm = add_rpm_histogram(report)

names = ["Engine RPM", "Vehicle Speed Sensor", "Engine Coolant Temperature"]
rpm, speed, coolant = (channel(report, name) for name in names)
container = ContainerEvent(name="container", desc="Whole measurement")
report.add_event(container)

delta = channel(report, "Engine Coolant Temperature") - channel(report, "Intake Air Temperature")
report.add_calculated_channel(
    CalculatedChannel(name="coolant_minus_intake", expr=delta, identity={"channel_name": "delta"})
)

page = Page(page_number=2)
page.add_aggregation(
    StatsAggregator(
        name="container_stats",
        input_expressions=[rpm, speed, coolant],
        channel_names=names,
        statistics=["min", "mean", "max"],
        event=container,
    )
)
page.add_aggregation(
    StatsAggregator(
        name="delta_stats",
        input_expressions=[delta],
        channel_names=["coolant_minus_intake"],
        statistics=["min", "mean", "max"],
        event=container,
    )
)
# The same statistics without an event (the whole series).
page.add_aggregation(
    StatsAggregator(
        name="delta_stats_unscoped",
        input_expressions=[delta],
        channel_names=["coolant_minus_intake"],
        statistics=["min", "mean", "max"],
    )
)
page.add_aggregation(
    Histogram2DDuration(
        name="rpm_speed",
        x_expr=rpm,
        y_expr=speed,
        x_bins=[float(x) for x in range(2000, 5001, 500)],
        y_bins=[float(x) for x in range(0, 201, 20)],
        event=high_rpm,
        x_channel_name="Engine RPM",
        y_channel_name="Vehicle Speed Sensor",
        values_unit="s",
    )
)
report.add_page(page)
report.run()

# COMMAND ----------


def delta_stats(scoped):
    rows = gold(p, "stats_aggregator_fact").where(F.col("channel_name") == "coolant_minus_intake")
    return (
        rows.where(F.col("event_id").isNotNull() if scoped else F.col("event_id").isNull())
        .groupBy(F.col("container_id").cast("long").alias("container_id"))
        .pivot("aggregation_label", ["min", "mean", "max"])
        .agg(F.first("statistic_value"))
    )


# StatsAggregator only counts samples overlapping [start, end), so the zero-length last sample
# of each container (at its stop time) is skipped with or without an event. The calculated
# channel's metrics include it; they are covered by their expected file.
scoped = delta_stats(scoped=True)
assertDataFrameEqual(delta_stats(scoped=False), scoped, rtol=1e-9)

fact = gold(p, "calculated_channel_fact").withColumn("dur", F.col("tend") - F.col("tstart"))
value = F.when(~F.isnan("value"), F.col("value"))
from_fact = (
    fact.where("dur > 0")
    .groupBy(F.col("container_id").cast("long").alias("container_id"))
    .agg(
        F.min(value).alias("min"),
        # Like StatsAggregator (nansum(v * d) / nansum(d)): NaN samples add nothing to the sum
        # but their duration counts.
        (F.sum(value * F.col("dur")) / F.sum("dur")).alias("mean"),
        F.max(value).alias("max"),
    )
)
assertDataFrameEqual(from_fact, scoped, rtol=1e-9)

# COMMAND ----------

for table in ["stats_aggregator_fact", "histogram2d_fact", "calculated_channel_metrics"]:
    check(p, table, gold(p, table))
