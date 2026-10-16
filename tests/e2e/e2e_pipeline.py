# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: pipeline
# MAGIC Adds a container event, per-container statistics and an RPM x speed heatmap to the
# MAGIC baseline report.

# COMMAND ----------

from e2e_utils import add_rpm_histogram, channel, check, config, gold, new_report, params

from impulse_reporting.aggregations.histogram2d import Histogram2DDuration
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.core.page import Page
from impulse_reporting.events.container_event import ContainerEvent

p = params()
report = new_report(config(p))
high_rpm = add_rpm_histogram(report)

names = ["Engine RPM", "Vehicle Speed Sensor", "Engine Coolant Temperature"]
rpm, speed, coolant = (channel(report, name) for name in names)
container = ContainerEvent(name="container", desc="Whole measurement")
report.add_event(container)

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

for table in ["stats_aggregator_fact", "histogram2d_fact"]:
    check(p, table, gold(p, table))
