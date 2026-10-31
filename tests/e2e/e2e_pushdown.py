# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: pushdown
# MAGIC Runs a report of `resample` expressions without Spark pushdown, with pushdown, and with
# MAGIC pushdown on small chunks (so channels span many chunks). The pushdown runs must log a
# MAGIC pushdown plan and solve the calculated channels without the pandas UDF, and their gold
# MAGIC tables must equal those of the run without pushdown. Covers calculated channels computed
# MAGIC in Spark (one channel at two rates, so its samples fan out into two variants), pushed
# MAGIC subtrees inside events and aggregations, a nested op above a pushdown, and a channel also
# MAGIC used raw, which is not pushed down.

# COMMAND ----------

import logging
from contextlib import contextmanager

import pyspark.sql.functions as F
from e2e_utils import channel, check, config, event_ids, gold, new_report, params
from pyspark.sql import Window
from pyspark.testing import assertDataFrameEqual

from impulse_query_engine.analyze.query.optimizer import pipeline
from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.channels.calculated_channel import CalculatedChannel
from impulse_reporting.core.page import Page
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.events.container_event import ContainerEvent

p = params()
RATE = 1.0  # The source timestamps are epoch seconds; an integral rate gives an exact grid.
SLOW_RATE = 5 * RATE  # A second rate on speed: two pushed-down variants of one channel.
SOLVER_LOGGER = "impulse_query_engine.analyze.query.solvers.default_solver"
PLAN_LOG = "Spark pushdown plan"
NATIVE_LOG = "Calculated channels solved natively in Spark"
#: Rate of each calculated channel, by channel id (filled by build()).
CALCULATED_RATES = {}


def build(prefix, pushdown):
    cfg = config(
        p,
        calculated_channels={"emit_channel_metrics": True},
        query_engine={"enable_pushdown": pushdown},
    )
    cfg["unity_sink"]["table_prefix"] = prefix
    report = new_report(cfg)
    rpm, speed, coolant = (
        channel(report, name)
        for name in ["Engine RPM", "Vehicle Speed Sensor", "Engine Coolant Temperature"]
    )

    # Only resamples, so the calculated channels are computed in Spark without the UDF.
    for name, source, rate in [
        ("speed_1s", speed, RATE),
        ("coolant_1s", coolant, RATE),
        ("speed_5s", speed, SLOW_RATE),
    ]:
        calculated = CalculatedChannel(
            name=name, expr=source.resample(rate), identity={"channel_name": name}
        )
        report.add_calculated_channel(calculated)
        CALCULATED_RATES[calculated.get_id()] = rate

    high_rpm = BasicEvent(name="high_rpm", expr=rpm > 2000)
    fast = BasicEvent(name="fast", expr=speed.resample(RATE) > 100)
    container = ContainerEvent(name="container", desc="Whole measurement")
    for event in [high_rpm, fast, container]:
        report.add_event(event)

    page = Page(page_number=1)
    page.add_aggregation(
        HistogramDuration(
            name="speed_at_high_rpm",
            base_expr=speed.resample(RATE),
            bins=[float(x) for x in range(0, 201, 20)],
            event=high_rpm,
            channel_name="Vehicle Speed Sensor",
            bins_unit="km/h",
            values_unit="s",
        )
    )
    page.add_aggregation(
        StatsAggregator(
            name="resampled_stats",
            # The speed change per second: an op above the pushed-down resample.
            input_expressions=[coolant.resample(RATE), speed.resample(RATE).diff()],
            channel_names=["coolant_1s", "speed_1s_diff"],
            statistics=["min", "mean", "max"],
            event=container,
        )
    )
    # rpm is also used raw in the same solve, so its resample runs in pandas.
    page.add_aggregation(
        StatsAggregator(
            name="raw_and_resampled_rpm",
            input_expressions=[rpm, rpm.resample(RATE)],
            channel_names=["Engine RPM", "rpm_1s"],
            statistics=["min", "mean", "max"],
            event=container,
        )
    )
    report.add_page(page)
    return report


@contextmanager
def solver_logs():
    """Messages the solver logs at INFO and above while the block runs."""
    messages = []
    handler = logging.Handler(logging.INFO)
    handler.emit = lambda record: messages.append(record.getMessage())
    logger = logging.getLogger(SOLVER_LOGGER)
    level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        yield messages
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)


def run(prefix, pushdown, chunk_rows=None):
    """Run the report into `<prefix>_*`; returns the solver's log messages."""
    report = build(prefix, pushdown)
    default_chunk_rows = pipeline.CHUNK_ROWS
    if chunk_rows is not None:
        pipeline.CHUNK_ROWS = chunk_rows
    try:
        with solver_logs() as messages:
            report.run()
    finally:
        pipeline.CHUNK_ROWS = default_chunk_rows
    return messages


ref, chunked = f"{p.prefix}_ref", f"{p.prefix}_chunked"
logs = {
    ref: run(ref, pushdown=False),
    p.prefix: run(p.prefix, pushdown=True),
    # About 40 chunks per channel instead of one.
    chunked: run(chunked, pushdown=True, chunk_rows=1000),
}

# COMMAND ----------

assert not any(PLAN_LOG in m or NATIVE_LOG in m for m in logs[ref]), "pushdown without opt-in"
for prefix in [p.prefix, chunked]:
    plans = [m for m in logs[prefix] if m.startswith(PLAN_LOG)]
    assert plans, f"{prefix}: no pushdown plan"
    assert all(f"resample(sample_rate={RATE})" in m for m in plans), plans
    assert any(f"resample(sample_rate={SLOW_RATE})" in m for m in plans), plans
    assert any(NATIVE_LOG in m for m in logs[prefix]), f"{prefix}: calculated channels in UDF"
    print(f"{prefix}: {plans}")

# COMMAND ----------

# Sample rows, events and pandas-computed aggregations are identical (compared in Spark, as
# the calculated channels have many rows); the calculated channel metrics are sums in Spark,
# whose order is not deterministic.
EXACT = [
    "calculated_channel_fact",
    "event_instance_fact",
    "histogram_fact",
    "stats_aggregator_fact",
]
for prefix in [p.prefix, chunked]:
    for table in EXACT:
        actual, expected = gold(p, table, prefix=prefix), gold(p, table, prefix=ref)
        for name, diff in [
            ("extra", actual.exceptAll(expected)),
            ("missing", expected.exceptAll(actual)),
        ]:
            assert diff.count() == 0, f"{prefix}_{table}: {name} rows {diff.take(5)}"
        print(f"{prefix}_{table}: equal to {ref} ({actual.count()} rows)")
    assertDataFrameEqual(
        gold(p, "calculated_channel_metrics", prefix=prefix),
        gold(p, "calculated_channel_metrics", prefix=ref),
        rtol=1e-12,
        atol=0,
    )

# COMMAND ----------

# The calculated channels lie on their grids (1 s or 5 s): every sample lasts exactly the rate,
# except the last one of each continuous block (whose end isn't the next sample's start).
series = Window.partitionBy("container_id", "channel_id").orderBy("tstart")
rate = F.create_map(
    *[x for cid, r in CALCULATED_RATES.items() for x in (F.lit(cid).cast("long"), F.lit(r))]
)[F.col("channel_id").cast("long")]
samples = gold(p, "calculated_channel_fact").withColumn("next", F.lead("tstart").over(series))
duration = F.col("tend") - F.col("tstart")
off_grid = samples.where(
    (duration != rate) & ((duration > rate) | (F.col("next") == F.col("tend")))
)
assert off_grid.count() == 0, f"samples off the grid: {off_grid.take(5)}"
n_series = samples.select("container_id", "channel_id").distinct().count()
n_containers = gold(p, "measurement_dimension").count()
assert n_series == len(CALCULATED_RATES) * n_containers, "missing series"

ids = event_ids(p)
fast = gold(p, "event_instance_fact").where(F.col("event_id") == ids["fast"])
assert fast.agg(F.sum(F.col("end_ts") - F.col("start_ts"))).first()[0] > 0, "no fast events"
assert gold(p, "histogram_fact").agg(F.sum("hist_value")).first()[0] > 0, "empty histogram"

# COMMAND ----------

# The calculated channel samples are covered by their metrics and the comparison above.
for table in [*EXACT[1:], "calculated_channel_metrics"]:
    check(p, table, gold(p, table))
