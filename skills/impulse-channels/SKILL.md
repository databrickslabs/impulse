---
name: impulse-channels
description: >
  Compute calculated (derived) channels in Impulse — new time-series signals built from existing
  channels and materialized at the same per-sample grain. Use when the user wants to "add a calculated
  channel", derive/persist a signal (e.g. "speed in km/h", "power = rpm × torque"), materialize a virtual
  signal into a queryable table, or run `solve_calculated_channels`. Covers the reporting-layer
  CalculatedChannel, the ad-hoc `QueryBuilder.solve_calculated_channels` endpoint, the
  calculated_channel_fact/dimension gold output, and the optional calculated_channel_metrics table.
---

# Impulse — calculated channels

A calculated channel is a **derived signal** computed from existing channels (see `impulse-tsal`) and
persisted at the same per-sample grain as the silver `channels` table — unlike an aggregation (see
`impulse-aggregations`), which summarizes a signal into bins or statistics. It is the *persisted, labeled*
form of a virtual signal.

**When to use — only when you want to persist or directly work with the derived time series itself.**
Aggregations and events accept any derived TSAL expression *directly* (e.g.
`StatsAggregator(input_expressions=[rpm * torque], ...)`, `BasicEvent(expr=speed > 100)`) and derive it
**on the fly** at solve time — they do **not** need a calculated channel to compute over a derived signal.
Reach for a `CalculatedChannel` when the derived signal is itself the deliverable: you want it materialized
to a queryable gold table, or returned as narrow silver-shaped rows via `solve_calculated_channels`.

**Every calculated channel must be registered with the report before computing:**

```python
report.add_calculated_channel(my_channel)
```

The wrapped TSAL expression **must evaluate to a `SampleSeries`** (a signal), not `Intervals` — validated
at construction (raises `ValueError`).

## CalculatedChannel

Couples a TSAL expression with an `identity` — an arbitrary key-value dict identifying the channel (stored
on the dimension, seeds `channel_id`). The expression is evaluated per container and exploded into narrow
rows (one per sample interval).

```python
from impulse_reporting.channels.calculated_channel import CalculatedChannel

veh_spd = report.get_db().query.channel(channel_name="Vehicle Speed Sensor")

speed_kmh = CalculatedChannel(
    name="speed_kmh",
    expr=veh_spd * 3.6,
    identity={"channel_name": "speed_kmh", "data_key": "CALC"},
    desc="Vehicle speed converted to km/h",
)
report.add_calculated_channel(speed_kmh)
```

| Parameter    | Type                   | Required | Description                                                                                       |
|--------------|------------------------|----------|---------------------------------------------------------------------------------------------------|
| `name`       | `str`                  | Yes      | Human-readable channel name; stored in `calculated_channel_dimension`.                            |
| `expr`       | `TimeSeriesExpression` | Yes      | Derived signal. **Must evaluate to `SampleSeries`.**                                              |
| `identity`   | `dict[str, str]`       | Yes      | Channel identity — any **non-empty** dict (arbitrary keys). Seeds `channel_id`; stored as a `map` on `calculated_channel_dimension`, not on fact rows. |
| `desc`       | `str`                  | No       | Description stored in `calculated_channel_dimension`.                                             |
| `attributes` | `Mapping[str, str]`    | No       | Free-form metadata; values coerced to strings.                                                    |

Multi-channel expressions (e.g. `eng_rpm * torque`) are time-base synchronized automatically; emitted
intervals are the intersection. The `channel_id` is a deterministic hash of the sorted `identity` — the
same value in both the fact and dimension tables, so they join on it.

## Ad-hoc: solve_calculated_channels

To compute channels without a report (see `impulse-analyze` for the ad-hoc query pattern), use the query
engine's narrow-solve endpoint directly. It returns a Spark DataFrame; it requires a `DefaultSolver` (the
default `BlobSolver` raises `NotImplementedError`).

```python
from impulse_query_engine.analyze.query.channels.calculated_channel import CalculatedChannel
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver

cc = CalculatedChannel(
    db.query.channel(channel_name="Vehicle Speed Sensor") * 3.6,
    {"channel_name": "speed_kmh", "data_key": "CALC"},
    # channel_id is a deterministic hash of the sorted identity.
)
df = db.query.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))
df.show()  # [container_id, channel_id, tstart, tend, value, identity]
#          identity is a map<string,string> holding the channel's identity dict
```

All selections in one call must be `CalculatedChannel`s; identity keys are arbitrary and need not match
across selections.

## Batching (in a Report)

Inside a `Report`, calculated channels are solved in **batches**, the same pattern events and
aggregations use (see `impulse-reporting`). The channels are partitioned by `query_engine.batch_size`
(max unique input selectors per batch), each batch is solved via `solve_calculated_channels` and
persisted as a temp table (`__impulse_temp_{run_id}_{batch_idx}` in the sink schema, or a Spark temp
view when sinkless), and the batches are unioned into the final `calculated_channel_fact`. The temp
tables share the `__impulse_temp_*` prefix, so they are cleaned up like the aggregation/event batches
(`unity_sink.cleanup_temp_tables`). This is internal orchestration; the `add_calculated_channel` API is
unchanged.

## Output schema

**calculated_channel_dimension** (one row per channel per report) — key columns: `channel_id`,
`report_id`, `channel_type` (`"CALCULATED_CHANNEL"`), `channel_description`, `channel_expression`
(TSAL string), `identity` (map), `definition_hash`, `attributes` (map).

**calculated_channel_fact** (one row per sample interval per container) — `container_id`, `channel_id`,
`tstart`, `tend`, `value`. Same narrow, run-length-encoded shape as the silver `channels` table. The
identity is **not** on the fact — it lives on the dimension; join on `channel_id` (and to
`measurement_dimension` on `container_id`; see `impulse-data-model`).

## Optional channel metrics table

`calculated_channel_fact` already matches the silver `channels` shape, so a calculated channel needs only a
companion `channel_metrics` table to become an Impulse silver source. Set
`config.calculated_channels.emit_channel_metrics = True` to also write `calculated_channel_metrics`, shaped
like silver `channel_metrics` (one row per `(container_id, channel_id)`, derived from the fact rows).

```python
from impulse_reporting.config.config_parser import CalculatedChannels

# in the ImpulseConfig:
calculated_channels = CalculatedChannels(
    emit_channel_metrics=True,
    attribute_columns=["unit"],          # attribute keys to surface as columns; default []
    kpis=["duration", "min", "max", "mean"],  # default; each must be a registered KPI
)
```

The schema is **dynamic**: fixed `container_id`, `channel_id`, `value_type` (`"double"`),
one column per configured KPI, one per identity key (union across the report's channels), and one per
`attribute_columns` entry. `null` fills a key a channel omits; an identity key wins over an attribute key of
the same name. KPIs are duration-weighted; an unknown KPI name is rejected at config validation. Adding a
new KPI is a one-line entry in `impulse_reporting.channels.calculated_channel_kpis.KPI_BUILDERS`.

## Incremental

Calculated channels reuse the report's incremental engine (see `impulse-reporting`). A definition change —
the `expr` string or the `identity` — reprocesses that channel across all containers (`replaceWhere` on
`channel_id`); unchanged definitions only recompute new/updated containers (`MERGE`). Renaming or editing
`desc`/`attributes` does not change the hash.
