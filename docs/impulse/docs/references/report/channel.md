---
sidebar_position: 3
title: Channels
---

# Channels

A calculated channel is a **derived time-series signal** computed from existing channels and persisted
alongside them — for example, `speed_kmh = raw_speed * 3.6`, or the sum of two sensors. Unlike an
aggregation (which summarizes a signal into bins or statistics), a calculated channel materializes a new
signal at the same per-sample grain as the silver `channels` table, so it can be queried and charted like
any physical channel.

A calculated channel wraps a TSAL expression (see [TSAL](../query_engine/tsal/index.md)) that **must
evaluate to a `SampleSeries`**. This is the *persisted, labeled* form of the ephemeral
[virtual signals](../query_engine/tsal/defining_expressions.md#virtual-signals) you can build ad-hoc in a
query.

Every calculated channel used in a report **must** be registered with the report via
`add_calculated_channel()` before `determine_report()` is called.

```python
my_report.add_calculated_channel(my_channel)
```

---

## CalculatedChannel

A `CalculatedChannel` couples a TSAL expression with an **identity** — an arbitrary key-value dict that
identifies the channel and seeds its deterministic `channel_id`. When the report is solved, the wrapped
expression is evaluated per measurement container and exploded into narrow rows matching the silver
`channels` shape. The identity itself is stored once on the channel dimension (as a `map`), not repeated
on every fact row.

```python
from impulse_reporting.channels.calculated_channel import CalculatedChannel

speed_kmh = CalculatedChannel(
    name="speed_kmh",
    expr=veh_spd * 3.6,
    identity={"channel_name": "speed_kmh", "data_key": "CALC"},
    desc="Vehicle speed converted to km/h",
)
```

### Parameters

| Parameter    | Type                   | Required | Description                                                                                                                                                             |
|--------------|------------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`       | `str`                  | Yes      | Human-readable channel name, stored in the channel dimension table.                                                                                                     |
| `expr`       | `TimeSeriesExpression` | Yes      | TSAL expression defining the derived signal. Must evaluate to `SampleSeries`; validated at construction (raises `ValueError` otherwise).                                 |
| `identity`   | `dict[str, str]`       | Yes      | Channel identity. Any **non-empty** dict (keys are arbitrary, e.g. `channel_name` / `data_key`). Seeds the deterministic `channel_id` and is stored as a `map` on `calculated_channel_dimension` (joined to the fact via `channel_id`); not repeated on fact rows. |
| `desc`       | `str`                  | No       | Human-readable description stored in the channel dimension table.                                                                                                       |
| `attributes` | `Mapping[str, str]`    | No       | Free-form key-value metadata stored in the channel dimension table. Values are coerced to strings.                                                                       |

### How it works

1. The `expr` is evaluated per measurement container by the solver, yielding a `SampleSeries` for each container.
2. Calculated channels take their **own narrow solve path** (`QueryBuilder.solve_calculated_channels`), distinct from the wide batch solve used by events and aggregations. Multi-channel expressions (e.g. `rpm + speed`) are time-base synchronized automatically; emitted intervals are the intersection.
3. Each series is exploded into one row per sample interval: `container_id, channel_id, tstart, tend, value`. The `identity` is **not** on the fact — it lives on the dimension, joined via `channel_id`.
4. The `channel_id` is a deterministic hash of the sorted `identity` — the same value appears in both the fact and dimension tables, so they join directly.
5. The sample rows are written to the `calculated_channel_fact` table; the channel definition (name, description, expression, identity) is written to the `calculated_channel_dimension` table.

:::note Requires a DefaultSolver
The calculated-channels solve path is implemented by `DefaultSolver`. A `Report` uses `DefaultSolver`
automatically; if you call `solve_calculated_channels` directly, pass a `DefaultSolver` — the default
`BlobSolver` raises `NotImplementedError`.
:::

### Examples

**Unit conversion (single channel):**

```python
speed_kmh = CalculatedChannel(
    name="speed_kmh",
    expr=veh_spd * 3.6,
    identity={"channel_name": "speed_kmh", "data_key": "CALC"},
    desc="Vehicle speed converted to km/h",
)
```

**Derived signal from multiple channels:**

```python
power = CalculatedChannel(
    name="power_w",
    expr=eng_rpm * torque,
    identity={"channel_name": "power_w", "data_key": "CALC"},
    desc="Mechanical power (RPM × torque), time-synchronized across both inputs",
)
```

---

## Calculated channel output schema

### calculated_channel_dimension

Stores channel definitions (one row per calculated channel per report).

| Column                | Type              | Description                                                                                  |
|-----------------------|-------------------|----------------------------------------------------------------------------------------------|
| `channel_id`          | `long`            | Unique channel identifier (deterministic hash of the sorted `identity`).                     |
| `report_id`           | `int`             | Report identifier.                                                                           |
| `channel_type`        | `str`             | `"CALCULATED_CHANNEL"`.                                                                       |
| `channel_name`        | `str`             | Channel name (from `identity["channel_name"]`).                                              |
| `channel_description` | `str`             | Channel description.                                                                         |
| `channel_expression`  | `str`             | String representation of the wrapped TSAL expression (encodes identity + expression).        |
| `identity`            | `map[str, str]`   | Full identity dict.                                                                          |
| `definition_hash`     | `long`            | Hash of the channel definition; used by incremental processing to detect definition changes. |
| `attributes`          | `map[str, str]`   | Free-form key-value metadata supplied via the `attributes` constructor argument.             |

### calculated_channel_fact

Stores the materialized derived signal (one row per sample interval per container) — the same narrow,
run-length-encoded shape as the silver `channels` table.

| Column         | Type     | Description                                                     |
|----------------|----------|-----------------------------------------------------------------|
| `container_id` | `int`    | Container identifier. Type is inherited from the silver source. |
| `channel_id`   | `long`   | Foreign key to `calculated_channel_dimension`.                  |
| `tstart`       | `long`   | Sample interval start timestamp.                                |
| `tend`         | `long`   | Sample interval end timestamp.                                  |
| `value`        | `double` | Computed value over the interval.                               |
| `channel_name` | `str`    | Identity column (from `identity`).                              |
| `data_key`     | `str`    | Identity column (from `identity`).                              |

Both tables carry the configurable `table_prefix` (e.g. `{prefix}_calculated_channel_fact`). See the
[gold layer schema](../../data_model/gold_layer_event_normalized.md) for how they fit the star schema, and
[incremental processing](./index.md#incremental-processing) for how definition changes are reprocessed.
