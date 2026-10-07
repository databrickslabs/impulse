---
sidebar_position: 1
title: Events
---

# Events

Events define time windows within measurement data that scope downstream aggregations. An event segments continuous
time-series recordings into meaningful intervals -- for example, "engine RPM between 2000 and 5000".

Every event used by an aggregation **must** be registered with the report via `add_event()` before `determine_report()`
is called.

```python
my_report.add_event(my_event)
```

:::note
If an event expression contains a UDF that declares
[container tags/metrics](../query_engine/tsal/defining_expressions.md#reading-container-level-metadata-inside-a-udf),
those values are injected as usual — the requirement propagates through the event.
:::

---

## BasicEvent

A `BasicEvent` derives event instances from a boolean TSAL expression. Each contiguous interval where the expression
evaluates to `True` becomes an event instance with a start and end timestamp.

```python
from impulse_reporting.events.basic_event import BasicEvent

rpm_event_expr = (eng_rpm > 2000) & (eng_rpm < 5000)

eng_rpm_event = BasicEvent(
    name="eng_rpm_event",
    expr=rpm_event_expr,
    desc="Engine RPM between 2000 and 5000",
    required_channels=["Engine RPM"],
)
```

### Parameters

| Parameter           | Type                   | Required | Description                                                                                          |
|---------------------|------------------------|----------|------------------------------------------------------------------------------------------------------|
| `name`              | `str`                  | Yes      | Unique event name. Used as identifier in fact and dimension tables.                                  |
| `expr`              | `TimeSeriesExpression` | Yes      | Boolean TSAL expression defining the event condition. Must evaluate to `Intervals`; validated at construction (raises `ValueError` otherwise). |
| `desc`              | `str`                  | No       | Human-readable description stored in the event dimension table.                                      |
| `required_channels` | `list[str]`            | No       | Channel names required for this event. Informational; stored in the event dimension table.           |
| `attributes`        | `Mapping[str, str]`    | No       | Free-form key-value metadata stored in the event dimension table (e.g. `limit_type`, `limit_direction`). Values are coerced to strings. |

### How it works

1. The `expr` is evaluated per measurement container by the solver.
2. Comparison operators on `SampleSeries` produce `Intervals` -- contiguous time windows where the condition holds.
3. Each interval becomes an **event instance** with a `start_ts`, `end_ts`, and a unique `event_instance_id`.
4. The event instances are written to the `event_instance_fact` table.
5. The event definition (name, description, expression, required channels) is written to the `event_dimension` table.

### Examples

**Simple threshold event:**

```python
high_speed = BasicEvent(
    name="high_speed",
    expr=veh_spd > 120,
    desc="Vehicle speed above 120 km/h",
    required_channels=["Vehicle Speed Sensor"],
)
```

**Multi-signal condition:**

```python
warm_and_fast = BasicEvent(
    name="warm_and_fast",
    expr=(amb_air_temp > 20) & (veh_spd > 80),
    desc="Warm ambient temperature and high speed",
    required_channels=["Ambient Air Temperature", "Vehicle Speed Sensor"],
)
```

---

## ContainerEvent

A `ContainerEvent` spans the full duration of each measurement container. It does not require a TSAL expression -- start
and end timestamps are taken directly from the `container_metrics` table.

This is useful when aggregations should run across complete measurements without any filtering.

```python
from impulse_reporting.events.container_event import ContainerEvent

container_event = ContainerEvent(
    name="container_event",
    desc="Full measurement container",
)
```

### Parameters

| Parameter    | Type                | Required | Description                                                                         |
|--------------|---------------------|----------|-------------------------------------------------------------------------------------|
| `name`       | `str`               | Yes      | Unique event name.                                                                  |
| `desc`       | `str`               | No       | Human-readable description.                                                         |
| `attributes` | `dict[str, str]`    | No       | Free-form key-value metadata stored in the event dimension table.                   |

### How it works

1. The solver reads `start_ts` and `stop_ts` from the `container_metrics` table.
2. Each container contributes exactly one event instance covering its full time span.
3. Event instances and metadata are written to the same fact and dimension tables as `BasicEvent`.

---

## SequenceOfEvents

A `SequenceOfEvents` event merges an ordered list of TSAL expressions into a single sequence by joining overlapping
consecutive intervals. Each expression must yield `Intervals`. When the next expression's interval starts before the
current one ends, the resulting sequence interval starts at the first interval's start and ends at the next interval's
end:

```
time --->
event_1: | ------------------- |
event_2:             | ------------- |
sequence:| ------------------------- |
```

```python
from impulse_reporting.events.sequence_of_events import SequenceOfEvents

idle_to_drive = SequenceOfEvents(
    name="idle_to_drive",
    expressions=[veh_spd == 0, veh_spd > 0],
    desc="Sequence: stationary followed by motion",
    required_channels=["Vehicle Speed Sensor"],
)
```

### Parameters

| Parameter           | Type                          | Required | Description                                                                                                                                                                  |
|---------------------|-------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`              | `str`                         | Yes      | Unique event name.                                                                                                                                                           |
| `expressions`       | `list[TimeSeriesExpression]`  | Yes      | Ordered list of expressions. Each must evaluate to `Intervals`; validated at construction (raises `ValueError` otherwise).                                                   |
| `desc`              | `str`                         | No       | Human-readable description.                                                                                                                                                  |
| `required_channels` | `list[str]`                   | No       | Channel names required for this event. Informational; stored in the event dimension table.                                                                                   |
| `max_overlap`       | `float`                       | No       | Maximum allowed overlap between consecutive intervals. Sequences whose overlap exceeds this value are skipped. Expressed in the same time unit as the underlying timestamps. |
| `attributes`        | `Mapping[str, str]`           | No       | Free-form key-value metadata stored in the event dimension table.                                                                                                            |

### How it works

1. Each expression in `expressions` is solved against the report's wide DataFrame.
2. Consecutive intervals are joined: the output interval spans the start of the first expression's interval to the end
   of the next expression's interval, when they overlap.
3. If `max_overlap` is set, candidate sequences whose overlap exceeds the threshold are discarded.
4. Each resulting interval is materialized as one event instance in the `event_instance_fact` table.

---

## PointsInTimeEvent

A `PointsInTimeEvent` derives event instances from a TSAL expression that evaluates to a
`PointsInTime` (a set of instants) — typically `channel.rising_edges()` or `channel.falling_edges()`.
Each instant becomes one **zero-duration** event instance (`start_ts == end_ts`), in contrast to
[`BasicEvent`](#basicevent), which produces durationed intervals.

```python
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent

eng_rpm = my_report.get_db().query.channel(channel_name="Engine RPM")
rpm_rising = PointsInTimeEvent(
    name="rpm_rising_edges",
    expr=eng_rpm.rising_edges(),
    desc="Instants where engine RPM rises",
)
my_report.add_event(rpm_rising)
```

### How it works

1. The expression is solved against the report's wide DataFrame, producing a flat array of timestamps.
2. Each timestamp is materialized as one event instance with `start_ts == end_ts` in the shared
   `event_instance_fact` table.

The expression **must** evaluate to a `PointsInTime`; otherwise construction raises a `ValueError`
(use [`BasicEvent`](#basicevent) for an `Intervals` condition). Point instances share
`event_instance_fact` with interval events — distinguish them by joining `event_dimension` on
`event_id` and filtering `event_type == "POINTS_IN_TIME_EVENT"`, or by the `start_ts == end_ts` property.

---

## TimeWindowEvent

A `TimeWindowEvent` divides each matching container into **consecutive fixed-duration windows** --
one event instance per slice. Unlike `ContainerEvent` (one instance for the whole container), it
produces repeated windows (e.g. one-minute, ten-minute, hourly, or daily segments) across every
matching container. No signal expression is needed: the window boundaries are derived from the
container's `start_ts` / `stop_ts` on the `container_metrics` table.

```python
from impulse_reporting.events.time_window_event import TimeWindowEvent

ten_minute_windows = TimeWindowEvent(
    name="ten_minute_windows",
    window_length=600_000,  # in the same time unit as the underlying timestamps (see note)
    desc="Ten-minute segments across each measurement",
)
my_report.add_event(ten_minute_windows)
```

### Parameters

| Parameter           | Type                | Required | Description                                                                                                     |
|---------------------|---------------------|----------|-----------------------------------------------------------------------------------------------------------------|
| `name`              | `str`               | Yes      | Unique event name.                                                                                              |
| `window_length`     | `float`             | Yes      | Fixed window length, **in the same time unit as the underlying timestamps** (e.g. milliseconds-since-epoch). Must be strictly positive and finite; validated at construction. |
| `desc`              | `str`               | No       | Human-readable description.                                                                                     |
| `required_channels` | `list[str]`         | No       | Channel names required for this event. Informational; stored in the event dimension table.                     |
| `attributes`        | `Mapping[str, str]` | No       | Free-form key-value metadata. `window_length` is surfaced here automatically (without overriding a user key).  |
| `max_windows_per_container` | `int`       | No       | Upper bound on the windows per container (default `1_000_000`). A container that would exceed it fails the report with an error naming the limit, which usually means `window_length` is in the wrong unit for the timestamps. Raise it for very long containers with short windows. Not part of the definition hash. |

:::note
`window_length` follows the same convention as `SequenceOfEvents.max_overlap`: it is expressed in
the same time unit as the channel timestamps (microseconds-since-epoch in the sample data), not
seconds or any derived unit. So 60 one-minute windows over millisecond timestamps use
`window_length=60_000`.

The windows are computed in the time frame of the channel timestamps, set by
[`solver_config.channel_time_unit`, `channel_time_origin` and `container_time_unit`](../../config/configuration.md#solver-column-mappings-and-filters):

- If `container_metrics.start_ts`/`stop_ts` are `TIMESTAMP` columns, set `channel_time_unit` to the
  unit of the channel timestamps (`tstart`/`tend`, or `timestamp` for RAW data; e.g. `"s"`), and
  `window_length` is expressed in it. Without it, the report fails with an error naming the
  setting.
- If the channel timestamps are relative to the container start (e.g. seconds since the
  recording started), also set `channel_time_origin="container_start"`. The windows then run from
  `0` to `stop_ts - start_ts`.
- If numeric `start_ts`/`stop_ts` are in another unit than the channel timestamps (e.g. epoch ms
  boundaries, µs samples), set `container_time_unit` to their unit (e.g. `"ms"`) and
  `channel_time_unit` to the channels' (e.g. `"us"`). Otherwise no window overlaps the samples.

Only the windows use these settings: `ContainerEvent`, `measurement_dimension` and UDFs that read
`start_ts`/`stop_ts` keep seeing the original values. The channel time frame is part of the
event's definition (and of the aggregations scoped to it), so changing it recomputes them over all
containers in incremental mode.
:::

### How it works

1. The event resolves the matching containers through the report's container filters (like
   `ContainerEvent`), reads `start_ts` and `stop_ts` from the `container_metrics` table, and
   tiles `[start_ts, stop_ts]`, in the channel time frame, into consecutive windows of length
   `window_length`. The window instances in `event_instance_fact` are in that frame too.
2. The **final window is clamped** to `stop_ts` when the last full window would overrun it; any
   zero-length trailing slice is dropped (every instance satisfies `start_ts < end_ts`).
   Containers whose `start_ts` or `stop_ts` is null, NaN or infinite get no windows.
3. Each window becomes one **event instance**, written to the shared `event_instance_fact` table.
   Its `event_instance_id` hashes the container, the event name and the window's position in
   the container (0, 1, 2, ...).
4. An aggregation scoped to the event (`StatsAggregator(..., event=time_window_event)`) computes
   its statistic **once per window** and joins back to those instances.

:::note
The windows are computed from `container_metrics` alone, so **every** container that matches the
report's filters gets windows, whether or not it has channel data and whether or not an
aggregation is scoped to the event. An aggregation scoped to the event uses the same window
function in the query engine, so its per-window rows carry the same `event_instance_id` values. For the per-window values to be meaningful, the container boundaries must share the channel
samples' time base (as they do in real measurement data).
:::

:::note
Window boundaries are stored as doubles (`start_ts` / `end_ts`), like every other event type. Epoch
timestamps in nanoseconds exceed the range doubles represent exactly, so their window boundaries
are rounded to about 256 ns. The `event_instance_id` depends on the window's position, not on its
boundaries, so the rounding does not affect how aggregations join to the windows.
:::

## Event output schema

### event_dimension

Stores event definitions (one row per event per report).

| Column              | Type                | Description                                                                 |
|---------------------|---------------------|-----------------------------------------------------------------------------|
| `event_id`          | `int`               | Unique event identifier (CRC32 hash of name + expression).                  |
| `report_id`         | `int`               | Report identifier.                                                          |
| `event_type`        | `str`               | `"BASIC_EVENT"`, `"CONTAINER_EVENT"`, `"SEQUENCE_OF_EVENTS"`, `"POINTS_IN_TIME_EVENT"`, or `"TIME_WINDOW_EVENT"`. |
| `event_name`        | `str`               | Event name.                                                                 |
| `event_description` | `str`               | Event description.                                                          |
| `required_channels` | `array[str]`        | Required channel names (null for `ContainerEvent`).                         |
| `event_expression`  | `str`               | String representation of the TSAL expression (`"NA"` for `ContainerEvent`). |
| `definition_hash`   | `long`              | Hash of the event definition; used by incremental processing to detect definition changes. |
| `attributes`        | `map[str, str]`     | Free-form key-value metadata supplied via the `attributes` constructor argument. |

### event_instance_fact

Stores materialized event occurrences (one row per event instance per container).

| Column              | Type   | Description                              |
|---------------------|--------|------------------------------------------|
| `container_id`      | `int`  | Container identifier.                    |
| `event_instance_id` | `long` | Unique instance identifier (xxHash64). |
| `event_id`          | `int`  | Foreign key to `event_dimension`.        |
| `start_ts`          | `long` | Event instance start timestamp.          |
| `end_ts`            | `long` | Event instance end timestamp.            |

Interval events satisfy `start_ts < end_ts`; `PointsInTimeEvent` instances are zero-duration
(`start_ts == end_ts`).

---

## Choosing between event types

| Criterion                        | BasicEvent                                              | ContainerEvent                                    | SequenceOfEvents                                                          | PointsInTimeEvent                                          | TimeWindowEvent                                             |
|----------------------------------|---------------------------------------------------------|---------------------------------------------------|---------------------------------------------------------------------------|------------------------------------------------------------|-------------------------------------------------------------|
| Requires a TSAL expression       | Yes (one)                                               | No                                                | Yes (ordered list)                                                        | Yes (one, must evaluate to `PointsInTime`)                 | No (needs a `window_length`)                                |
| Multiple instances per container | Yes (one per matching interval)                         | No (always one per container)                     | Yes (one per joined sequence)                                             | Yes (one per instant)                                      | Yes (one per fixed window)                                  |
| Instance duration                | Interval (`start_ts < end_ts`)                          | Full container window                             | Interval (`start_ts < end_ts`)                                            | Zero (`start_ts == end_ts`)                                | Fixed window (last clamped to container end)                |
| Use case                         | Signal-based conditions, operating bands, distance bins | Full-run aggregations, container-level statistics | State transitions and multi-step patterns where consecutive states overlap | Edge/instant events, e.g. `rising_edges()` / `falling_edges()` | Repeated time segments (1-min / 10-min / hourly / daily)    |
