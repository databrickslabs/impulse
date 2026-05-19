---
sidebar_position: 4
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
| `expr`              | `TimeSeriesExpression` | Yes      | Boolean TSAL expression defining the event condition. Must resolve to `Intervals` at execution time. |
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
| `expressions`       | `list[TimeSeriesExpression]`  | Yes      | Ordered list of expressions. Each must resolve to `Intervals`.                                                                                                               |
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

## Event output schema

### event_dimension

Stores event definitions (one row per event per report).

| Column              | Type                | Description                                                                 |
|---------------------|---------------------|-----------------------------------------------------------------------------|
| `event_id`          | `int`               | Unique event identifier (CRC32 hash of name + expression).                  |
| `report_id`         | `int`               | Report identifier.                                                          |
| `event_type`        | `str`               | `"BASIC_EVENT"`, `"CONTAINER_EVENT"`, or `"SEQUENCE_OF_EVENTS"`.             |
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
| `event_instance_id` | `long` | Unique instance identifier (CRC32 hash). |
| `event_id`          | `int`  | Foreign key to `event_dimension`.        |
| `start_ts`          | `long` | Event instance start timestamp.          |
| `end_ts`            | `long` | Event instance end timestamp.            |

---

## Choosing between event types

| Criterion                        | BasicEvent                                              | ContainerEvent                                    | SequenceOfEvents                                                          |
|----------------------------------|---------------------------------------------------------|---------------------------------------------------|---------------------------------------------------------------------------|
| Requires a TSAL expression       | Yes (one)                                               | No                                                | Yes (ordered list)                                                        |
| Multiple instances per container | Yes (one per matching interval)                         | No (always one per container)                     | Yes (one per joined sequence)                                             |
| Use case                         | Signal-based conditions, operating bands, distance bins | Full-run aggregations, container-level statistics | State transitions and multi-step patterns where consecutive states overlap |
