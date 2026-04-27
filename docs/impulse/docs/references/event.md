---
sidebar_position: 3
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
from mda_reporting.events.basic_event import BasicEvent

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
from mda_reporting.events.container_event import ContainerEvent

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

## Event output schema

### event_dimension

Stores event definitions (one row per event per report).

| Column              | Type                | Description                                                                 |
|---------------------|---------------------|-----------------------------------------------------------------------------|
| `event_id`          | `int`               | Unique event identifier (CRC32 hash of name + expression).                  |
| `report_id`         | `int`               | Report identifier.                                                          |
| `event_type`        | `str`               | `"BASIC_EVENT"` or `"CONTAINER_EVENT"`.                                     |
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

## Choosing between BasicEvent and ContainerEvent

| Criterion                        | BasicEvent                                              | ContainerEvent                                    |
|----------------------------------|---------------------------------------------------------|---------------------------------------------------|
| Requires a TSAL expression       | Yes                                                     | No                                                |
| Multiple instances per container | Yes (one per matching interval)                         | No (always one per container)                     |
| Use case                         | Signal-based conditions, operating bands, distance bins | Full-run aggregations, container-level statistics |
