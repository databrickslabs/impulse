---
name: impulse-events
description: >
  Define event windows in Impulse — the time spans that scope aggregations. Use when the user wants to
  "define an event", segment recordings into intervals (e.g. "engine RPM between 2000 and 5000"),
  aggregate over the whole recording, capture state transitions/sequences, or mark instants like rising
  edges. Covers BasicEvent, ContainerEvent, SequenceOfEvents, and PointsInTimeEvent — which TSAL result
  type each requires, their constructor parameters, and the event fact/dimension output.
---

# Impulse — events

An event defines time windows within a recording that scope downstream aggregations (see
`impulse-aggregations`). Events are built from TSAL expressions (see `impulse-tsal`).

**Every event used by an aggregation must be registered with the report before computing:**

```python
report.add_event(my_event)
```

Choose the type by what you need:

| Type                | TSAL input required            | Instances per container      | Duration                     |
|---------------------|--------------------------------|------------------------------|------------------------------|
| `BasicEvent`        | one, must yield `Intervals`    | one per matching interval    | interval (`start < end`)     |
| `ContainerEvent`    | none                           | exactly one                  | full recording               |
| `SequenceOfEvents`  | ordered list, each `Intervals` | one per joined sequence      | interval (`start < end`)     |
| `PointsInTimeEvent` | one, must yield `PointsInTime` | one per instant              | zero (`start == end`)        |

The TSAL result type is validated at construction — passing the wrong type raises `ValueError`.

## BasicEvent

Each contiguous interval where a boolean TSAL expression is `True` becomes one event instance.

```python
from impulse_reporting.events.basic_event import BasicEvent

eng_rpm = report.get_db().query.channel(channel_name="Engine RPM")

rpm_band = BasicEvent(
    name="eng_rpm_band",
    expr=(eng_rpm > 2000) & (eng_rpm < 5000),
    desc="Engine RPM between 2000 and 5000",
    required_channels=["Engine RPM"],
)
report.add_event(rpm_band)
```

| Parameter           | Type                   | Required | Description                                                        |
|---------------------|------------------------|----------|--------------------------------------------------------------------|
| `name`              | `str`                  | Yes      | Unique event name; identifier in fact/dimension tables.            |
| `expr`              | `TimeSeriesExpression` | Yes      | Boolean condition. **Must evaluate to `Intervals`.**               |
| `desc`              | `str`                  | No       | Description stored in `event_dimension`.                           |
| `required_channels` | `list[str]`            | No       | Informational; stored in `event_dimension`.                        |
| `attributes`        | `Mapping[str, str]`    | No       | Free-form metadata; values coerced to strings.                     |

## ContainerEvent

Spans the full duration of each recording — start/end come from `container_metrics`
(`start_ts`/`stop_ts`), no expression needed. Use it for whole-recording aggregations.

```python
from impulse_reporting.events.container_event import ContainerEvent

container_event = ContainerEvent(name="container_event", desc="Full measurement recording")
report.add_event(container_event)
```

Parameters: `name` (required), `desc`, `attributes`.

## SequenceOfEvents

Joins an ordered list of `Intervals` expressions into single sequence intervals: when the next
expression's interval overlaps the current one, the sequence spans from the first interval's start to
the next interval's end. Use it for state transitions (e.g. stationary → moving).

```python
from impulse_reporting.events.sequence_of_events import SequenceOfEvents

veh_spd = report.get_db().query.channel(channel_name="Vehicle Speed Sensor")

idle_to_drive = SequenceOfEvents(
    name="idle_to_drive",
    expressions=[veh_spd == 0, veh_spd > 0],
    desc="Stationary followed by motion",
    required_channels=["Vehicle Speed Sensor"],
)
report.add_event(idle_to_drive)
```

| Parameter           | Type                          | Required | Description                                                                     |
|---------------------|-------------------------------|----------|---------------------------------------------------------------------------------|
| `name`              | `str`                         | Yes      | Unique event name.                                                              |
| `expressions`       | `list[TimeSeriesExpression]`  | Yes      | Ordered list; **each must evaluate to `Intervals`.**                            |
| `desc`              | `str`                         | No       | Description.                                                                    |
| `required_channels` | `list[str]`                   | No       | Informational.                                                                  |
| `max_overlap`       | `float`                       | No       | Skip sequences whose overlap exceeds this (same time unit as the timestamps).   |
| `attributes`        | `Mapping[str, str]`           | No       | Free-form metadata.                                                             |

## PointsInTimeEvent

Each instant of a `PointsInTime` expression (typically `rising_edges()` / `falling_edges()`) becomes
one **zero-duration** event instance (`start_ts == end_ts`). Pair it with `PointValueAggregator` to
sample channel values at those instants (see `impulse-aggregations`).

```python
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent

eng_rpm = report.get_db().query.channel(channel_name="Engine RPM")

rpm_rising = PointsInTimeEvent(
    name="rpm_rising_edges",
    expr=eng_rpm.rising_edges(),          # must evaluate to PointsInTime
    desc="Instants where engine RPM rises",
)
report.add_event(rpm_rising)
```

Parameters: `name` (required), `expr` (required, must yield `PointsInTime`), `desc`, `required_channels`,
`attributes`.

## Output schema

All event types share two gold tables.

**event_dimension** (one row per event) — key columns: `event_id`, `report_id`,
`event_type` (`"BASIC_EVENT"`, `"CONTAINER_EVENT"`, `"SEQUENCE_OF_EVENTS"`, `"POINTS_IN_TIME_EVENT"`),
`event_name`, `event_description`, `required_channels`, `event_expression` (TSAL string, `"NA"` for
`ContainerEvent`), `definition_hash`, `attributes`.

**event_instance_fact** (one row per instance per container) — `container_id`, `event_instance_id`,
`event_id`, `start_ts`, `end_ts`. Interval events satisfy `start_ts < end_ts`; `PointsInTimeEvent`
instances are zero-duration (`start_ts == end_ts`). Point instances share this table with interval
events — distinguish them by joining `event_dimension` on `event_id` and filtering
`event_type == "POINTS_IN_TIME_EVENT"`.
