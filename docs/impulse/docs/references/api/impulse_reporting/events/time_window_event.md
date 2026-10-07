---
sidebar_label: time_window_event
title: impulse_reporting.events.time_window_event
---

TimeWindowEvent — splits each container into consecutive fixed-duration windows.


## TimeWindowEvent

```python
class TimeWindowEvent(ContainerBoundaryEvent)
```

Event that divides each measurement container into consecutive fixed windows.

Unlike ``ContainerEvent`` (one instance per container), a ``TimeWindowEvent`` emits one
event instance per fixed-duration slice, tiling the container's ``start_ts`` / ``stop_ts``
span with windows of length ``window_length``.  The final slice is clamped to the
container end.

The event fact is computed from ``container_metrics`` alone (via


#### \_\_init\_\_

```python
def __init__(name: str,
             window_length: float,
             desc: str = None,
             required_channels: list[str] = None,
             attributes: Mapping[str, str] = None,
             max_windows_per_container: int = MAX_WINDOWS_PER_CONTAINER)
```

Initialize a TimeWindowEvent object.

**Arguments**:

- `name` (`str`): Name of the event.
- `window_length` (`float`): Fixed window length, in the same time unit as the underlying timestamps
(e.g. milliseconds-since-epoch). Must be strictly positive and finite.
- `desc` (`str`): Description of the event.
- `required_channels` (`list of str`): List of required channels for the event. Informational; stored in the event
dimension table.
- `attributes` (`Mapping[str, str]`): Key-value metadata for the event. ``window_length`` is surfaced here
automatically (without overriding a user-supplied key).
- `max_windows_per_container` (`int`): Maximum number of windows per container (default 1,000,000). A container
exceeding it fails the report with an error naming the limit, which usually
means ``window_length`` is in the wrong unit for the boundaries. Not part of
the definition hash.

**Raises**:

- `ValueError`: If ``window_length`` is not strictly positive and finite, or
``max_windows_per_container`` is not a positive integer.

#### set\_channel\_time

```python
def set_channel_time(unit: str | None,
                     origin: str = "epoch",
                     container_unit: str | None = None) -> None
```

Record the channel time frame the windows are computed in.

Set by ``Report.add_event`` from the report's ``solver_config``.  Stored on the
expression, whose string form feeds the definition hashes of this event and of the
aggregations scoped to it.

**Arguments**:

- `unit` (`str or None`): The report's ``solver_config.channel_time_unit``.
- `origin` (`str`): The report's ``solver_config.channel_time_origin`` (default ``"epoch"``).
- `container_unit` (`str or None`): The report's ``solver_config.container_time_unit``.

#### get\_expression

```python
def get_expression() -> TimeSeriesExpression | None
```

Get the time series expression associated with the event.

**Returns**:

`TimeSeriesExpression or None`: The time-window expression for the event.

#### get\_event\_type\_str

```python
def get_event_type_str() -> str
```

Get the event type string for TimeWindowEvent.

**Returns**:

`str`: Event type string.

#### determine\_definition\_hash

```python
def determine_definition_hash() -> int
```

Calculate definition hash for the time-window event.

Only includes the expression string, which encodes the attributes that affect the
event results: ``window_length`` and the channel time frame (``channel_time_unit``,
``channel_time_origin``, ``container_time_unit``; omitted while unset / default).
Resizing the window or changing the time frame therefore forces a full recompute in
incremental mode.

Excludes: name, description, required_channels, max_windows_per_container,
report_id

**Returns**:

`int`: Hash value representing the computation definition.

#### as\_dict

```python
def as_dict() -> dict
```

Get a dictionary representation of the event.

**Returns**:

`dict`: Dictionary containing event metadata.

#### determine\_events

```python
def determine_events(
        cls,
        spark: SparkSession,
        events: list[TimeWindowEvent],
        *,
        solved_df: DataFrame = None,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df: DataFrame = None) -> DataFrame
```

Extract the event fact table for the given list of TimeWindowEvent objects.

Resolves the matching containers via the solver's filter pipeline (like
``ContainerEvent``) and computes each event's windows natively from the
containers' ``start_ts`` / ``stop_ts`` in the channel time frame
(``solvers.utils.window_bounds.with_window_bounds``), so every filtered container
gets windows.
Each window becomes one event instance (``start_ts < end_ts``) whose
``event_instance_id`` hashes its boundaries. The solve uses the same window function
for scoped aggregations (see :func:`window_intervals_udf`), so the ids match.

**Arguments**:

- `spark` (`SparkSession`): Spark session for data processing.
- `events` (`list of TimeWindowEvent`): List of TimeWindowEvent objects to process.
- `solved_df` (`DataFrame`): Not used by TimeWindowEvent (kept for interface compatibility).
- `query` (`QueryBuilder`): Query builder with filters applied.
- `solver` (`QuerySolver`): Solver whose filter pipeline is used for container resolution.
- `pre_filtered_containers_df` (`DataFrame`): Pre-filtered containers for incremental processing.

**Returns**:

`DataFrame`: Spark DataFrame containing event instance facts.

