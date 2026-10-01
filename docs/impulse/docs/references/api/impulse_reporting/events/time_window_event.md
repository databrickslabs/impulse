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

The event fact is computed natively in Spark from ``container_metrics`` (via


#### \_\_init\_\_

```python
def __init__(name: str,
             window_length: float,
             desc: str = None,
             required_channels: list[str] = None,
             attributes: Mapping[str, str] = None)
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

**Raises**:

- `ValueError`: If ``window_length`` is not strictly positive and finite.

#### get\_id

```python
def get_id() -> int
```

Returns a unique identifier for the event.

**Returns**:

`int`: Unique positive 32-bit integer identifier for the event.

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

Only includes the expression string (which encodes ``window_length``), the sole
attribute that affects the event results, so resizing the window forces a full
recompute in incremental mode.

Excludes: name, description, required_channels, report_id

**Returns**:

`int`: Hash value representing the computation definition.

#### as\_dict

```python
def as_dict() -> dict
```

Get a dictionary representation of the event.

**Returns**:

`dict`: Dictionary containing event metadata.

#### as\_spark\_row

```python
def as_spark_row() -> Row
```

Get a Spark Row representation of the event.

**Returns**:

`Row`: Spark Row containing event metadata.

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
containers' ``start_ts`` / ``stop_ts``, so every filtered container gets windows.
Each window becomes one event instance (``start_ts < end_ts``). The windows are
bit-identical to the ones the solve computes for scoped aggregations (see
:func:`window_intervals_col`), so the ``event_instance_id`` values match.

**Arguments**:

- `spark` (`SparkSession`): Spark session for data processing.
- `events` (`list of TimeWindowEvent`): List of TimeWindowEvent objects to process.
- `solved_df` (`DataFrame`): Not used by TimeWindowEvent (kept for interface compatibility).
- `query` (`QueryBuilder`): Query builder with filters applied.
- `solver` (`QuerySolver`): Solver whose filter pipeline is used for container resolution.
- `pre_filtered_containers_df` (`DataFrame`): Pre-filtered containers for incremental processing.

**Returns**:

`DataFrame`: Spark DataFrame containing event instance facts.

#### determine\_metadata\_df

```python
def determine_metadata_df(cls, spark: SparkSession,
                          events: list[TimeWindowEvent])
```

Create a Spark DataFrame containing event metadata.

**Arguments**:

- `spark` (`SparkSession`): Spark session for data processing.
- `events` (`list of TimeWindowEvent`): List of TimeWindowEvent objects.

**Returns**:

`DataFrame`: Spark DataFrame containing event metadata.

