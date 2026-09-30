---
sidebar_label: time_window_event
title: impulse_reporting.events.time_window_event
---

TimeWindowEvent — splits each container into consecutive fixed-duration windows.


## TimeWindowEvent

```python
class TimeWindowEvent(Event)
```

Event that divides each measurement container into consecutive fixed windows.

Unlike ``ContainerEvent`` (one instance per container), a ``TimeWindowEvent`` emits one
event instance per fixed-duration slice, tiling the container's ``start_ts`` / ``stop_ts``
span with windows of length ``window_length``.  The final slice is clamped to the
container end.  Boundaries come from a :class:`TimeWindowExpression`, so the event
fact and any aggregation scoped to this event share the same solved windows and their
``event_instance_id`` values match by construction.


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
(e.g. milliseconds-since-epoch). Must be strictly positive.
- `desc` (`str`): Description of the event.
- `required_channels` (`list of str`): List of required channels for the event. Informational; stored in the event
dimension table.
- `attributes` (`Mapping[str, str]`): Key-value metadata for the event. ``window_length`` is surfaced here
automatically (without overriding a user-supplied key).

**Raises**:

- `ValueError`: If ``window_length`` is not strictly positive.

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
def determine_events(cls,
                     spark: SparkSession,
                     events: list[TimeWindowEvent],
                     *,
                     solved_df: DataFrame = None,
                     query: QueryBuilder = None,
                     solver: QuerySolver = None,
                     pre_filtered_containers_df=None)
```

Extract the event fact table for the given list of TimeWindowEvent objects.

Each window becomes one event instance (``start_ts < end_ts``). The window intervals
are read from the centralized solve (the same column consumed by scoped aggregations),
so the resulting ``event_instance_id`` values match on both sides.

**Arguments**:

- `spark` (`SparkSession`): Spark session for data processing.
- `events` (`list of TimeWindowEvent`): List of TimeWindowEvent objects to process.
- `solved_df` (`DataFrame`): Pre-solved wide DataFrame from centralized batch solve. Required.
- `query` (`QueryBuilder`): Query builder (unused, kept for interface compatibility).
- `solver` (`QuerySolver`): Query solver (unused, kept for interface compatibility).
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

