---
sidebar_label: basic_event
title: impulse_reporting.events.basic_event
---

## BasicEvent

```python
class BasicEvent(Event)
```

Class representing a basic event in a report.


#### \_\_init\_\_

```python
def __init__(name: str,
             expr: TimeSeriesExpression,
             desc: str = None,
             required_channels: list[str] = None,
             attributes: Mapping[str, str] = None)
```

Initialize a BasicEvent object.

**Arguments**:

- `name` (`str`): Name of the event.
- `expr` (`TimeSeriesExpression`): Time series expression for the event.
- `desc` (`str`): Description of the event.
- `required_channels` (`list of str`): List of required channels for the event.
- `attributes` (`Mapping[str, str]`): Key-value metadata for the event (e.g. limit_type, limit_direction).

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

`TimeSeriesExpression or None`: The time series expression for the event.

#### get\_event\_type\_str

```python
def get_event_type_str() -> str
```

Get the event type string for BasicEvent.

**Returns**:

`str`: Event type string.

#### determine\_definition\_hash

```python
def determine_definition_hash() -> int
```

Calculate definition hash for basic event.

Only includes the expression (computation logic), which is the
only attribute that affects the event results.

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
                     events: list[BasicEvent],
                     *,
                     solved_df: "DataFrame" = None,
                     query: QueryBuilder = None,
                     solver: QuerySolver = None,
                     pre_filtered_containers_df=None)
```

Extract event fact table for the given list of BasicEvent objects.

**Arguments**:

- `spark` (`SparkSession`): Spark session for data processing.
- `events` (`list of BasicEvent`): List of BasicEvent objects to process.
- `solved_df` (`DataFrame`): Pre-solved wide DataFrame from centralized batch solve. Required.
- `query` (`QueryBuilder`): Query builder (unused, kept for interface compatibility).
- `solver` (`QuerySolver`): Query solver (unused, kept for interface compatibility).
- `pre_filtered_containers_df` (`DataFrame`): Pre-filtered containers for incremental processing.

**Returns**:

`DataFrame`: Spark DataFrame containing event instance facts.

#### determine\_metadata\_df

```python
def determine_metadata_df(cls, spark: SparkSession, events: list[BasicEvent])
```

Create a Spark DataFrame containing event metadata.

**Arguments**:

- `spark` (`SparkSession`): Spark session for data processing.
- `events` (`list of BasicEvent`): List of BasicEvent objects.

**Returns**:

`DataFrame`: Spark DataFrame containing event metadata.

