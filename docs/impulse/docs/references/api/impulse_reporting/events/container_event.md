---
sidebar_label: container_event
title: impulse_reporting.events.container_event
---

ContainerEvent — an event spanning the full measurement container.


## ContainerEvent

```python
class ContainerEvent(ContainerBoundaryEvent)
```

Event that treats the full measurement container as a single event instance.

Unlike ``BasicEvent``, no time-series expression is needed — the event
boundaries are derived directly from the container's ``start_ts`` and
``stop_ts`` metadata.


#### \_\_init\_\_

```python
def __init__(name: str, desc: str = None, attributes: dict[str, str] = None)
```

Initialise a ContainerEvent.

**Arguments**:

- `name` (`str`): Name of the event.
- `desc` (`str`): Human-readable description.
- `attributes` (`dict`): Key-value metadata for the event.

#### get\_expression

```python
def get_expression() -> TimeSeriesExpression | None
```

ContainerEvent has no time-series expression.

**Returns**:

`None`: 

#### get\_event\_type\_str

```python
def get_event_type_str() -> str
```

Get the event type string for ContainerEvent.

**Returns**:

`str`: Event type string.

#### determine\_definition\_hash

```python
def determine_definition_hash() -> int
```

Calculate definition hash.

The hash only captures computation-relevant attributes.
For a ``ContainerEvent`` the identity is fully determined by the
fact that it is a container event (there is no expression to vary),
so the name of the event is hashed.

**Returns**:

`int`: Hash value representing the computation definition.

#### as\_dict

```python
def as_dict() -> dict
```

Return a dictionary representation of the event.

**Returns**:

`dict`: 

#### determine\_events

```python
def determine_events(
        cls,
        spark: SparkSession,
        events: list[ContainerEvent],
        *,
        solved_df: DataFrame = None,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df: DataFrame = None) -> DataFrame
```

Determine event instances from container metadata.

Resolves matching containers via the solver's filter pipeline and
produces one event instance per container.

**Arguments**:

- `spark` (`SparkSession`): Active Spark session.
- `events` (`list of ContainerEvent`): List of ContainerEvent objects (only the first is used for naming).
- `solved_df` (`DataFrame`): Not used by ContainerEvent (kept for interface compatibility).
- `query` (`QueryBuilder`): Query builder with filters applied.
- `solver` (`QuerySolver`): Solver whose filter pipeline is used for container resolution.
- `pre_filtered_containers_df` (`DataFrame`): Pre-filtered containers for incremental processing.

**Returns**:

`DataFrame`: Spark DataFrame matching ``EVENT_INSTANCE_FACT_SCHEMA``.

