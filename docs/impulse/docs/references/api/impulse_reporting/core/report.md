---
sidebar_label: report
title: impulse_reporting.core.report
---

## Report

```python
class Report()
```

Represents a report containing pages, events, and configurations for data processing and persistence.


#### \_\_init\_\_

```python
def __init__(name: str,
             spark: SparkSession,
             workspace_client: WorkspaceClient,
             config: dict[str, Any] | None = None,
             config_path: str | None = None)
```

Initialize the Report object.

**Arguments**:

- `name` (`str`): Name of the report.
- `spark` (`SparkSession`): Spark session to be used for data processing.
- `workspace_client` (`WorkspaceClient`): Authenticated Databricks workspace client used for telemetry attribution.
- `config` (`Optional[dict[str, Any]]`): Dictionary containing configuration parameters.
- `config_path` (`Optional[str]`): Path to the JSON configuration file.

**Raises**:

- `ValueError`: If neither config nor config_path is provided.
- `DatabricksError`: If the workspace is not reachable.

#### get\_id

```python
def get_id() -> int
```

Returns a unique identifier for the report.

**Returns**:

`int`: Unique positive 32-bit integer identifier for the report.

#### get\_db

```python
def get_db() -> MeasurementDB
```

Get the measurement database associated with this report.

**Returns**:

`MeasurementDB`: The measurement database instance.

#### get\_solver

```python
def get_solver() -> QuerySolver
```

Get the query solver associated with this report.

**Returns**:

`QuerySolver`: The query solver instance.

#### load\_config\_from\_file

```python
def load_config_from_file(config_path: str) -> ImpulseConfig
```

Load Impulse configuration from a JSON file.

**Arguments**:

- `config_path` (`str`): Path to the JSON configuration file.

**Returns**:

`UnitySinkConfig`: The loaded Unity sink configuration.

#### load\_config\_from\_dict

```python
def load_config_from_dict(config_info: dict[str, Any]) -> ImpulseConfig
```

Load Impulse configuration from a dictionary.

**Arguments**:

- `config_info` (`dict of str to Any`): Dictionary containing configuration parameters.

**Returns**:

`ImpulseConfig`: The loaded Impulse configuration.

#### create\_measurement\_db

```python
def create_measurement_db(config: ImpulseConfig,
                          ws: WorkspaceClient) -> MeasurementDB
```

Create a measurement database based on the provided configuration.

Maps the optional ``container_tags`` field from the Source config
to the ``container_tags_table`` parameter expected by
``MeasurementDBConfig``.

**Arguments**:

- `config` (`ImpulseConfig`): The Impulse configuration.
- `ws` (`WorkspaceClient`): Authenticated Databricks workspace client.

**Returns**:

`MeasurementDB`: The measurement database instance.

#### create\_query\_builder

```python
def create_query_builder(db: MeasurementDB,
                         config: ImpulseConfig) -> QueryBuilder
```

Create a query builder based on the provided configuration and set container filters.

Validates that tag filters are only used when a
``container_tags_table`` is configured in ``source``.  Both
KeyValueStoreSolver and DeltaSolver support tag and metric filters,
but tag filters require the narrow ``container_tags`` table to be
available.

**Arguments**:

- `db` (`MeasurementDB`): The measurement database instance.
- `config` (`ImpulseConfig`): The Impulse configuration.

**Raises**:

- `ValueError`: If tag filters are configured but ``source.container_tags_table``
is not set.

**Returns**:

`QueryBuilder`: The query builder instance with applied filters.

#### create\_sink

```python
def create_sink(config: ImpulseConfig) -> Sink
```

Create a sink based on the provided configuration.

**Arguments**:

- `config` (`ImpulseConfig`): The Impulse configuration.

**Returns**:

`Sink`: The sink instance for report persistence.

#### create\_solver

```python
def create_solver(spark: SparkSession, config: ImpulseConfig) -> QuerySolver
```

Create a query solver based on the provided configuration.

**Arguments**:

- `spark` (`SparkSession`): The Spark session to use for the solver.
- `config` (`ImpulseConfig`): The configuration

**Raises**:

- `ValueError`: If the solver type is unknown.

**Returns**:

`QuerySolver`: An instance of the appropriate query solver based on the configuration.

#### get\_sink\_config

```python
def get_sink_config() -> SinkConfig
```

Get the current sink configuration.

**Raises**:

- `ValueError`: If no sink is configured (sinkless mode).

**Returns**:

`SinkConfig`: The sink configuration associated with this report.

#### add\_page

```python
def add_page(page: Page)
```

Add a page to the report.

**Arguments**:

- `page` (`Page`): The page to add.

**Returns**:

`None`: 

#### add\_event

```python
def add_event(event: Event)
```

Add an event to the report.

**Arguments**:

- `event` (`Event`): The event to add.

**Raises**:

- `ValueError`: If the event is a ContainerEvent and a ContainerEvent already exists in the report.

**Returns**:

`None`: 

#### get\_events

```python
def get_events() -> list[Event]
```

Get the list of events associated with the report.

**Returns**:

`list of Event`: List of events.

#### get\_events\_dict

```python
def get_events_dict() -> dict
```

Get a dictionary of events part of the report keyed by event name.

**Returns**:

`dict`: Dictionary mapping event names to Event objects.

#### persist\_results

```python
def persist_results()
```

Persist report results using appropriate strategy based on definition changes.

Uses tracked state from determine_report() to decide persistence strategy:
- Changed definitions: replaceWhere (atomic delete + insert)
- Unchanged definitions: MERGE (upsert)

**Returns**:

`None`: 

#### determine\_report

```python
def determine_report(is_incremental: bool = None)
```

Determine and process events, aggregations, and container dimensions for the report.

Results are accessible in the report's attributes.

Supports incremental processing with definition-hash-based optimization:
- Changed definitions trigger full reprocessing (all containers)
- Unchanged definitions use incremental processing (only new/updated containers)

**Arguments**:

- `is_incremental` (`bool`): Hint for processing mode. Overwritten by config when incremental
config is provided.
- True: Request incremental processing (if gold layer exists)
- False: Force full processing
- None: Use config value (default: full processing)

**Returns**:

`None`: 

