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
``container_tags_table`` is configured in ``source``.  DefaultSolver
supports tag and metric filters, but tag filters require the narrow
``container_tags`` table to be available.

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

- `ValueError`: If the event is a ContainerEvent and a ContainerEvent already exists in the report,
or if an event with the same name, ignoring case, has already been added (event
names identify events in the fact and dimension tables and name the event's
column in the solved data, which Spark resolves case-insensitively).

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

#### add\_calculated\_channel

```python
def add_calculated_channel(channel)
```

Add a calculated channel to the report.

**Arguments**:

- `channel` (`CalculatedChannel`): The calculated channel to add.

**Returns**:

`None`: 

#### get\_calculated\_channels

```python
def get_calculated_channels() -> list
```

Get the list of calculated channels associated with the report.

**Returns**:

`list of CalculatedChannel`: List of calculated channels.

#### persist\_results

```python
def persist_results(cleanup_temp_tables: bool | None = None)
```

Deprecated: use :meth:`run`, which determines and persists in one call.

Persists the results of a prior :meth:`determine_report` call. Unlike :meth:`run`,
it does not drain capped incremental batches.

**Arguments**:

- `cleanup_temp_tables` (`bool | None`): Same as the :meth:`run` argument.

#### run

```python
def run(is_incremental: bool = None,
        persist_results: bool = True,
        cleanup_temp_tables: bool | None = None)
```

Determine and persist a report, draining all container batches.

The entry point for executing a report. Computes events, aggregations, calculated
channels, and container dimensions, then writes them to the gold layer. Without a
configured sink nothing is written.

``run(persist_results=False)`` is the compute-only path: results are left on the
report's attributes (e.g. ``aggregation_dfs``, ``event_dfs``) for inspection. These
are lazy DataFrames, so after a persisting run read the gold tables instead: an
incremental run's attributes re-detect containers against the updated gold, a capped
run's hold only the last batch, and ``cleanup_temp_tables`` drops the temp tables
they read from.

Supports incremental processing with definition-hash-based optimization:
- Changed definitions trigger full reprocessing (all containers)
- Unchanged definitions use incremental processing (only new/updated containers)

With ``max_containers_per_batch`` set on an incremental run, ``run()`` loops: each
iteration commits at most that many upserted containers, and the loop continues
until a run observes at most that many remaining (the last batch clears the rest).
After the first iteration all definition hashes are current, so later iterations
recompute only unchanged entities over the next batch of new containers. Without a
cap (or in full mode) it is a single determine+persist pass.

**Arguments**:

- `is_incremental` (`bool`): Hint for processing mode. Overwritten by config when incremental
config is provided.
- True: Request incremental processing (if gold layer exists)
- False: Force full processing
- None: Use config value (default: full processing)
- `persist_results` (`bool`): When True (default), persist after determining and drain the batches. When
False, determine once without persisting (no iteration — nothing commits, so
the batch cannot advance).
- `cleanup_temp_tables` (`bool | None`): Whether to drop the batch-solving ``__impulse_temp_*`` tables from the
sink schema after persistence completes successfully.
- True/False: use this value, overriding the config flag.
- None (default): fall back to ``config.unity_sink.cleanup_temp_tables``
  (which itself defaults to False).

#### determine\_report

```python
def determine_report(is_incremental: bool = None)
```

Deprecated: use :meth:`run`, or ``run(persist_results=False)`` to compute only.

Computes the report without persisting; results are accessible in the report's
attributes.

**Arguments**:

- `is_incremental` (`bool`): Same as the :meth:`run` argument.

