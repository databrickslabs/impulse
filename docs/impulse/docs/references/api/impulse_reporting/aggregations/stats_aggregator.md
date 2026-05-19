---
sidebar_label: stats_aggregator
title: impulse_reporting.aggregations.stats_aggregator
---

StatsAggregator reporting class for computing statistics within event intervals.


## StatsAggregator

```python
class StatsAggregator(Aggregation)
```

Class representing a statistics aggregation in a report.

This aggregation computes various statistics (min, max, mean, median, etc.)
on time series data within defined event intervals.


#### \_\_init\_\_

```python
def __init__(name: str,
             input_expressions: list[TimeSeriesExpression],
             channel_names: list[str],
             statistics: list[str],
             event: Event | None = None,
             desc: str = None,
             agg_type: str = "stats_aggregator",
             values_unit: str = None)
```

Initialize a StatsAggregator object.

**Arguments**:

- `name` (`str`): Name of the statistics aggregation.
- `input_expressions` (`list of TimeSeriesExpression`): List of time series expressions to compute statistics on.
- `channel_names` (`list of str`): Names of the signals associated with input expressions. Must be the same length as input_expressions.
- `statistics` (`list of str`): List of statistic types to compute (e.g., ['min', 'max', 'mean', 'median']).
- `event` (`Event`): Event defining intervals for statistics computation. If None, statistics
are computed over the entire time series.
- `desc` (`str`): Description of the aggregation.
- `agg_type` (`str`): Type of aggregation, defaults to "stats_aggregator".
- `values_unit` (`str`): Unit of the statistic values.

#### get\_id

```python
def get_id() -> int
```

Get a unique identifier for the statistics aggregation.

**Returns**:

`int`: Unique identifier for the statistics aggregation.

#### get\_event

```python
def get_event() -> Event
```

Get the event associated with the aggregation.

**Returns**:

`Event`: The event associated with the aggregation, or None if not set.

#### get\_expression

```python
def get_expression() -> TimeSeriesExpression
```

Get the time series expression for the statistics aggregation.

**Returns**:

`TimeSeriesExpression`: The time series expression for the statistics aggregation.

#### get\_expression\_str

```python
def get_expression_str() -> str
```

Get a string representation of the time series expression.

**Returns**:

`str`: String representation of the time series expression.

#### as\_dict

```python
def as_dict() -> dict
```

Get a dictionary representation of the statistics aggregation.

**Returns**:

`dict`: Dictionary containing aggregation metadata.

#### as\_spark\_row

```python
def as_spark_row() -> Row
```

Get a Spark Row representation of the statistics aggregation.

**Returns**:

`Row`: Spark Row containing aggregation metadata.

#### determine\_aggregations

```python
def determine_aggregations(cls,
                           spark: SparkSession,
                           aggregations: list[StatsAggregator],
                           *,
                           solved_df: DataFrame = None,
                           query: QueryBuilder = None,
                           solver: QuerySolver = None,
                           pre_filtered_containers_df: DataFrame = None)
```

Determine and process aggregations for a list of StatsAggregator visuals.

**Arguments**:

- `spark` (`pyspark.sql.SparkSession`): Spark session to use for computation.
- `aggregations` (`list of StatsAggregator`): List of StatsAggregator visual aggregations.
- `solved_df` (`DataFrame`): Pre-solved wide DataFrame from centralized batch solve. Required.
- `query` (`QueryBuilder`): Query builder (unused, kept for interface compatibility).
- `solver` (`QuerySolver`): Solver (unused, kept for interface compatibility).
- `pre_filtered_containers_df` (`DataFrame`): Pre-filtered containers (unused, kept for interface compatibility).

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing the processed stats aggregations.

#### determine\_metadata\_df

```python
def determine_metadata_df(
        cls, spark: SparkSession,
        stats_aggregators: list[StatsAggregator]) -> DataFrame
```

Create a metadata DataFrame for the provided StatsAggregator aggregations.

**Arguments**:

- `spark` (`pyspark.sql.SparkSession`): Spark session to use for DataFrame creation.
- `stats_aggregators` (`list of StatsAggregator`): List of StatsAggregator aggregations.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing metadata for the stats aggregations.

#### determine\_definition\_hash

```python
def determine_definition_hash() -> int
```

Calculate definition hash for stats aggregator.

Only includes computation-affecting attributes:
- input_expressions
- statistics to be calculated
- event expression if there is any

Excludes: name, desc, signal_name, units, page_number, report_id

**Returns**:

`int`: Hash value representing the computation definition.

