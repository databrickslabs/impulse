---
sidebar_position: 1
title: Defining Expressions
---

# Defining Expressions

TSAL expressions are built in Python by selecting channels and combining them with operators and
signal methods. Every expression is a `TimeSeriesExpression` and stays **lazy** until the query is
[evaluated](evaluation.md).

## Channel selection

Physical channels are selected by their metadata tags through the `QueryBuilder.channel()` method. Every keyword
argument becomes a tag filter; all filters must match for a channel to be selected.

```python
db = my_report.get_db()

eng_rpm = db.query.channel(channel_name='Engine RPM', brand='Seat', model='Leon')
veh_spd = db.query.channel(channel_name='Vehicle Speed Sensor')
```

The returned object is a `TimeSeriesSelector`, which is a `TimeSeriesExpression`. It can be used directly in arithmetic,
comparisons, or signal methods.

:::note Where selection tags must live
Each tag passed to `channel(...)` must be resolvable by the solver — either as a **column on
`channel_metrics`** (the wide model) or, when a `channel_tags` table is provisioned, as a
**`(key, value)` row in `channel_tags`**. Which mode applies depends on whether a `channel_tags`
table is configured — see [Query Solvers](../query_solvers.md#how-defaultsolver-adapts).
:::

### Logical aliases via channel mapping

For workflows where a stable logical name should resolve to one of many physical channels through a separately
maintained mapping table, use `channel_with_alias()` instead. This requires
a `channel_mapping_table` to be configured in `source` (see [Configuration](../../../config/configuration)).

```python
rpm = db.query.channel_with_alias(channel_name='Engine RPM')
```

Each keyword argument becomes a tag filter on the `channel_mapping` table; the solver joins the matched logical entries
to the physical channels at read time. Use this when the consuming code should not need to know which physical signal
backs a given logical name.

:::note
The tag used for the lookup (e.g. `channel_name`) must also exist as a **column on `channel_metrics`**:
the solver resolves each logical name by joining `channel_mapping` to `channel_metrics` on the
channel-identifying columns (by default `channel_name` and `data_key`). See
[Query Solvers](../query_solvers.md) for details.
:::

When the `channel_mapping` table carries `source_unit` and `target_unit` columns and the report config sets
`source.unit_conversion_table`, values returned from `channel_with_alias()` are automatically converted from source
to target unit before any expression is evaluated. Constants and parameters in expressions over an aliased selector
must therefore be expressed in the target unit. Direct selectors via `channel(...)` on the same physical channel are
unaffected — conversion is a property of the alias, not of the channel.

---

## Operators

### Arithmetic operators

Arithmetic operators work between two expressions or between an expression and a scalar. They produce a new
`TimeSeriesExpression`.

| Operator | Example  | Description    |
|----------|----------|----------------|
| `+`      | `a + b`  | Addition       |
| `-`      | `a - b`  | Subtraction    |
| `*`      | `a * b`  | Multiplication |
| `/`      | `a / b`  | Division       |
| `%`      | `a % 10` | Modulo         |

```python
avg_temp = (amb_air_temp + intake_air_temp) / 2
```

When two `SampleSeries` are combined, the framework automatically synchronizes them to overlapping time intervals before
applying the operation.

### Comparison operators

Comparison operators produce `Intervals` — a set of time windows where the condition holds true. This makes them the
primary building block for event definitions.

| Operator | Example          | Description           |
|----------|------------------|-----------------------|
| `>`      | `signal > 5000`  | Greater than          |
| `>=`     | `signal >= 5000` | Greater than or equal |
| `<`      | `signal < 1000`  | Less than             |
| `<=`     | `signal <= 1000` | Less than or equal    |
| `==`     | `signal == 0`    | Equal                 |
| `!=`     | `signal != 0`    | Not equal             |

```python
high_rpm = eng_rpm > 5000  # Intervals where RPM exceeds 5000
```

### Logical operators

Logical operators combine `Intervals` (boolean results) into compound conditions.

| Operator | Example                    | Description        |
|----------|----------------------------|--------------------|
| `&`      | `(a > 2000) & (a < 5000)`  | Intersection (AND) |
| `\|`     | `(a < 1000) \| (a > 7000)` | Union (OR)         |

```python
rpm_band = (eng_rpm > 2000) & (eng_rpm < 5000)
```

Parentheses are required around each comparison because of Python operator precedence.

---

## Signal methods

Methods available on any `TimeSeriesExpression`. They are forwarded to the underlying `SampleSeries` (or other result
type) at execution time.

### Resampling and integration

| Method                   | Signature            | Description                                                                                                                                |
|--------------------------|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `.resample(sample_rate)` | `sample_rate: float` | Resample the signal to a uniform sample rate. The rate is specified in the same time unit as the underlying data (typically microseconds). |
| `.cumtrapz()`            | —                    | Cumulative trapezoidal integration over the signal.                                                                                        |
| `.trapz()`               | —                    | Total trapezoidal integration (returns a scalar).                                                                                          |

```python
distance_km = veh_spd.resample(1e6).cumtrapz() / 3600 / 1e6
```

### Filtering

| Method              | Signature                         | Description                                                                                                                                                                                                                                                              |
|---------------------|-----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `.where(condition)` | `condition: TimeSeriesExpression` | Filter the signal by another expression. An `Intervals` condition restricts the signal to those windows and returns a `SampleSeries`. A `PointsInTime` condition (e.g. `rising_edges()`) samples the signal's value at those instants and returns a `PointsInTimeSeries`. |

```python
# Intervals condition -> SampleSeries restricted to the windows
rpm_in_band = eng_rpm.where((eng_rpm > 2000) & (eng_rpm < 5000))

# PointsInTime condition -> PointsInTimeSeries sampled at those instants
# (instants outside every sample interval are dropped)
rpm_at_starts = eng_rpm.where(veh_spd.rising_edges())
```

See [Core Data Model](core_data_model.md) for the difference between `SampleSeries` and
`PointsInTimeSeries`.

### Aggregation (scalar results)

These methods reduce a signal to a single scalar value.

| Method    | Description                           |
|-----------|---------------------------------------|
| `.sum()`  | Duration-weighted sum of all values.  |
| `.min()`  | Minimum value in the series.          |
| `.max()`  | Maximum value in the series.          |
| `.mean()` | Duration-weighted mean of all values. |

### Edge detection

| Method                               | Description                                                                                   | Returns        |
|--------------------------------------|-----------------------------------------------------------------------------------------------|----------------|
| `.rising_edges()`                    | Points in time where the value increases from the previous sample.                            | `PointsInTime` |
| `.falling_edges()`                   | Points in time where the value decreases from the previous sample.                            | `PointsInTime` |
| `.intervals_between_falling_edges()` | Intervals delimited by consecutive falling edges. Useful for distance or cycle-based binning. | `Intervals`    |

```python
distance_bins = (distance_km % 10).intervals_between_falling_edges()
```

### Histogram methods

| Method                                 | Signature                                                                    | Description                                                                                            |
|----------------------------------------|------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| `.histogram(bins)`                     | `bins: list[float]`                                                          | Compute a 1D histogram with the given bin edges. Returns histogram counts weighted by sample duration. |
| `.histogram2d(y_expr, x_bins, y_bins)` | `y_expr: TimeSeriesExpression`, `x_bins: list[float]`, `y_bins: list[float]` | Compute a 2D histogram against another signal.                                                         |

These are lower-level methods on the expression itself. For report-level aggregations, use the `Histogram` and
`Histogram2D` classes from `impulse_reporting.aggregations`.

### Signal manipulation

| Method                 | Signature             | Description                                                                                                                    |
|------------------------|-----------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `.sparse()`            | —                     | Merge consecutive samples with the same value into a single interval. Reduces data volume.                                     |
| `.synchronized(other)` | `other: SampleSeries` | Align two signals to shared overlapping time intervals. Called automatically when combining signals with arithmetic operators. (`PointsInTimeSeries` offers `synchronized` / `synchronized_all` too — see [Core Data Model](core_data_model.md).) |
| `.alias(name)`         | `name: str`           | Assign a display name to the expression. Used as the column name in result DataFrames.                                         |

### Rolling window operations

| Method                          | Signature            | Description                                                                     |
|---------------------------------|----------------------|---------------------------------------------------------------------------------|
| `.rolling_average(window_size)` | `window_size: float` | Compute a rolling average over a sliding window.                                |
| `.rolling_stats(window_size)`   | `window_size: float` | Compute rolling min, max, and average. Returns a tuple of three `SampleSeries`. |

### User-defined functions

| Method                           | Signature        | Description                                             |
|----------------------------------|------------------|---------------------------------------------------------|
| `.apply(func)`                   | `func: callable` | Apply a custom function to the resolved `SampleSeries`. |
| `TimeSeriesExpression.udf(func)` | `func: callable` | Wrap a function as a reusable TSAL expression.          |

```python
@TimeSeriesExpression.udf
def custom_transform(series):
    return SampleSeries(series.tstarts, series.tends, series.values ** 2)

squared_rpm = custom_transform(eng_rpm)
```

---

## Virtual signals

Virtual signals are TSAL expressions that derive new channels from physical ones. They are not stored in the Silver
layer but computed on-the-fly by the solver.

:::note Persisting a virtual signal
A virtual signal is ephemeral — it exists only for the query that defines it. To *materialize* a derived
signal into a queryable gold table (with incremental updates), wrap the same expression in a
[`CalculatedChannel`](../../report/channel.md) and register it on a report.
:::

### Derived signals

Combine physical channels with arithmetic:

```python
avg_temp = (amb_air_temp + intake_air_temp) / 2
power = voltage * current
delta_temp = intake_air_temp - amb_air_temp
```

### Integration-based signals

Compute cumulative quantities from rate signals:

```python
distance_km = veh_spd.resample(1e6).cumtrapz() / 3600 / 1e6
```

### Modulo-based binning

Create distance or cycle-based bins using modulo and edge detection:

```python
every_10km = (distance_km % 10).intervals_between_falling_edges()
```

This produces `Intervals` where each interval spans exactly 10 km of travel. These can be used as events for
aggregation.
