---
sidebar_position: 2
title: TSAL
---

# TSAL -- Time Series Analytics Language

TSAL is the expression language at the core of Impulse. It provides a Pythonic, Matlab-style syntax for
selecting physical channels, defining virtual signals, and expressing event conditions. All TSAL expressions are **lazy
** -- no computation happens until a solver executes the query.

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

### Channel aliases

When the same physical signal may be stored under different tag combinations, use `with_alias()` to provide fallback
selectors:

```python
rpm = db.query.channel(channel_name='Engine RPM', brand='Seat').with_alias(
    db.query.channel(channel_name='EngineSpeed', brand='Seat')
)
```

The solver tries each alias in order and returns the first match.

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

Comparison operators produce `Intervals` -- a set of time windows where the condition holds true. This makes them the
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
| `.cumtrapz()`            | --                   | Cumulative trapezoidal integration over the signal.                                                                                        |
| `.trapz()`               | --                   | Total trapezoidal integration (returns a scalar).                                                                                          |

```python
distance_km = veh_spd.resample(1e6).cumtrapz() / 3600 / 1e6
```

### Filtering

| Method              | Signature                         | Description                                                                                    |
|---------------------|-----------------------------------|------------------------------------------------------------------------------------------------|
| `.where(condition)` | `condition: TimeSeriesExpression` | Restrict the signal to time intervals where the condition (an `Intervals` expression) is true. |

```python
rpm_in_band = eng_rpm.where((eng_rpm > 2000) & (eng_rpm < 5000))
```

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
`Histogram2D` classes from `mda_reporting.aggregations`.

### Signal manipulation

| Method                 | Signature             | Description                                                                                                                    |
|------------------------|-----------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `.sparse()`            | --                    | Merge consecutive samples with the same value into a single interval. Reduces data volume.                                     |
| `.synchronized(other)` | `other: SampleSeries` | Align two signals to shared overlapping time intervals. Called automatically when combining signals with arithmetic operators. |
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

---

## Expression types

Under the hood, TSAL expressions form a tree of typed nodes:

| Type                      | Role                                                                      |
|---------------------------|---------------------------------------------------------------------------|
| `TimeSeriesSelector`      | Leaf node: selects a physical channel by tag expression.                  |
| `TimeSeriesAliasSelector` | Selects from multiple channel candidates (alias/fallback).                |
| `TimeSeriesOp`            | Internal node: arithmetic, comparison, logical, or method-call operation. |
| `TimeSeriesUDF`           | User-defined function applied to one or more expressions.                 |

The expression tree is materialized when `QueryBuilder.solve()` is called. The solver resolves `TimeSeriesSelector`
nodes into `SampleSeries` objects, then the tree is evaluated bottom-up.

---

## Result types

Depending on the operations applied, a TSAL expression resolves to one of these types at execution time:

| Type             | Description                                                                                                                                         |
|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `SampleSeries`   | Time series with `(tstarts, tends, values)` arrays. Produced by channel selection, arithmetic, resampling, and integration.                         |
| `Intervals`      | Set of `(tstart, tend)` pairs. Produced by comparison and logical operators. Supports `&` (intersection), `\|` (union), `expand()`, and `shrink()`. |
| `PointsInTime`   | Set of individual timestamps. Produced by `.rising_edges()` and `.falling_edges()`.                                                                 |
| Scalar (`float`) | Single numeric value. Produced by `.min()`, `.max()`, `.mean()`, `.sum()`.                                                                          |
