---
name: impulse-tsal
description: >
  Write TSAL (Time Series Analytics Language) expressions in Impulse — select measurement channels by
  metadata tags, derive virtual signals with arithmetic, express conditions as time windows, detect
  edges, resample, integrate, and build histograms. Use when the user wants to "select a channel",
  "build a virtual/derived signal", "detect when signal > X", "compute distance from speed", "find
  rising edges", or asks what a TSAL expression evaluates to. Covers `QueryBuilder.channel()`,
  operators, all signal methods, and the four result types (SampleSeries, Intervals, PointsInTime,
  PointsInTimeSeries).
---

# Impulse — TSAL expressions

TSAL is the Python DSL every Impulse mode is built on. You select physical channels and combine them
with operators and methods; the result is a `TimeSeriesExpression` that stays **lazy** until a query
is solved (`impulse-analyze`) or a report is computed (`impulse-reporting`).

You reach the builder through a `MeasurementDB`, which you get from a `Report` (`report.get_db()`)
or construct directly (see `impulse-analyze`):

```python
db = report.get_db()
query = db.query    # a QueryBuilder
```

## Selecting channels

Every keyword argument to `channel()` is a tag filter; all must match. Which tags are valid depends on
your silver layer — either columns on `channel_metrics` (wide model) or `(key, value)` rows in a
`channel_tags` table (see `impulse-data-model`).

```python
eng_rpm = db.query.channel(channel_name="Engine RPM", brand="Seat", model="Leon")
veh_spd = db.query.channel(channel_name="Vehicle Speed Sensor")
```

`channel()` returns a `TimeSeriesSelector`, which is a `TimeSeriesExpression` — use it directly in
arithmetic, comparisons, and methods.

**Logical aliases.** When a stable logical name should resolve to one of many physical channels via a
separately maintained mapping table, use `channel_with_alias()` instead. It requires a
`channel_mapping_table` in `source` (see `impulse-config`):

```python
rpm = db.query.channel_with_alias(channel_name="Engine RPM")
```

## Operators

**Arithmetic** (`+ - * / %`) — combine two signals or a signal and a scalar; produces a `SampleSeries`.
Two signals are automatically synchronized to overlapping validity intervals first.

```python
avg_temp   = (amb_air_temp + intake_air_temp) / 2
power      = voltage * current
```

**Comparison** (`> >= < <= == !=`) — produces `Intervals`, the time windows where the condition holds.
This is the primary building block for events.

```python
high_rpm = eng_rpm > 5000            # Intervals where RPM exceeds 5000
```

**Logical** (`&` intersection / `|` union) — combine `Intervals`. Parenthesize each comparison because
of Python precedence.

```python
rpm_band = (eng_rpm > 2000) & (eng_rpm < 5000)
cold_or_hot = (temp < 0) | (temp > 90)
```

## Signal methods

All methods are available on any `TimeSeriesExpression`.

**Resampling and integration**

| Method                   | Description                                                                              |
|--------------------------|------------------------------------------------------------------------------------------|
| `.resample(sample_rate)` | Resample to a uniform rate. Rate is in the data's time unit (typically microseconds).    |
| `.cumtrapz()`            | Cumulative trapezoidal integration (returns a signal).                                   |
| `.trapz()`               | Total trapezoidal integration (returns a scalar).                                        |

```python
distance_km = veh_spd.resample(1e6).cumtrapz() / 3600 / 1e6   # speed (km/h) -> cumulative km
```

**Filtering**

| Method              | Description                                                                                          |
|---------------------|------------------------------------------------------------------------------------------------------|
| `.where(condition)` | An `Intervals` condition restricts the signal to those windows → `SampleSeries`. A `PointsInTime` condition samples the value at those instants → `PointsInTimeSeries`. |

```python
rpm_in_band  = eng_rpm.where((eng_rpm > 2000) & (eng_rpm < 5000))   # SampleSeries
rpm_at_starts = eng_rpm.where(veh_spd.rising_edges())               # PointsInTimeSeries
```

**Reducers (scalar results)** — `.sum()`, `.min()`, `.max()`, `.mean()`. On a `SampleSeries` these are
**duration-weighted** (a value that is current for 2 s counts twice as much as one current for 1 s).

**Edge detection**

| Method                               | Returns        | Description                                          |
|--------------------------------------|----------------|------------------------------------------------------|
| `.rising_edges()`                    | `PointsInTime` | Instants where the value increased.                  |
| `.falling_edges()`                   | `PointsInTime` | Instants where the value decreased.                  |
| `.intervals_between_falling_edges()` | `Intervals`    | Windows delimited by consecutive falling edges.      |

**Histograms** (lower-level; for reports prefer the classes in `impulse-aggregations`)

| Method                                 | Description                                    |
|----------------------------------------|------------------------------------------------|
| `.histogram(bins)`                     | 1D duration-weighted histogram, `bins: list[float]`. |
| `.histogram2d(y_expr, x_bins, y_bins)` | 2D histogram against another signal.           |

**Signal manipulation**

| Method                 | Description                                                                            |
|------------------------|----------------------------------------------------------------------------------------|
| `.sparse()`            | Merge consecutive equal-valued samples into one interval (reduces volume).             |
| `.synchronized(other)` | Align two signals to shared validity intervals (automatic in arithmetic).              |
| `.alias(name)`         | Name the expression; used as the result-DataFrame column name.                         |

**Rolling windows** — `.rolling_average(window_size)`; `.rolling_stats(window_size)` returns a tuple of
three `SampleSeries` (min, max, average).

**User-defined functions**

```python
from impulse_query_engine.model.series import SampleSeries
from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesExpression

@TimeSeriesExpression.udf
def squared(series):
    return SampleSeries(series.tstarts, series.tends, series.values ** 2)

squared_rpm = squared(eng_rpm)          # a reusable TSAL expression
# or apply inline: eng_rpm.apply(some_callable)
```

## What an expression evaluates to (the core data model)

Every expression resolves — per container — to one of four in-memory types. Knowing the type tells you
what you can do next and which event/aggregation accepts it.

| Class                | Carries values? | Has duration? | Produced by                                        |
|----------------------|-----------------|---------------|----------------------------------------------------|
| `SampleSeries`       | yes             | yes           | channel selection, arithmetic, resampling          |
| `Intervals`          | no              | yes           | comparison / logical operators, `intervals_between_falling_edges()` |
| `PointsInTime`       | no              | no            | `rising_edges()` / `falling_edges()`               |
| `PointsInTimeSeries` | yes             | no            | `.where(PointsInTime)` — sampling a signal at instants |

Transitions between them:

| From                 | Operation                                 | Produces             |
|----------------------|-------------------------------------------|----------------------|
| `SampleSeries`       | `+ - * /` with scalar or another series   | `SampleSeries`       |
| `SampleSeries`       | `> >= < <= == !=`                         | `Intervals`          |
| `SampleSeries`       | `rising_edges()` / `falling_edges()`      | `PointsInTime`       |
| `SampleSeries`       | `where(Intervals)`                        | `SampleSeries`       |
| `SampleSeries`       | `where(PointsInTime)`                     | `PointsInTimeSeries` |
| `SampleSeries`       | `sum()` / `min()` / `max()` / `mean()`    | scalar               |
| `Intervals`          | `&` / `\|`                                | `Intervals`          |
| `Intervals`          | `& PointsInTime`                          | `PointsInTime`       |
| `Intervals`          | `start_points()` / `end_points()`         | `PointsInTime`       |
| `PointsInTime`       | `expand()` / `expand_left/right()`        | `Intervals`          |
| `PointsInTimeSeries` | `> >= < <= == !=`                         | `PointsInTime`       |
| `PointsInTimeSeries` | `sum()` / `mean()` / `min()` / `max()`    | scalar (unweighted)  |

`Intervals` also supports `expand()` / `shrink()`, `merge_overlaps()` / `merge_intervals(gap)`,
`debounce(d)`, and `filter(d)` (drop windows shorter than `d`). `where(PointsInTime)` drops any instant
that falls in a gap where the signal is not valid, so the result may be shorter than the input.

## Which type does each consumer need

- **Events** (`impulse-events`): `BasicEvent` and `SequenceOfEvents` need **`Intervals`**;
  `PointsInTimeEvent` needs **`PointsInTime`**. Validated at construction — a wrong type raises `ValueError`.
- **Aggregations** (`impulse-aggregations`): histogram/stats signal inputs must be **`SampleSeries`**.

## Common virtual-signal recipes

```python
# derived signal
delta_temp = intake_air_temp - amb_air_temp

# integration-based signal (distance from speed)
distance_km = veh_spd.resample(1e6).cumtrapz() / 3600 / 1e6

# distance/cycle binning: one interval per 10 km travelled
every_10km = (distance_km % 10).intervals_between_falling_edges()
```
