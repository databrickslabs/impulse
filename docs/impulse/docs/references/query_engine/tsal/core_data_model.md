---
sidebar_position: 2
title: Core Data Model
---

# Core Data Model

A TSAL expression resolves to one of a small set of in-memory, numpy-backed classes — the **core data
model**. Channel selection and arithmetic produce a `SampleSeries`, comparisons produce `Intervals`,
edge detection produces `PointsInTime`, and sampling at instants produces a `PointsInTimeSeries`.
([Evaluation](evaluation.md) explains how a query produces these per container.)

| Class                | Carries values? | Has duration? | Typical source                                  |
|----------------------|-----------------|---------------|-------------------------------------------------|
| `SampleSeries`       | yes             | yes           | channel selection, arithmetic, resampling       |
| `Intervals`          | no              | yes           | comparison / logical operators, edge windows    |
| `PointsInTime`       | no              | no            | `rising_edges()` / `falling_edges()`            |
| `PointsInTimeSeries` | yes             | no            | sampling a signal at instants via `where(...)`  |

:::note Not the storage schema
This page describes the **in-memory result classes** a query evaluates to. It is unrelated to the
[Data Model](../../../data_model/index.md) section, which documents the **silver-layer storage schema**
(the Delta tables Impulse reads from). The core data model lives only in memory during query
execution.
:::

## The classes at a glance

```mermaid
graph LR
  SS["SampleSeries"] -->|comparison| IV["Intervals"]
  SS -->|"rising / falling edges"| PIT["PointsInTime"]
  SS -->|"where(Intervals)"| SS
  SS -->|"where(PointsInTime)"| PITS["PointsInTimeSeries"]
  SS -->|"min / max / mean / sum"| SC(["scalar"])
  IV -->|"intersect with PointsInTime"| PIT
  IV -->|"start / end points"| PIT
  PIT -->|expand| IV
  PITS -->|compare| PIT
  PITS -->|"min / max / mean / sum"| SC
```

---

## SampleSeries

A measured signal, stored as three aligned arrays `(tstarts, tends, values)`. Two distinct
assumptions define its semantics:

- **The series is *valid* across its intervals.** Each half-open `[tstart_i, tend_i)` is an interval
  over which the signal was available (being recorded). It is this validity — not the values — that
  synchronization relies on. The intervals may have gaps; in a gap the signal is not valid.
- **Each value is a measurement that stands as the most recent one.** A value `v_i` was measured *at*
  `tstart_i`, and no other value was measured until `tend_i` (exclusive). So at any instant inside
  `[tstart_i, tend_i)`, `v_i` is the most recently measured value — not necessarily the "true" value
  at that instant.

```python
SampleSeries([0, 1, 2], [1, 2, 3], [10, 20, 30])
# the signal is valid on [0, 3); 10 was measured at t=0 and is the most
# recent value until t=1, 20 the most recent until t=2, 30 until t=3
```

These two assumptions are what make signals composable: two `SampleSeries` recorded at different
sample rates can be **synchronized** onto a common set of intervals (using their validity) before an
operation, and aggregations are **duration-weighted** — a measurement that is the most recent value
for 2 s counts twice as much as one that stands for 1 s.

Key operations: arithmetic (`+ - * / %`) and comparisons (which produce `Intervals`),
`where(...)`, `resample()`, `synchronized()`, `rolling_average()` / `rolling_stats()`,
`trapz()` / `cumtrapz()`, `rising_edges()` / `falling_edges()`, `histogram()`, and the
duration-weighted reducers `sum()` / `min()` / `max()` / `mean()`.

→ API: [`SampleSeries`](../../api/impulse_query_engine/model/series/sample_series.md)

---

## Intervals

A set of time windows `(tstarts, tends)` with **no values** — just *when* something is true. This is
the result of every comparison and logical operator, and therefore the building block of events.

```python
eng_rpm > 2000          # Intervals: every window where RPM exceeds 2000
(a > 2000) & (a < 5000) # intersection of two Interval sets
```

Key operations: `&` (intersection), `|` (union), `expand()` / `shrink()` (grow or contract window
bounds), `merge_overlaps()` / `merge_intervals(gap)`, `debounce(d)`, and `filter(d)` (drop windows
shorter than `d`). `start_points()` / `end_points()` turn the window boundaries into a
`PointsInTime`. Intersecting `Intervals` with a `PointsInTime` keeps only the points that fall
inside a window and returns a `PointsInTime`.

→ API: [`Intervals`](../../api/impulse_query_engine/model/series/intervals.md)

---

## PointsInTime

A set of bare timestamps `(tstarts)` — **no duration, no values**. Produced by edge detection on a
`SampleSeries`, where each timestamp marks the instant the signal rose or fell.

```python
eng_rpm.rising_edges()   # PointsInTime: instants where RPM increased
```

Key operations: `&` / `|` (set intersection / union by timestamp) and `expand()` / `expand_left()`
/ `expand_right()`, which widen each point into a window and return `Intervals`.

→ API: [`PointsInTime`](../../api/impulse_query_engine/model/series/points_in_time.md)

---

## PointsInTimeSeries

A timestamp→value series `(tstarts, values)`. It is the value-carrying counterpart of
`PointsInTime`, and the point-wise counterpart of `SampleSeries` — but with one decisive difference
from `SampleSeries`: **a value pertains only *to* its own timestamp** and makes no claim about the
signal in between consecutive timestamps. There are no durations and no most-recent-value carried
forward — each value stands alone at its instant.

The natural way to obtain one is to **sample a signal at specific instants** — e.g. read engine RPM
exactly at the moments the vehicle starts moving:

```python
rpm_at_starts = eng_rpm.where(veh_spd.rising_edges())  # PointsInTimeSeries
```

Because there is no validity between points, the operators differ from `SampleSeries`:

- **Arithmetic** (`+ - * /`) with another `PointsInTimeSeries` aligns the two on **exactly matching
  timestamps** (a value at one instant can only combine with a value at the *same* instant); with a
  scalar it applies element-wise. A `SampleSeries` operand is sampled at this series' instants first.
- **Comparisons** (`> >= < <= == !=`) return a `PointsInTime` — the instants where the condition
  holds (mirroring how `SampleSeries` comparisons return `Intervals`).
- **Aggregations** `sum()` / `mean()` / `min()` / `max()` / `count()` are **unweighted** (plain
  reductions over the values), since there are no durations to weight by — in contrast to
  `SampleSeries`' duration-weighted reducers.
- **`synchronized()` / `synchronized_all()`** align this series with a `SampleSeries` or another
  `PointsInTimeSeries` onto their shared instants, returning value-carrying point series.
- **String-valued POI series** — produced by `poi_channel(dtype='string')` (e.g. DTC fault codes) —
  support only equality (`==` / `!=`) and sampling (`synchronized` / `.where`); arithmetic, ordering,
  and numeric reductions raise, since they are meaningless for strings.

→ API: [`PointsInTimeSeries`](../../api/impulse_query_engine/model/series/points_in_time_series.md)

---

## How the classes interact

Operations move between the four classes (and scalars) in well-defined ways:

| From                 | Operation                                      | Produces             |
|----------------------|------------------------------------------------|----------------------|
| `SampleSeries`       | `+ - * /` with a scalar or another series      | `SampleSeries`       |
| `SampleSeries`       | `> >= < <= == !=`                              | `Intervals`          |
| `SampleSeries`       | `rising_edges()` / `falling_edges()`           | `PointsInTime`       |
| `SampleSeries`       | `where(Intervals)`                             | `SampleSeries`       |
| `SampleSeries`       | `where(PointsInTime)`                          | `PointsInTimeSeries` |
| `SampleSeries`       | `sum()` / `min()` / `max()` / `mean()`         | scalar               |
| `Intervals`          | `&` / `\|`                                     | `Intervals`          |
| `Intervals`          | `& PointsInTime`                               | `PointsInTime`       |
| `Intervals`          | `start_points()` / `end_points()`              | `PointsInTime`       |
| `PointsInTime`       | `expand()` / `expand_left()` / `expand_right()`| `Intervals`          |
| `PointsInTimeSeries` | `> >= < <= == !=`                              | `PointsInTime`       |
| `PointsInTimeSeries` | `+ - * /`                                      | `PointsInTimeSeries` |
| `PointsInTimeSeries` | `sum()` / `mean()` / `min()` / `max()`         | scalar               |

`where(PointsInTime)` is the bridge from a continuous signal to a point series: for each requested
instant it takes the most recently measured value at that instant — the value of the sample interval
`[tstart, tend)` that contains it — and keeps it. **Points that fall outside every sample interval**
— in a gap, before the first or after the last sample, where the signal is not valid — **are
dropped**, so the result may be shorter than the input. (The trailing sample is treated as a closed
point, so a query exactly at the final timestamp returns the last value.)

A short example chaining several transitions — read the engine RPM at each wheel-speed rising edge,
then keep only the high-RPM starts:

```python
starts = veh_spd.rising_edges()            # PointsInTime
rpm_at_starts = eng_rpm.where(starts)      # PointsInTimeSeries
hot_starts = rpm_at_starts > 3000          # PointsInTime (instants only)
avg_start_rpm = rpm_at_starts.mean()       # scalar (unweighted)
```
