---
sidebar_position: 2
title: Aggregations
---

# Aggregations

Aggregations compute summary results over channels, optionally scoped to an event.
Impulse provides four aggregation types: **Histogram**, **Histogram2D**, **Statistics**, and
**PointValue** (sampling channel values at the instants of a `PointsInTimeEvent`).

Aggregations are organized into **Pages**, which group related visuals within a report.

:::note Expression types
Aggregation signal inputs — `base_expr`, `x_expr`, `y_expr`, `weights_expr`, and each element of
`input_expressions` — must evaluate to a **`SampleSeries`** (a channel-like signal). This is validated when
the aggregation is constructed; passing an expression that evaluates to something else (e.g. an `Intervals`
comparison or a `PointsInTime`) raises a `ValueError`.
:::

---

## Page

A `Page` is a logical container for aggregations. Pages are numbered for ordering and attached to the report.

```python
from impulse_reporting.core.page import Page

page = Page(page_number=1)
my_report.add_page(page)

page.add_aggregation(my_histogram)
page.add_aggregation(my_statistics)
```

| Parameter     | Type  | Description                                         |
|---------------|-------|-----------------------------------------------------|
| `page_number` | `int` | Page number for logical ordering within the report. |

The `page_number` is stored in dimension tables so downstream consumers can reconstruct report layout.

---

## Histogram

`Histogram` is an abstract base class. Use one of the concrete variants:

| Class                    | Bin weight                                                             |
|--------------------------|------------------------------------------------------------------------|
| `HistogramDuration`      | Sample duration. Default; makes results independent of sampling rate.  |
| `HistogramCustomWeights` | A second `weights_expr` time series (any TSAL expression).             |
| `HistogramDistance`      | Distance. Subclass of `HistogramCustomWeights` with distance weights.  |

The example below uses `HistogramDuration`. Bin counts are **weighted by sample duration** (not sample count), making results
independent of sampling rate.

```python
from impulse_reporting.aggregations.histogram import HistogramDuration

rpm_hist = HistogramDuration(
    name="rpm_hist",
    base_expr=eng_rpm,
    bins=[0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000],
    event=eng_rpm_event,
    desc="RPM distribution during high-RPM event",
    channel_name="Engine RPM",
    values_unit="s",
    bins_unit="rpm",
)
page.add_aggregation(rpm_hist)
```

### Parameters

| Parameter     | Type                   | Required | Description                                                                                |
|---------------|------------------------|----------|--------------------------------------------------------------------------------------------|
| `name`        | `str`                  | Yes      | Unique aggregation name.                                                                   |
| `base_expr`   | `TimeSeriesExpression` | Yes      | Signal expression to histogram.                                                            |
| `bins`        | `list[float]`          | Yes      | Bin edges. `N` edges produce `N-1` bins.                                                   |
| `event`       | `Event`                | No       | Event to scope the aggregation. If provided, only data within event instances is included. |
| `desc`        | `str`                  | No       | Description stored in the dimension table.                                                 |
| `agg_type`    | `str`                  | No       | Aggregation type label. Default: `"histogram_duration"`.                                   |
| `channel_name`| `str`                  | No       | Display name for the signal (stored as `channel_name` in the dimension table).             |
| `values_unit` | `str`                  | No       | Unit of histogram values (e.g. `"s"` for seconds).                                         |
| `bins_unit`   | `str`                  | No       | Unit of bin edges (e.g. `"rpm"`).                                                          |

### Output schema

**histogram_fact:**

| Column         | Type     | Description                                          |
|----------------|----------|------------------------------------------------------|
| `container_id` | `int`    | Container identifier.                                |
| `visual_id`    | `int`    | Foreign key to `histogram_dimension`.                |
| `event_id`     | `int`    | Foreign key to `event_dimension` (null if no event). |
| `bin_id`       | `int`    | Bin index (0-based).                                 |
| `hist_value`   | `double` | Duration-weighted bin count.                         |
| `lower_bound`  | `double` | Lower edge of the bin.                               |
| `upper_bound`  | `double` | Upper edge of the bin.                               |
| `bin_name`     | `str`    | Label: `"lower_bound-upper_bound"`.                  |

**histogram_dimension:**

| Column                | Type            | Description                                   |
|-----------------------|-----------------|-----------------------------------------------|
| `visual_id`           | `int`           | Unique aggregation identifier.                |
| `report_id`           | `int`           | Report identifier.                            |
| `name`                | `str`           | Aggregation name.                             |
| `page_number`         | `int`           | Page number.                                  |
| `description`         | `str`           | Description.                                  |
| `agg_type`            | `str`           | Aggregation type label.                       |
| `bins`                 | `array[double]` | Bin edges.                                    |
| `channel_name`         | `str`           | Signal display name.                          |
| `signal_expression`    | `str`           | String representation of the TSAL expression. |
| `weights_channel_name` | `str`           | Reserved for future use.                      |
| `weights_expression`   | `str`           | Reserved for future use.                      |
| `values_unit`          | `str`           | Unit of values.                               |
| `bins_unit`            | `str`           | Unit of bins.                                 |
| `definition_hash`      | `long`          | Hash of the aggregation definition; used by incremental processing to detect definition changes. |

### Example: generating bin edges

```python
bins = [float(i) for i in range(0, 5000, 250)]
```

This creates 20 bins from 0 to 4750 in steps of 250.

---

## Histogram2D

`Histogram2D` is an abstract base class. Use one of the concrete variants:

| Class                      | Bin weight                                                                  |
|----------------------------|-----------------------------------------------------------------------------|
| `Histogram2DDuration`      | Sample duration. Default.                                                   |
| `Histogram2DCustomWeights` | A user-supplied `weights_expr` time series.                                 |
| `Histogram2DDistance`      | Distance. Subclass of `Histogram2DCustomWeights` with distance weights.     |

Both signals are synchronized so they are comparable even when sampling frequencies differ.

```python
from impulse_reporting.aggregations.histogram2d import Histogram2DDuration

heatmap = Histogram2DDuration(
    name="rpm_vs_speed",
    x_expr=eng_rpm,
    y_expr=veh_spd,
    x_bins=[0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000],
    y_bins=[0, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200],
    event=container_event,
    desc="RPM vs Vehicle Speed heatmap",
    x_channel_name="Engine RPM",
    y_channel_name="Vehicle Speed",
    values_unit="s",
    x_bins_unit="rpm",
    y_bins_unit="km/h",
)
page.add_aggregation(heatmap)
```

### Parameters

| Parameter       | Type                   | Required | Description                                              |
|-----------------|------------------------|----------|----------------------------------------------------------|
| `name`          | `str`                  | Yes      | Unique aggregation name.                                 |
| `x_expr`        | `TimeSeriesExpression` | Yes      | Signal expression for the x-axis.                        |
| `y_expr`        | `TimeSeriesExpression` | Yes      | Signal expression for the y-axis.                        |
| `x_bins`        | `list[float]`          | Yes      | Bin edges for the x-axis.                                |
| `y_bins`        | `list[float]`          | Yes      | Bin edges for the y-axis.                                |
| `event`         | `Event`                | No       | Event to scope the aggregation.                          |
| `desc`          | `str`                  | No       | Description.                                             |
| `agg_type`      | `str`                  | No       | Aggregation type label. Default: `"histogram_duration"`. |
| `x_channel_name`| `str`                  | No       | Display name for the x-axis signal.                      |
| `y_channel_name`| `str`                  | No       | Display name for the y-axis signal.                      |
| `values_unit`   | `str`                  | No       | Unit of histogram values.                                |
| `x_bins_unit`   | `str`                  | No       | Unit of x-axis bins.                                     |
| `y_bins_unit`   | `str`                  | No       | Unit of y-axis bins.                                     |

### Output schema

**histogram2d_fact:**

| Column          | Type     | Description                             |
|-----------------|----------|-----------------------------------------|
| `container_id`  | `int`    | Container identifier.                   |
| `visual_id`     | `int`    | Foreign key to `histogram2d_dimension`. |
| `event_id`      | `int`    | Foreign key to `event_dimension`.       |
| `x_bin_id`      | `int`    | X-axis bin index.                       |
| `y_bin_id`      | `int`    | Y-axis bin index.                       |
| `hist_value`    | `double` | Duration-weighted bin count.            |
| `x_lower_bound` | `double` | Lower edge of the x bin.                |
| `x_upper_bound` | `double` | Upper edge of the x bin.                |
| `y_lower_bound` | `double` | Lower edge of the y bin.                |
| `y_upper_bound` | `double` | Upper edge of the y bin.                |
| `x_bin_name`    | `str`    | X-axis bin label.                       |
| `y_bin_name`    | `str`    | Y-axis bin label.                       |

**histogram2d_dimension:**

| Column                | Type            | Description                    |
|-----------------------|-----------------|--------------------------------|
| `visual_id`           | `int`           | Unique aggregation identifier. |
| `report_id`           | `int`           | Report identifier.             |
| `page_number`         | `int`           | Page number.                   |
| `name`                | `str`           | Aggregation name.              |
| `description`         | `str`           | Description.                   |
| `agg_type`            | `str`           | Aggregation type label.        |
| `x_bins`               | `array[double]` | X-axis bin edges.              |
| `y_bins`               | `array[double]` | Y-axis bin edges.              |
| `x_channel_name`       | `str`           | X-axis signal display name.    |
| `x_signal_expression`  | `str`           | X-axis TSAL expression.        |
| `y_channel_name`       | `str`           | Y-axis signal display name.    |
| `y_signal_expression`  | `str`           | Y-axis TSAL expression.        |
| `weights_channel_name` | `str`           | Reserved.                      |
| `weights_expression`   | `str`           | Reserved.                      |
| `values_unit`          | `str`           | Unit of values.                |
| `x_bins_unit`          | `str`           | Unit of x-axis bins.           |
| `y_bins_unit`          | `str`           | Unit of y-axis bins.           |
| `definition_hash`      | `long`          | Hash of the aggregation definition; used by incremental processing to detect definition changes. |

---

## StatsAggregator

Computes descriptive statistics for one or more signals within an event. Statistics are computed **per event instance**,
allowing per-interval breakdowns.

```python
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator

stats = StatsAggregator(
    name="signal_stats",
    input_expressions=[eng_rpm, veh_spd, avg_temp],
    channel_names=["Engine RPM", "Vehicle Speed", "Avg Temperature"],
    statistics=["min", "median", "mean", "max"],
    event=container_event,
    desc="Basic statistics per container",
)
page.add_aggregation(stats)
```

### Parameters

| Parameter            | Type                         | Required | Description                                                                           |
|----------------------|------------------------------|----------|---------------------------------------------------------------------------------------|
| `name`               | `str`                        | Yes      | Unique aggregation name.                                                              |
| `input_expressions`  | `list[TimeSeriesExpression]` | Yes      | Signal expressions to compute statistics for.                                         |
| `channel_names`      | `list[str]`                  | Yes      | Display names for each signal. Must match length of `input_expressions`.              |
| `statistics`         | `list[str]`                  | Yes      | Statistics to compute.                                                                |
| `event`              | `Event`                      | No       | Event to scope the aggregation. If None, statistics cover the entire series.          |
| `desc`               | `str`                        | No       | Description.                                                                          |
| `agg_type`           | `str`                        | No       | Aggregation type identifier. Defaults to `"stats_aggregator"`.                        |
| `values_unit`        | `str`                        | No       | Unit of the statistic values.                                                         |
| `per_channel_custom_statistics`  | `list[PerChannelStatistic]`  | No | Custom statistics computed once per channel per interval (see below).       |
| `cross_channel_custom_statistics`| `list[CrossChannelStatistic]`| No | Custom statistics computed once per interval across a set of channels (see below). |

### Supported statistics

| Label      | Description                              |
|------------|------------------------------------------|
| `"min"`    | Minimum value across the event instance. |
| `"max"`    | Maximum value across the event instance. |
| `"mean"`   | Duration-weighted mean value.            |
| `"median"` | Duration-weighted median value.          |
| `"start"`  | First value in the interval.             |
| `"end"`    | Last value in the interval.              |

A `ValueError` is raised if unsupported statistics are provided.

### Custom statistics

Beyond the built-in labels, you can inject your own statistic functions. Each is declared as a descriptor
and returns a **sequence of scalars** mapped positionally to its `aggregation_labels`; a single-output
statistic returns a one-element sequence. Custom outputs land in the fact table as ordinary
`aggregation_label` / `statistic_value` rows — no schema change — and their labels are added to the
dimension row's `statistics` array.

There are two kinds:

- **Per-channel** (`PerChannelStatistic`) — computed once per input channel per interval, exactly like a
  built-in. Fact rows carry the **real channel name**. Signature:
  `func(series: SampleSeries, t_start: float, t_end: float, **params) -> Sequence[float]`.
- **Cross-channel** (`CrossChannelStatistic`) — computed once per interval across a set of channels. Fact
  rows carry the descriptor's `channel_name` (or the output label when it is `None`). Signature:
  `func(series: list[SampleSeries], t_start: float, t_end: float, **params) -> Sequence[float]`.

```python
import numpy as np
from impulse_query_engine.analyze.query.aggregations.custom_statistic import (
    PerChannelStatistic,
    CrossChannelStatistic,
)

def percentiles(series, t_start, t_end):          # per-channel, multi-output
    return [float(np.nanpercentile(series.values, 50)),
            float(np.nanpercentile(series.values, 90))]

def spread(series, t_start, t_end):               # cross-channel, single output
    values = [v for s in series for v in s.values]
    return [float(max(values) - min(values))] if values else [float("nan")]

stats = StatsAggregator(
    name="custom_stats",
    input_expressions=[eng_rpm, veh_spd],
    channel_names=["Engine RPM", "Vehicle Speed"],
    statistics=["min", "max"],
    event=eng_rpm_event,
    per_channel_custom_statistics=[
        PerChannelStatistic(func=percentiles, aggregation_labels=["p50", "p90"]),
    ],
    cross_channel_custom_statistics=[
        CrossChannelStatistic(func=spread, aggregation_labels=["spread"],
                              channel_name="rpm_speed_spread"),
    ],
)
```

**Descriptor fields**

| Field                | Applies to        | Description                                                                             |
|----------------------|-------------------|-----------------------------------------------------------------------------------------|
| `func`               | both              | Callable returning a sequence of scalars (length must equal `aggregation_labels`).      |
| `aggregation_labels` | both (required)   | Output labels; become `aggregation_label`s in the fact table. Non-empty, unique.        |
| `params`             | both              | Keyword arguments passed to `func` on every call. Keys must be valid identifiers.       |
| `inputs`             | cross-channel     | Names (from `channel_names`) of the channels to pass, in order. `None` passes all.      |
| `channel_name`       | cross-channel     | `channel_name` for all output rows. `None` uses each output's `aggregation_label`.      |

Notes:

- Labels must be **globally unique** across built-ins and all custom statistics, and must not shadow a
  built-in label.
- `func` is cloudpickled to Spark executors: use a module-level importable function and never capture
  Spark objects.
- A statistic's `func`, labels, resolved inputs, `params`, and (for cross-channel) `channel_name` are part
  of the definition hash, so changing any of them triggers reprocessing. `channel_name` is included because
  it is the fact table's merge key — a rename must recompute so old-name rows are pruned rather than left
  stale in incremental mode.

### Output schema

**stats_aggregator_fact:**

| Column              | Type     | Description                                      |
|---------------------|----------|--------------------------------------------------|
| `container_id`      | `int`    | Container identifier.                            |
| `visual_id`         | `int`    | Foreign key to `stats_aggregator_dimension`.     |
| `channel_name`      | `str`    | Signal display name (or a cross-channel statistic's `channel_name`). |
| `event_id`          | `int`    | Event identifier.                                |
| `event_instance_id` | `long`   | Foreign key to `event_instance_fact`.            |
| `aggregation_label` | `str`    | Statistic label — a built-in (e.g. `"mean"`) or a custom `aggregation_labels` entry. |
| `statistic_value`   | `double` | Computed statistic value.                        |

**stats_aggregator_dimension:**

| Column               | Type         | Description                    |
|----------------------|--------------|--------------------------------|
| `visual_id`          | `int`        | Unique aggregation identifier. |
| `report_id`          | `int`        | Report identifier.             |
| `name`               | `str`        | Aggregation name.              |
| `page_number`        | `int`        | Page number.                   |
| `description`        | `str`        | Description.                   |
| `agg_type`           | `str`        | Aggregation type identifier.   |
| `statistics`         | `array[str]` | All output labels: built-ins plus every custom statistic's `aggregation_labels`. |
| `channel_names`      | `array[str]` | Signal display names.          |
| `signal_expressions` | `array[str]` | TSAL expression strings.       |
| `values_unit`        | `str`        | Unit of statistic values.      |
| `definition_hash`    | `long`       | Hash of computation definition.|

### Example: statistics with different event types

**Per-container statistics (full measurement):**

```python
container_stats = StatsAggregator(
    name="full_run_stats",
    input_expressions=[eng_rpm, veh_spd],
    channel_names=["Engine RPM", "Vehicle Speed"],
    statistics=["min", "max", "mean"],
    event=container_event,
)
```

**Per-event-instance statistics (within operating band):**

```python
band_stats = StatsAggregator(
    name="band_stats",
    input_expressions=[eng_rpm, veh_spd],
    channel_names=["Engine RPM", "Vehicle Speed"],
    statistics=["min", "median", "mean", "max"],
    event=eng_rpm_event,
)
```

---

## PointValueAggregator

`PointValueAggregator` samples one or more channels **at the instants of a
[`PointsInTimeEvent`](event#pointsintimeevent)** — for example at every rising edge — and records the
channel's value at each instant. Where `StatsAggregator` summarizes a channel over event *intervals*,
`PointValueAggregator` reads a single value *at each point in time*.

Each input channel is filtered with `.where(<event points>)`, which yields the value valid at every
point; one fact row is produced per (channel, instant).

```python
from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent

eng_rpm = my_report.get_db().query.channel(channel_name="Engine RPM")

rpm_rising = PointsInTimeEvent(name="rpm_rising", expr=eng_rpm.rising_edges())
my_report.add_event(rpm_rising)

rpm_at_edges = PointValueAggregator(
    name="rpm_at_edges",
    input_expressions=[eng_rpm],
    channel_names=["Engine RPM"],
    event=rpm_rising,
    desc="Engine RPM sampled at its rising edges",
)
page.add_aggregation(rpm_at_edges)
```

### Parameters

| Parameter           | Type                         | Required | Description                                                                                     |
|---------------------|------------------------------|----------|-------------------------------------------------------------------------------------------------|
| `name`              | `str`                        | Yes      | Unique aggregation name.                                                                        |
| `input_expressions` | `list[TimeSeriesExpression]` | Yes      | Channel expressions to sample. Each must evaluate to a `SampleSeries`.                          |
| `channel_names`     | `list[str]`                  | Yes      | Display names for each signal. Must match the length of `input_expressions`.                    |
| `event`             | `PointsInTimeEvent`          | Yes      | Event whose instants define where the channels are sampled. **Must be a `PointsInTimeEvent`.**  |
| `desc`              | `str`                        | No       | Description stored in the dimension table.                                                      |
| `values_unit`       | `str`                        | No       | Unit of the sampled values.                                                                     |

### How it works

1. The `event`'s expression is solved to a `PointsInTime` (the instants).
2. Each input channel is sampled at those instants via `SampleSeries.where(points)`. A point that
   falls outside a channel's coverage is simply omitted for that channel.
3. Each (channel, instant) becomes one fact row.

### Output schema

`PointValueAggregator` reuses the **`stats_aggregator_fact`** and **`stats_aggregator_dimension`**
tables (see [StatsAggregator output schema](#output-schema-2)). In the fact rows:

- `aggregation_label` is always `"value"`.
- `statistic_value` is the sampled channel value at the instant.
- `event_instance_id` matches the zero-duration instance materialized by the `PointsInTimeEvent`
  (`start_ts == end_ts`), so fact rows join to that event's instances in `event_instance_fact`.

Rows from `StatsAggregator` and `PointValueAggregator` coexist in the same tables and are distinguished
by `agg_type` (`"point_value_aggregator"`).
