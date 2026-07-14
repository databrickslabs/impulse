---
name: impulse-aggregations
description: >
  Compute results over channels in Impulse, optionally scoped to an event. Use when the user wants a
  "histogram" or "2D histogram / heatmap" of a signal, duration- or distance-weighted binning,
  per-event descriptive statistics (min/max/mean/median), or channel values sampled at instants. Covers
  HistogramDuration / HistogramDistance / HistogramCustomWeights, the Histogram2D variants,
  StatsAggregator, PointValueAggregator, and the Page that groups them, plus their gold-layer output.
---

# Impulse — aggregations

Aggregations compute summary results over channels, optionally scoped to an event (see
`impulse-events`). They are grouped into **pages** and attached to a report.

All signal inputs — `base_expr`, `x_expr`, `y_expr`, `weights_expr`, and each element of
`input_expressions` — must evaluate to a **`SampleSeries`** (see `impulse-tsal`). Passing anything else
(e.g. an `Intervals` comparison) raises `ValueError` at construction.

## Page

A `Page` is a logical group of aggregations, numbered for ordering.

```python
from impulse_reporting.core.page import Page

page = Page(page_number=1)
report.add_page(page)
page.add_aggregation(my_histogram)
page.add_aggregation(my_stats)
```

## Histogram (1D)

`Histogram` is abstract — use a concrete variant by bin weight:

| Class                    | Bin weight                                                          |
|--------------------------|--------------------------------------------------------------------|
| `HistogramDuration`      | Sample duration (default; independent of sampling rate).           |
| `HistogramDistance`      | Distance (subclass of custom-weights with distance weights).       |
| `HistogramCustomWeights` | A second `weights_expr` time series.                               |

```python
from impulse_reporting.aggregations.histogram import HistogramDuration

rpm_hist = HistogramDuration(
    name="rpm_hist",
    base_expr=eng_rpm,
    bins=[0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000],   # N edges -> N-1 bins
    event=rpm_band,                # optional; scopes to event instances
    desc="RPM distribution during high-RPM event",
    channel_name="Engine RPM",
    values_unit="s",
    bins_unit="rpm",
)
page.add_aggregation(rpm_hist)
```

| Parameter      | Type                   | Required | Description                                             |
|----------------|------------------------|----------|---------------------------------------------------------|
| `name`         | `str`                  | Yes      | Unique aggregation name.                                |
| `base_expr`    | `TimeSeriesExpression` | Yes      | Signal to histogram (`SampleSeries`).                   |
| `bins`         | `list[float]`          | Yes      | Bin edges. `N` edges → `N-1` bins.                      |
| `event`        | `Event`                | No       | Scope to event instances; omit for the whole series.    |
| `desc`         | `str`                  | No       | Description.                                            |
| `channel_name` | `str`                  | No       | Display name stored in the dimension.                   |
| `values_unit`  | `str`                  | No       | Unit of histogram values (e.g. `"s"`).                  |
| `bins_unit`    | `str`                  | No       | Unit of bin edges (e.g. `"rpm"`).                       |

Generate regular bins with a comprehension:

```python
bins = [float(i) for i in range(0, 5000, 250)]   # 20 bins, 0..4750 step 250
```

## Histogram2D

Abstract — variants `Histogram2DDuration` (default), `Histogram2DDistance`, `Histogram2DCustomWeights`.
Both signals are synchronized so they are comparable even at different sampling rates.

```python
from impulse_reporting.aggregations.histogram2d import Histogram2DDuration

heatmap = Histogram2DDuration(
    name="rpm_vs_speed",
    x_expr=eng_rpm,
    y_expr=veh_spd,
    x_bins=[0, 2000, 4000, 6000, 8000],
    y_bins=[0, 40, 80, 120, 160, 200],
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

Required: `name`, `x_expr`, `y_expr`, `x_bins`, `y_bins`. Optional: `event`, `desc`, `x_channel_name`,
`y_channel_name`, `values_unit`, `x_bins_unit`, `y_bins_unit`.

## StatsAggregator

Descriptive statistics for one or more signals, computed **per event instance**.

```python
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator

stats = StatsAggregator(
    name="signal_stats",
    input_expressions=[eng_rpm, veh_spd, avg_temp],
    channel_names=["Engine RPM", "Vehicle Speed", "Avg Temperature"],   # same length as inputs
    statistics=["min", "median", "mean", "max"],
    event=container_event,
    desc="Basic statistics per container",
)
page.add_aggregation(stats)
```

| Parameter           | Type                         | Required | Description                                             |
|---------------------|------------------------------|----------|---------------------------------------------------------|
| `name`              | `str`                        | Yes      | Unique aggregation name.                                |
| `input_expressions` | `list[TimeSeriesExpression]` | Yes      | Signals to summarize (each a `SampleSeries`).           |
| `channel_names`     | `list[str]`                  | Yes      | Display names; must match `input_expressions` length.   |
| `statistics`        | `list[str]`                  | Yes      | See supported labels below.                             |
| `event`             | `Event`                      | No       | Scope; if omitted, covers the entire series.            |
| `desc`              | `str`                        | No       | Description.                                            |
| `values_unit`       | `str`                        | No       | Unit of the statistic values.                           |

Supported statistics: `"min"`, `"max"`, `"mean"` (duration-weighted), `"median"` (duration-weighted),
`"start"` (first value), `"end"` (last value). Unsupported labels raise `ValueError`.

## PointValueAggregator

Samples channels **at the instants of a `PointsInTimeEvent`** (see `impulse-events`) — one value per
(channel, instant). Where `StatsAggregator` summarizes over intervals, this reads a single value at
each point.

```python
from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent

eng_rpm = report.get_db().query.channel(channel_name="Engine RPM")
rpm_rising = PointsInTimeEvent(name="rpm_rising", expr=eng_rpm.rising_edges())
report.add_event(rpm_rising)

rpm_at_edges = PointValueAggregator(
    name="rpm_at_edges",
    input_expressions=[eng_rpm],
    channel_names=["Engine RPM"],
    event=rpm_rising,               # MUST be a PointsInTimeEvent
    desc="Engine RPM sampled at its rising edges",
)
page.add_aggregation(rpm_at_edges)
```

Required: `name`, `input_expressions`, `channel_names`, `event` (must be a `PointsInTimeEvent`).
Optional: `desc`, `values_unit`. A point outside a channel's coverage is omitted for that channel.

## Output schema

- **`histogram_fact`** — `container_id`, `visual_id`, `event_id`, `bin_id`, `hist_value`,
  `lower_bound`, `upper_bound`, `bin_name`. **`histogram_dimension`** carries `bins`, `channel_name`,
  `signal_expression`, units, and `definition_hash`.
- **`histogram2d_fact`** — as above with `x_bin_id`/`y_bin_id` and per-axis bounds/labels;
  **`histogram2d_dimension`** carries both axes' bins, names, expressions, and units.
- **`stats_aggregator_fact`** — `container_id`, `visual_id`, `channel_name`, `event_id`,
  `event_instance_id`, `aggregation_label`, `statistic_value`. **`stats_aggregator_dimension`** carries
  `statistics`, `channel_names`, `signal_expressions`, `values_unit`, `definition_hash`.

`PointValueAggregator` reuses the `stats_aggregator_*` tables: `aggregation_label` is always `"value"`,
`statistic_value` is the sampled value, and rows are distinguished from `StatsAggregator` by
`agg_type = "point_value_aggregator"`.

Persisting these to gold and computing them is the job of the report — see `impulse-reporting`.
