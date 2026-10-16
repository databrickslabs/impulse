---
sidebar_label: custom_statistic
title: impulse_query_engine.analyze.query.aggregations.custom_statistic
---

Descriptors for custom statistics (per-channel and cross-channel).


## PerChannelStatistic

```python
class PerChannelStatistic()
```

Descriptor for a single per-channel custom statistic.

**Arguments**:

- `func` (`Callable`): Function with signature ``func(series: SampleSeries, t_start: float,
t_end: float, **params) -> Sequence[float]``. Called once per input
channel and event interval with the channel's series clipped to the
interval. It must return a sequence of scalars whose length equals
``aggregation_labels`` (a single label still requires a one-element
sequence, e.g. ``[value]``). The series may be empty; return
``float("nan")`` entries for undefined results. The function is
cloudpickled to Spark executors, so a module-level importable function is
recommended; never capture Spark objects.
- `aggregation_labels` (`list of str`): Output labels this statistic produces. The values returned by ``func``
are mapped positionally to these labels, which become the keys of the
statistic's result maps. Labels must be non-empty, unique strings;
changing them changes the aggregation's definition hash.
- `params` (`dict`): Keyword arguments passed to ``func`` on every invocation
(``func(series, t_start, t_end, **params)``). Keys must be valid Python
identifiers matching parameter names of ``func``. Changing params
changes the aggregation's definition hash.

## CrossChannelStatistic

```python
class CrossChannelStatistic()
```

Descriptor for a single cross-channel custom statistic.

**Arguments**:

- `func` (`Callable`): Function with signature ``func(series: list[SampleSeries], t_start: float,
t_end: float, **params) -> Sequence[float]``. Called once per event
interval with the series listed in ``inputs`` (clipped to the interval,
in declared order). It must return a sequence of scalars whose length
equals ``aggregation_labels`` (a single label still requires a
one-element sequence, e.g. ``[value]``). Any series may be empty; return
``float("nan")`` entries for undefined results. The function is
cloudpickled to Spark executors, so a module-level importable function is
recommended; never capture Spark objects.
- `aggregation_labels` (`list of str`): Output labels this statistic produces. The values returned by ``func``
are mapped positionally to these labels, which become the keys of the
statistic's result maps. Labels must be non-empty, unique strings;
changing them changes the aggregation's definition hash.
- `inputs` (`list of str`): Names of the input channels the function requires, resolved against the
aggregator's ``input_names``. ``None`` (default) passes all input
channels in input order.
- `channel_name` (`str`): A channel name applied to all of the statistic's output rows. Consumed by
downstream consumers (e.g. the reporting layer) only; ignored by the
query engine. ``None`` (default) leaves it to the consumer, which
typically falls back to each output's ``aggregation_label``.
- `params` (`dict`): Keyword arguments passed to ``func`` on every invocation
(``func(series, t_start, t_end, **params)``). Keys must be valid Python
identifiers matching parameter names of ``func``. Changing params
changes the aggregation's definition hash.

#### normalize\_per\_channel\_statistics

```python
def normalize_per_channel_statistics(
    per_channel_custom_statistics: list[PerChannelStatistic] | None
) -> list[PerChannelStatistic]
```

Validate a per-channel statistics list.

**Arguments**:

- `per_channel_custom_statistics` (`list or None`): List of ``PerChannelStatistic`` descriptors.

**Raises**:

- `TypeError`: If the value is not a list, an item is not a ``PerChannelStatistic``
with a callable ``func``, or params/labels are invalid.
- `ValueError`: If a descriptor's ``aggregation_labels`` are not unique.

**Returns**:

`list of PerChannelStatistic`: The validated list; empty when the input is ``None``.

#### normalize\_cross\_channel\_statistics

```python
def normalize_cross_channel_statistics(
    cross_channel_custom_statistics: list[CrossChannelStatistic] | None
) -> list[CrossChannelStatistic]
```

Validate a cross-channel statistics list.

**Arguments**:

- `cross_channel_custom_statistics` (`list or None`): List of ``CrossChannelStatistic`` descriptors.

**Raises**:

- `TypeError`: If the value is not a list, an item is not a ``CrossChannelStatistic``
with a callable ``func``, or params/labels are invalid.
- `ValueError`: If a descriptor's ``aggregation_labels`` are not unique.

**Returns**:

`list of CrossChannelStatistic`: The validated list; empty when the input is ``None``.

