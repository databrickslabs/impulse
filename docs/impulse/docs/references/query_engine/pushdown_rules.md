---
sidebar_position: 4
title: Pushdown Rules
---

# Pushdown Rules

With [Spark pushdown](query_solvers.md#spark-pushdown-opt-in) enabled, each rule computes one kind
of subtree natively in Spark, with the same result as the pandas evaluation. A subtree is pushed
down only if every use of its channel in the solve is ([When a channel is pushed
down](query_solvers.md#spark-pushdown-opt-in)); otherwise it runs in pandas as before.

| Rule | Subtree | Result | Solve without the UDF |
|------|---------|--------|-----------------------|
| [`resample`](#resample) | `channel.resample(sample_rate)` | `SampleSeries` | As a calculated channel |
| [`condition`](#condition) | `channel > k`, also `>=`, `<`, `<=`, `==`, `!=` | `Intervals` | Yes |

Both apply to continuous `channel()` and `channel_with_alias()` selections, and their subtree can
sit anywhere in an expression, e.g. `veh_spd.resample(1e6).cumtrapz()` or
`(rpm > 4000).debounce(1e6)`.

## resample

- **Matches** `channel.resample(sample_rate)` with a positive, finite rate (default `1`).
- **Computes** the resampled series per continuous block, like `SampleSeries.resample`.
- **Limits:**
  - If the timestamps are integers (e.g. `long` epoch microseconds), the rate must be a whole
    number too, e.g. `resample(1e6)`; otherwise, e.g. `resample(2.5)`, the resample runs in
    pandas.
  - The rule is meant for downsampling: a rate finer than the channel's sample spacing produces
    **more** rows than the raw channel.
- **API:** [`ResamplePushdownRule`](../api/impulse_query_engine/analyze/query/optimizer/resample_rule.md)

## condition

- **Matches** a comparison of a channel with a number, on either side (`500 < rpm`). The number is
  a regular Python or numpy integer or float, e.g. `4000` or `80.5`; other number types, e.g.
  `Fraction`, are compared in pandas.
- **Computes** the intervals where the comparison holds, merged like
  `Intervals(..., merge_overlaps=True)`.
  - Comparisons follow numpy: NaN and null values satisfy only `!=`.
  - Aliased selections compare the unit-converted values.
- **Limits:**
  - The comparison must be on the channel itself: in `rpm.resample(1e6) > 4000`, only the
    resample is pushed down.
- **Without the UDF:** solves whose selections are all comparisons
  ([Solves without the pandas UDF](query_solvers.md#solves-without-the-pandas-udf)).
- **API:** [`ConditionPushdownRule`](../api/impulse_query_engine/analyze/query/optimizer/condition_rule.md)

## Planned rules

- **Scalar maps**, e.g. `rpm * 60 / 1000`.
- **`where` on the same channel**, e.g. `veh_spd.where(veh_spd > 0)`.
- **Compositions** on one channel, e.g. `rpm.resample(1e6) > 4000`.
- **Interval operations**: `debounce`, `merge_intervals`, `&`, `|`, `~`.
- **Per-series aggregates**: statistics and histograms in Spark.
