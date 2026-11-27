---
sidebar_label: pipeline
title: impulse_query_engine.analyze.query.optimizer.pipeline
---

Spark execution of a pushdown plan: one shared pass for all rules.


#### CHUNK\_ROWS

Default ``chunk_rows``: average rows per chunk of a channel, a sample counted once per variant.


#### apply\_pushdowns

```python
def apply_pushdowns(rows: DataFrame,
                    variants: DataFrame,
                    plan: PushdownPlan,
                    config,
                    include_raw: bool = True,
                    chunk_rows: int = CHUNK_ROWS) -> DataFrame
```

The rows a solve ships: raw rows (when *include_raw*) and all derived rows.

**Arguments**:

- `rows` (`pyspark.sql.DataFrame`): Channel data ``(container_id, channel_id, tstart, tend, value)``, one row
per sample.
- `variants` (`pyspark.sql.DataFrame`): ``(container_id, channel_id, VARIANTS_COL)``: the variant ids each matched
channel ships, one row per channel (see :meth:`PushdownPlan.channel_variants`).
Channels without a row are dropped.  An optional :data:`FACTOR_COL` holds the
channel's unit-conversion factor, for rules that compute on converted values
(:meth:`~.pushdown.PushdownRule.needs_factor`).
- `plan` (`PushdownPlan`): The solve's pushdown plan.
- `config` (`SolverConfig`): Provides the internal column names.
- `include_raw` (`bool`): Whether to keep the raw rows (variant 0).
- `chunk_rows` (`int`): Average number of rows per chunk of a channel in the ordered pass, counting a
sample once per variant.

**Returns**:

`pyspark.sql.DataFrame`: Rows with the columns and types of *rows* plus :data:`VARIANT_COL`.  When a
rule may emit nothing (:attr:`~.pushdown.PushdownRule.may_emit_nothing`),
every channel shipping one of its variants also ships one row of
:data:`PRESENCE_VARIANT`.  Rules that finish
(:attr:`~.pushdown.PushdownRule.finishes`) still need :func:`finish_rows`.

## SeriesPipeline

```python
class SeriesPipeline()
```

The ordered pass shared by all rules that need their series in order.

Chunking and everything that crosses chunk edges runs once per channel; its
samples fan out into one series per variant only for the rules' hooks.  The
shuffle therefore carries each sample once, while a task after it processes
about k rows per shuffled sample of a channel with k variants (chunks are k
times smaller to keep the rows per chunk at *chunk_rows*).

Rule runs are computed for every channel of the pass; a rule only reads its
own runs, on its own series.

**Arguments**:

- `rules` (`Sequence of (PushdownRule, Mapping[int, Pushdown])`): Ordered rules with their variants in this solve.
- `config` (`SolverConfig`): Provides the internal column names.
- `chunk_rows` (`int`): Average number of rows per chunk of a channel, counting a sample once per variant.

#### run

```python
def run(rows: DataFrame, variants: DataFrame) -> DataFrame
```

Replace the samples of the rules' variants with their derived rows.

*rows* holds the channel data, one row per sample; *variants* the variant
ids of each channel (see :func:`apply_pushdowns`).  Only channels shipping a
variant of the rules are processed.


#### finishing\_rules

```python
def finishing_rules(
        plan: PushdownPlan) -> list[tuple[PushdownRule, dict[int, Pushdown]]]
```

The plan's rules with a finishing pass, with their variants.


#### series\_order

```python
def series_order(config) -> list[Column]
```

Sort keys of a channel's rows as the UDF's cache orders them.

By ``tstart``, then ``tend``, NaN and null last and equal to each other, as
``numpy.lexsort`` orders float64 arrays.


#### finish\_rows

```python
def finish_rows(df: DataFrame, plan: PushdownPlan, config) -> DataFrame
```

Apply the finishing pass of the plan's rules to the shipped rows *df*.

Each finishing rule sees all derived rows of each of its series (one channel's
rows of one variant in a container) in order (:func:`series_order`), through a
:class:`~.context.RowsContext`, and decides which rows are shipped and what
they hold.  Rows of other variants pass unchanged; all columns of *df* are kept.

The window is partitioned by container, channel and variant: called on rows
already partitioned by container, it adds one sort, no shuffle, and leaves the
rows sorted by container, channel and variant.

**Arguments**:

- `df` (`pyspark.sql.DataFrame`): Shipped rows with :data:`VARIANT_COL` (see :func:`apply_pushdowns`), possibly
with further columns.
- `plan` (`PushdownPlan`): The solve's pushdown plan.
- `config` (`SolverConfig`): Provides the internal column names.

**Returns**:

`pyspark.sql.DataFrame`: *df* with the finishing rules' rows replaced by their finished rows.

