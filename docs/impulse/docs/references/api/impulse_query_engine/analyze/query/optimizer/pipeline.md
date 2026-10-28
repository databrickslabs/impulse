---
sidebar_label: pipeline
title: impulse_query_engine.analyze.query.optimizer.pipeline
---

Spark execution of a pushdown plan: one shared pass for all rules.


#### CHUNK\_ROWS

Average number of rows per chunk of a channel, counting a sample once per variant.


#### apply\_pushdowns

```python
def apply_pushdowns(rows: DataFrame,
                    variants: DataFrame,
                    plan: PushdownPlan,
                    config,
                    include_raw: bool = True) -> DataFrame
```

The rows a solve ships: raw rows (when *include_raw*) and all derived rows.

**Arguments**:

- `rows` (`pyspark.sql.DataFrame`): Channel data ``(container_id, channel_id, tstart, tend, value)``, one row
per sample.
- `variants` (`pyspark.sql.DataFrame`): ``(container_id, channel_id, VARIANTS_COL)``: the variant ids each matched
channel ships, one row per channel (see :meth:`PushdownPlan.channel_variants`).
Channels without a row are dropped.  Both branches broadcast this one frame.
- `plan` (`PushdownPlan`): The solve's pushdown plan.
- `config` (`SolverConfig`): Provides the internal column names.
- `include_raw` (`bool`): Whether to keep the raw rows (variant 0).

**Returns**:

`pyspark.sql.DataFrame`: Rows with the columns and types of *rows* plus :data:`VARIANT_COL`.

## SeriesPipeline

```python
class SeriesPipeline()
```

The ordered pass shared by all rules that need their series in order.

Chunking and everything that crosses chunk edges runs once per channel; its
samples fan out into one series per variant only for the rules' hooks.  The
shuffle therefore carries each sample once, while a task after it processes
about k rows per shuffled sample of a channel with k variants (chunks are k
times smaller to keep the rows per chunk at :data:`CHUNK_ROWS`).

Rule runs are computed for every channel of the pass; a rule only reads its
own runs, on its own series.

**Arguments**:

- `rules` (`Sequence of (PushdownRule, Mapping[int, Pushdown])`): Ordered rules with their variants in this solve.
- `config` (`SolverConfig`): Provides the internal column names.

#### run

```python
def run(rows: DataFrame, variants: DataFrame) -> DataFrame
```

Replace the samples of the rules' variants with their derived rows.

*rows* holds the channel data, one row per sample; *variants* the variant
ids of each channel (see :func:`apply_pushdowns`).  Only channels shipping a
variant of the rules are processed.


