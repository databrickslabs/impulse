---
sidebar_label: pipeline
title: impulse_query_engine.analyze.query.optimizer.pipeline
---

Spark execution of a pushdown plan: one shared pass for all rules.


#### CHUNK\_ROWS

Average number of samples per chunk of a series.


#### apply\_pushdowns

```python
def apply_pushdowns(rows: DataFrame,
                    plan: PushdownPlan,
                    config,
                    include_raw: bool = True) -> DataFrame
```

The rows a solve ships: raw rows (when *include_raw*) and all derived rows.

**Arguments**:

- `rows` (`pyspark.sql.DataFrame`): Channel data ``(container_id, channel_id, tstart, tend, value)`` tagged
with :data:`VARIANT_COL`, one copy per variant a channel ships.
- `plan` (`PushdownPlan`): The solve's pushdown plan.
- `config` (`SolverConfig`): Provides the internal column names.
- `include_raw` (`bool`): Whether to keep the raw rows (variant 0).

**Returns**:

`pyspark.sql.DataFrame`: Rows with the columns and types of *rows*.

## SeriesPipeline

```python
class SeriesPipeline()
```

The ordered pass shared by all rules that need their series in order.

**Arguments**:

- `rules` (`Sequence of (PushdownRule, Mapping[int, Pushdown])`): Ordered rules with their variants in this solve.
- `config` (`SolverConfig`): Provides the internal column names.

#### run

```python
def run(rows: DataFrame) -> DataFrame
```

Replace the samples of the rules' variants with their derived rows.


