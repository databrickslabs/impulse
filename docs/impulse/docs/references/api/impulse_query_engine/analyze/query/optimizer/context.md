---
sidebar_label: context
title: impulse_query_engine.analyze.query.optimizer.context
---

What a pushdown rule sees of the samples it computes on.

A rule never handles DataFrames.  The pipeline (:mod:`.pipeline`) evaluates the
rule's hooks with a :class:`SeriesContext`, whose properties are Spark columns
describing the current sample of one of the rule's series: its times and value,
its neighbours, the run (e.g. continuous block) it belongs to, and facts about
the whole series.  Columns returned by a hook become part of the shared pass
over the channel data.


#### ELEMENT\_TYPE

Element type of the arrays returned by :meth:`PushdownRule.emit`.


#### BLOCK

Name of the built-in run of continuous samples.


#### nan\_to\_null

```python
def nan_to_null(col: Column) -> Column
```

*col* with NaN as null, as ``numpy.nanmin`` / ``nanmax`` skip it and Arrow encodes it.


#### converted

```python
def converted(value: Column, factor: Column, applies: Column) -> Column
```

*value* times the unit-conversion *factor* where *applies*, as the UDF's cache converts.

Like ``TimeSeriesCache.to_sample_series``: a null or NaN factor leaves the value as it is.


#### element

```python
def element(tstart: Column | None = None,
            tend: Column | None = None,
            value: Column | None = None) -> Column
```

One emitted row; ``tstart`` / ``tend`` left out keep the row's own times.


#### run\_col

```python
def run_col(run: str, name: str) -> str
```

Name of the pipeline column *name* of the run *run*.


## Sample

```python
class Sample()
```

One sample as columns: ``tstart`` and ``tend`` as double, and its value as double.


#### RunBreak

A run definition: whether the sample *cur* starts a new run after *prev*.  It

is called with ``prev`` columns that are null before the series' first
sample; a null result counts as a break.


## Run

```python
class Run()
```

Where the current sample sits in its run, a maximal stretch without a break.

**Arguments**:

- `starts` (`Column`): The sample starts its run.
- `id` (`Column`): Number of the run within the series, from 1.
- `start` (`Column`): ``tstart`` of the run's first sample.
- `next_starts` (`Column`): The next sample starts a new run; null for the series' last sample.
- `is_last` (`Column`): The run is the last of its series.

#### block\_break

```python
def block_break(prev: Sample, cur: Sample) -> Column
```

Continuous blocks: a sample continues its block when it starts where the previous ended.


## SeriesContext

```python
class SeriesContext()
```

Columns a rule's hooks build on, for the current sample of one of its series.

Every series is one channel's samples of one variant, ordered by
``(tstart, tend, value)``; all variants of a channel have the same samples.
Rules running in series order (``ordered``) see neighbours, runs and series
facts; per-sample rules see only the sample.

**Arguments**:

- `rule` (`PushdownRule`): The rule the hooks belong to.
- `variants` (`Mapping[int, Pushdown]`): The rule's variants in this solve.
- `key` (`Sequence[str]`): Columns identifying a series.
- `order` (`Sequence[str]`): The channel-data ``tstart``, ``tend`` and value columns: the series order.
- `ordered` (`bool`): Whether the pipeline runs the rule in series order.
- `runs` (`Mapping[str, str]`): The rule's run names to pipeline run names.
- `kept` (`bool`): Whether the hook runs on the kept samples (``kept_columns``, ``emit``).
- `has_factor` (`bool`): Whether the rows carry the channel's conversion factor (:data:`FACTOR_COL`).

#### kept\_stage

```python
def kept_stage() -> SeriesContext
```

This context, for the hooks that run on the kept samples.


#### restricted

```python
def restricted(variant_ids: Iterable[int]) -> SeriesContext
```

This context for a subset of its variants, with the same column names.

Lets a rule compute columns for some of its variants only (e.g. those
absorbing another pushdown).


#### scoped

```python
def scoped(scope: str, rule: PushdownRule,
           variants: Mapping[int, Pushdown]) -> SeriesContext
```

A context for *rule*'s hooks on some of this rule's variants, in its own namespace.

A rule absorbing a pushed operand (:attr:`~.pushdown.Pushdown.absorbed`) runs
the operand's rule's hooks through it: *variants* maps each of its variant
ids to the absorbed pushdown, so :meth:`param` and :meth:`per_variant` read
that pushdown.  Column names live under *scope* within this rule's names, so
they cannot collide with the rule's own as long as those don't start with
``"<scope>_"``.  Return the scoped hooks' columns through :meth:`columns`.

**Raises**:

- `ValueError`: If *rule* defines runs of its own (:meth:`PushdownRule.runs`): the
pipeline only computes the runs of the rules it was given.

#### columns

```python
def columns(columns: Mapping[str, Column]) -> dict[str, Column]
```

The columns of a scoped context's hooks, named for the enclosing rule's hook.

Each column is defined for the scope's variants only: the absorbed rule's
columns may fail for variants without its parameters.


#### variants

```python
def variants() -> Mapping[int, Pushdown]
```

The variants the hooks compute, by variant id.


#### active

```python
def active() -> Column
```

The sample belongs to one of the rule's variants.


#### name

```python
def name(column: str) -> str
```

Pipeline name of the rule's column *column*.


#### col

```python
def col(column: str) -> Column
```

The rule's column *column*, defined by an earlier hook entry.


#### param

```python
def param(name: str) -> Column
```

The pushdown parameter *name* of the sample's variant.


#### per\_variant

```python
def per_variant(column: Callable[[Pushdown], Column]) -> Column
```

``column(pushdown)`` for the sample's variant: a CASE over the rule's variants.


#### tstart

```python
def tstart() -> Column
```

The sample's ``tstart`` as double.


#### tend

```python
def tend() -> Column
```

The sample's ``tend`` as double.


#### value

```python
def value() -> Column
```

The sample's value as double.


#### converted\_value

```python
def converted_value() -> Column
```

The sample's value as the UDF's cache loads it for the variant's selector.

Multiplied by the channel's conversion factor for variants whose rule


#### sample

```python
def sample() -> Sample
```

The current sample.


#### prev

```python
def prev() -> Sample
```

The previous sample of the series; null columns for its first sample.


#### next

```python
def next() -> Sample
```

The next sample of the series; null columns for its last sample.


#### n

```python
def n() -> Column
```

Number of samples in the series.


#### last

```python
def last() -> Sample
```

The series' last sample.


#### first\_in\_chunk

```python
def first_in_chunk() -> Column
```

The sample is the first of its chunk.


#### in\_chunk

```python
def in_chunk() -> WindowSpec
```

Window over the sample's chunk in series order, for local neighbour access.

Chunks are consecutive slices of a channel, the same for all its series;
use :attr:`prev`, :attr:`next` and runs for anything that must see across
chunk boundaries.


#### block

```python
def block() -> Run
```

The continuous block the sample belongs to (gaps end blocks).


#### run

```python
def run(name: str) -> Run
```

The sample's run of the rule's run definition *name* (see :meth:`PushdownRule.runs`).


#### block\_end

```python
def block_end() -> Column
```

``nanmax`` of the ``tend`` values of the sample's block (kept samples only).


#### kept\_in\_chunk\_reversed

```python
def kept_in_chunk_reversed() -> WindowSpec
```

Window over the kept samples of the sample's chunk in reverse series order.

For ``lag`` / ``lead`` (``lead`` is the previous kept sample) and, with a
running frame, for looking forward.  It is the order the pipeline already
sorts the kept samples in, so it adds no sort; the ascending


#### reverse\_chunk\_window

```python
def reverse_chunk_window(key: Sequence[str],
                         order: Sequence[str]) -> WindowSpec
```

Window over a series' chunk in reverse series order, without a frame.


## RowsContext

```python
class RowsContext()
```

Columns a rule's finishing hooks build on, for one derived row of one of its series.

The finishing pass (:func:`~.pipeline.finish_rows`) sees all derived rows of a
series (one channel's rows of one variant in a container), ordered like the
UDF's cache orders them: by ``tstart``, then ``tend``, NaN and null last.

**Arguments**:

- `rule` (`PushdownRule`): The rule the hooks belong to.
- `variants` (`Mapping[int, Pushdown]`): The rule's variants in this solve.
- `series` (`WindowSpec`): Window over the row's series in order.

#### active

```python
def active() -> Column
```

The row belongs to one of the rule's variants.


#### name

```python
def name(column: str) -> str
```

Pipeline name of the rule's finishing column *column*.


#### col

```python
def col(column: str) -> Column
```

The rule's finishing column *column*, defined by an earlier hook entry.


#### per\_variant

```python
def per_variant(column: Callable[[Pushdown], Column]) -> Column
```

``column(pushdown)`` for the row's variant: a CASE over the rule's variants.


#### tstart

```python
def tstart() -> Column
```

The row's ``tstart`` as double.


#### tend

```python
def tend() -> Column
```

The row's ``tend`` as double.


#### value

```python
def value() -> Column
```

The row's value as double.


#### prev

```python
def prev() -> Sample
```

The previous row of the series; null columns for its first row.


#### next

```python
def next() -> Sample
```

The next row of the series; null columns for its last row.


#### is\_first

```python
def is_first() -> Column
```

The row is the first of its series.


#### is\_last

```python
def is_last() -> Column
```

The row is the last of its series.


#### running

```python
def running() -> WindowSpec
```

Window from the series' first row up to the current row.


#### series

```python
def series() -> WindowSpec
```

Window over the row's series in order, for ``lag`` / ``lead``.


