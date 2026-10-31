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

#### kept\_stage

```python
def kept_stage() -> SeriesContext
```

This context, for the hooks that run on the kept samples.


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


#### sample

```python
def sample() -> Sample
```

The current sample.


#### element

```python
def element(tstart: Column | None = None,
            tend: Column | None = None,
            value: Column | None = None) -> Column
```

One emitted row; ``tstart`` / ``tend`` left out keep the sample's own times.


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


