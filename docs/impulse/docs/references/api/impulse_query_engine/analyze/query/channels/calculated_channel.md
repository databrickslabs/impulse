---
sidebar_label: calculated_channel
title: impulse_query_engine.analyze.query.channels.calculated_channel
---

CalculatedChannel: a labeled derived time-series channel.


## CalculatedChannel

```python
class CalculatedChannel(TimeSeriesExpression)
```

A derived channel: a wrapped ``TimeSeriesExpression`` plus output identity.

Wraps an expression built from the operator DSL (e.g. ``rpm * 3.6``) that must
evaluate to a :class:`SampleSeries`.  ``build()`` returns that series' raw
``(tstarts, tends, values)`` arrays, which
:meth:`QueryBuilder.solve_calculated_channels` explodes into narrow
silver-shaped rows plus an ``identity`` ``MapType`` column.

**Arguments**:

- `expr` (`TimeSeriesExpression`): The wrapped expression; must ``build()`` to a ``SampleSeries``.
- `identity` (`dict of str`): Non-empty identity dict (arbitrary keys, e.g.
``{"channel_name": "Eng_RPM", "data_key": "TM"}``), emitted per row and
used to seed the deterministic :attr:`channel_id`.

#### canonical\_identity

```python
def canonical_identity() -> str
```

Return a stable string encoding of the identity, used for the id hash.

Keys are sorted so the encoding (and the derived ``channel_id``) is
independent of kwarg order.


#### channel\_id

```python
def channel_id() -> int
```

Deterministic output ``channel_id`` derived from the identity.

A CRC32 of :meth:`canonical_identity` masked to a positive int32, so the
value is stable across runs/processes and fits both ``IntegerType`` and
``LongType`` source ``channel_id`` columns.  Determinism makes writes
idempotent and joins predictable; sharing this one derivation across the
query-engine and reporting layers keeps their ids in lockstep.


#### build

```python
def build(cache: SeriesCache)
```

Evaluate the wrapped expression and return its raw ``(tstarts, tends, values)``.

Unlike a typical ``TimeSeriesExpression`` (whose ``build`` yields a
``SampleSeries``), this returns the three parallel ``float64`` arrays
underlying that series, since the calculated-channels solve path consumes
the raw samples directly.


#### dtype

```python
def dtype() -> T.DataType
```

Spark type of ``build()``'s output: the raw ``(tstarts, tends, values)`` arrays.

A struct of three arrays mirroring the tuple ``build()`` returns, with
element types matching the narrow calculated-channels output
(``tstart``/``tend`` are ``long``, ``value`` is ``double``).


