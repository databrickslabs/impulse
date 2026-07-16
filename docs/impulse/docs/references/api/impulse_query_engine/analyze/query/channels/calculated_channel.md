---
sidebar_label: calculated_channel
title: impulse_query_engine.analyze.query.channels.calculated_channel
---

CalculatedChannel: a labeled derived time-series channel.


## CalculatedChannel

```python
class CalculatedChannel(TimeSeriesExpression)
```

A derived channel: a wrapped time-series expression plus output identity.

A ``CalculatedChannel`` wraps an arbitrary :class:`TimeSeriesExpression`
built from the operator DSL (e.g. ``q.channel(channel_name="raw_speed") * 3.6``
or ``rpm + speed``).  Like the other ``TimeSeriesExpression`` leaves/nodes
(``TimeSeriesSelector``, ``TimeSeriesOp``) it ``build()``s to a
:class:`SampleSeries` — it is a *labeled derived signal*, not a reduction, so
it is a plain ``TimeSeriesExpression`` rather than an ``Aggregation``.
:meth:`QueryBuilder.solve_calculated_channels` explodes that series into
narrow rows matching the silver ``channel_data`` shape
(``container_id, channel_id, tstart, tend, value``) plus one string column
per identity entry.

**Arguments**:

- `expr` (`TimeSeriesExpression`): The wrapped expression; must ``build()`` to a ``SampleSeries``.
- `identity` (`dict of str`): Identity columns for the output rows, e.g.
``{"channel_name": "Eng_RPM", "data_key": "TM"}``.  Each key becomes a
``StringType`` output column and each value is emitted as a literal on
every row of this channel.  Must be non-empty; the identity also seeds
the deterministic ``channel_id`` hash.
- `channel_id` (`int or None`): Output ``channel_id`` for every emitted row.  When omitted (the
``_AUTO`` sentinel) a deterministic id is derived from the identity by
the solver, typed to match the source ``channel_id`` column.  Pass an
``int`` to use it verbatim, or ``None`` to emit a SQL null.

#### canonical\_identity

```python
def canonical_identity() -> str
```

Return a stable string encoding of the identity, used for the id hash.

Keys are sorted so the encoding (and the derived ``channel_id``) is
independent of kwarg order.


#### build

```python
def build(cache: SeriesCache)
```

Evaluate the wrapped expression against the cache (yields a SampleSeries).


#### dtype

```python
def dtype() -> T.DataType
```

Spark type of ``build()``'s result: a serialized ``SampleSeries``.

Matches :meth:`TimeSeriesSelector.dtype` (``BinaryType``), since the
wrapped expression evaluates to a ``SampleSeries``.  The narrow
calculated-channels solve path does not consume this — it emits its own
``container_id, channel_id, tstart, tend, value`` schema — but keeping it
honest makes the expression safe if ever routed through ``solve()``.


