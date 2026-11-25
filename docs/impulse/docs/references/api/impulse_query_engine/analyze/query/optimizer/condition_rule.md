---
sidebar_label: condition_rule
title: impulse_query_engine.analyze.query.optimizer.condition_rule
---

Pushdown rule computing comparisons of a channel with a scalar natively in Spark.


## ConditionPushdownRule

```python
class ConditionPushdownRule(PushdownRule)
```

Pushes ``channel op scalar`` (``>``, ``>=``, ``<``, ``<=``, ``==``, ``!=``) down to Spark.

The channel is a continuous physical or aliased selection; the scalar may come
first (``500 < rpm``).  The result is what pandas computes,
``Intervals(tstarts[holds], tends[holds], merge_overlaps=True)``: the samples
where the comparison holds, merged where one starts at or before the previous
one's ``tend``.  Comparisons follow numpy: NaN and null values satisfy only
``!=``.  Aliased selections compare unit-converted values.

Spark computes the merged intervals in two passes:

1. **Pieces**, in the ordered pass, chunk by chunk: runs of consecutive selected
   samples that pandas merges for certain.  A sample only joins the previous one
   of its chunk if both are selected, it has a positive duration and starts at or
   before the previous ``tend``, and the previous ``tstart`` is not null (null
   times sort first in Spark but last in the UDF's cache).  A piece is shipped as
   ``(tstart of its first sample, tend of its last sample)``.
2. **Merge**, in the finishing pass over the pieces of a series: pandas merges
   consecutive samples pairwise, so merging consecutive pieces makes exactly the
   comparisons pandas makes between their samples, including its rules for a
   trailing zero-duration interval and for dropping empty intervals.

Exact unless two samples of different pieces share a ``tstart``: the UDF's
cache orders those arbitrarily, and the pandas result depends on the order.


