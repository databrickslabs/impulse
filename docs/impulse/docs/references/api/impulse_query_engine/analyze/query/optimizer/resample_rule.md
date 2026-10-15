---
sidebar_label: resample_rule
title: impulse_query_engine.analyze.query.optimizer.resample_rule
---

Pushdown rule computing ``SampleSeries.resample`` natively in Spark.


## ResamplePushdownRule

```python
class ResamplePushdownRule(PushdownRule)
```

Pushes ``channel.resample(sample_rate)`` on a physical channel down to Spark.

Reproduces :meth:`SampleSeries.resample`: per continuous block of samples, a
regular grid starting at the block's first ``tstart`` (built like
``numpy.arange``), each grid point holding the value of the last sample that
starts at or before it.  Meant for downsampling: a rate finer than the native
sample spacing yields more rows than the raw channel.

Each sample is assigned the bucket ``c`` of the first grid point at or after its
``tstart``.  The last sample of a bucket holds the value of grid points
``c, c + 1, …`` up to the next non-empty bucket, so only those samples emit rows
and every row carries just the grid points it covers.  Values are looked up
within a block; this matches the pandas evaluation unless intervals overlap.

Every series is cut into equal time slices (chunks) holding about *chunk_rows*
samples on average, and the windows over samples run per chunk, so a worker
holds one chunk of a series at a time instead of the whole series.  Gaps at
chunk boundaries, the block a chunk starts in and that block's start are
resolved from one summary row per chunk.

Grid points are computed as ``t_min + k * delta`` in IEEE double arithmetic.
numpy builds that fuse this into a multiply-add can differ by one ulp for
fractional grids (e.g. seconds-as-double with a rate of ``0.1``); integral
grids are exact.

**Arguments**:

- `chunk_rows` (`int`): Average number of samples per chunk (default 100 000).

