"""Pushdown rule computing ``SampleSeries.resample`` natively in Spark."""

from __future__ import annotations

import math
import numbers
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame, Window

from impulse_query_engine.analyze.metadata.time_series_expression import (
    SeriesType,
    TimeSeriesExpression,
    TimeSeriesOp,
    TimeSeriesSelector,
)

from .pushdown import VARIANT_COL, Pushdown, PushdownContext, PushdownRule

if TYPE_CHECKING:
    from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig

_RATE = "sample_rate"
_DEFAULT_RATE = 1.0
_TIME_TYPES = (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.DoubleType)


class ResamplePushdownRule(PushdownRule):
    """Pushes ``channel.resample(sample_rate)`` on a physical channel down to Spark.

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

    Parameters
    ----------
    chunk_rows : int, optional
        Average number of samples per chunk (default 100 000).
    """

    name = "resample"

    def __init__(self, chunk_rows: int = 100_000):
        if isinstance(chunk_rows, bool) or not isinstance(chunk_rows, numbers.Integral):
            raise TypeError(f"chunk_rows must be an integer, got {chunk_rows!r}")
        if chunk_rows < 1:
            raise ValueError(f"chunk_rows must be positive, got {chunk_rows}")
        self.chunk_rows = int(chunk_rows)

    def match(self, node: TimeSeriesExpression) -> Pushdown | None:
        if (
            type(node) is not TimeSeriesOp
            or node.optype != "cls"
            or not isinstance(node.operation, str)
            or node.operation != "resample"
        ):
            return None
        if not node.args or not isinstance(node.args[0], TimeSeriesSelector):
            return None
        selector = node.args[0]
        if selector.series_type is not SeriesType.CONTINUOUS:
            return None
        rate = _sample_rate(node.args[1:], node.kwargs)
        if rate is None:
            return None
        return Pushdown(
            rule=self, selector=selector, params=((_RATE, rate),), consumed=(selector,)
        )

    def supports(self, pushdown: Pushdown, context: PushdownContext) -> bool:
        time_types = (context.tstart_type, context.tend_type)
        if not all(isinstance(t, _TIME_TYPES) for t in time_types):
            return False
        # Integral time columns receive the grid cast back from double; an integral
        # rate keeps every grid point integral, so that cast is exact.
        rate = dict(pushdown.params)[_RATE]
        return rate.is_integer() or not any(isinstance(t, T.IntegralType) for t in time_types)

    def to_spark(
        self, rows: DataFrame, variants: Mapping[int, Pushdown], config: SolverConfig
    ) -> DataFrame:
        cid, ch = config.container_id_col, config.channel_id_col
        ts, te = config.tstart_col, config.tend_col
        key = [cid, ch, VARIANT_COL]
        rate = F.try_element_at(
            F.create_map(
                *[
                    item
                    for vid, pushdown in sorted(variants.items())
                    for item in (F.lit(vid), F.lit(dict(pushdown.params)[_RATE]))
                ]
            ),
            F.col(VARIANT_COL),
        )
        ts_d, te_d = F.col(ts).cast("double"), F.col(te).cast("double")
        # The pandas cache's sample order.
        order = F.struct(ts, te)

        # Series boundaries.  This summary and the chunk summary below read only the
        # key and time columns, so Spark serves both from one extra narrow read.
        series = rows.groupBy(*key).agg(
            F.min(ts_d).alias("_rs_first"),
            F.max(_ignore_nan(te_d)).alias("_rs_end"),
            F.count(F.lit(1)).alias("_rs_n"),
        )
        df = rows.join(F.broadcast(series), on=key)

        # Equal time slices of about chunk_rows samples; monotone in tstart, so the
        # chunks of a series follow its sample order.
        first, end = F.col("_rs_first"), F.col("_rs_end")
        n_chunks = F.ceil(F.col("_rs_n") / F.lit(self.chunk_rows))
        df = df.withColumn(
            "_rs_chunk",
            F.when(
                end > first,
                F.least(F.floor((ts_d - first) / (end - first) * n_chunks), n_chunks - 1),
            ).otherwise(F.lit(0).cast("long")),
        )

        # Pass 1, per chunk: only running and neighbour windows (linear in the chunk
        # size), sharing one sort and the container partitioning.  A chunk splits into
        # segments at its gaps; segment 0 continues whatever block the previous chunk
        # ended in, which the chunk summary resolves.
        in_chunk = Window.partitionBy(*key, "_rs_chunk").orderBy(ts, te)
        running = in_chunk.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        prev_te = F.lag(te_d).over(in_chunk)
        is_break = F.col("_rs_break") == 1
        df = (
            df.withColumn(
                "_rs_break",
                F.when(F.row_number().over(in_chunk) == 1, F.lit(0).cast("long")).otherwise(
                    (prev_te.isNull() | ((ts_d - prev_te) != 0)).cast("long")
                ),
            )
            .withColumn("_rs_seg", F.sum("_rs_break").over(running))
            .withColumn(
                "_rs_seg_t_min", F.last(F.when(is_break, ts_d), ignorenulls=True).over(running)
            )
            # Largest tend so far within the segment (segments are numbered in order).
            .withColumn(
                "_rs_seg_t_max",
                F.max(F.struct(F.col("_rs_seg"), _ignore_nan(te_d).alias("te")))
                .over(running)
                .getField("te"),
            )
            .withColumn("_rs_next_ts", F.lead(ts_d).over(in_chunk))
            .withColumn("_rs_next_break", F.lead("_rs_break").over(in_chunk))
        )

        # One summary row per chunk, scanned in chunk order: whether the chunk starts
        # a block, the block of its segment 0 and that block's start, plus the number
        # of blocks and the last sample of the series.
        chunks = df.groupBy(*key, "_rs_chunk").agg(
            F.min_by(ts_d, order).alias("_rs_head_ts"),
            F.max_by(F.struct(ts_d.alias("ts"), te_d.alias("te")), order).alias("_rs_tail"),
            F.sum("_rs_break").alias("_rs_inner_breaks"),
            F.max(F.when(is_break, ts_d)).alias("_rs_last_start"),
        )
        by_chunk = Window.partitionBy(*key).orderBy("_rs_chunk")
        all_chunks = by_chunk.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        head_ts, head_break = F.col("_rs_head_ts"), F.col("_rs_head_break") == 1
        prev_tail_te = F.lag(F.col("_rs_tail.te")).over(by_chunk)
        last_start = F.coalesce(F.col("_rs_last_start"), F.when(head_break, head_ts))
        chunks = (
            chunks.withColumn(
                "_rs_head_break",
                (prev_tail_te.isNull() | ((head_ts - prev_tail_te) != 0)).cast("long"),
            )
            .withColumn("_rs_breaks", F.col("_rs_head_break") + F.col("_rs_inner_breaks"))
            .withColumn(
                "_rs_block_base",
                F.sum("_rs_breaks").over(
                    by_chunk.rowsBetween(Window.unboundedPreceding, Window.currentRow)
                )
                - F.col("_rs_inner_breaks"),
            )
            .withColumn(
                "_rs_head_t_min",
                F.when(head_break, head_ts).otherwise(
                    F.last(last_start, ignorenulls=True).over(
                        by_chunk.rowsBetween(Window.unboundedPreceding, -1)
                    )
                ),
            )
            .withColumn("_rs_nblk", F.sum("_rs_breaks").over(all_chunks))
            .withColumn("_rs_last", F.last("_rs_tail").over(all_chunks))
            .select(*key, "_rs_chunk", "_rs_block_base", "_rs_head_t_min", "_rs_nblk", "_rs_last")
        )
        df = (
            df.join(F.broadcast(chunks), on=[*key, "_rs_chunk"])
            .withColumn("_rs_block", F.col("_rs_block_base") + F.col("_rs_seg"))
            .withColumn("_rs_t_min", F.coalesce(F.col("_rs_seg_t_min"), F.col("_rs_head_t_min")))
        )

        # numpy.arange(t_min, t_max + rate, rate): length ceil(((t_max + rate) - t_min) / rate),
        # filled as t_min, t_min + rate, then t_min + k * ((t_min + rate) - t_min).
        t_min = F.col("_rs_t_min")
        delta = (t_min + rate) - t_min

        def grid_point(k: Column) -> Column:
            return (
                F.when(k == 0, t_min)
                .when(k == 1, t_min + rate)
                .otherwise(t_min + k.cast("double") * delta)
            )

        resampled = F.col(VARIANT_COL).isin(sorted(variants)) & (F.col("_rs_n") >= 2)
        next_in_block = F.coalesce(F.col("_rs_next_break") == 0, F.lit(False))
        df = _with_first_grid_index(
            df, "_rs_c", ts_d, F.lit(False), resampled, grid_point, t_min, delta
        )
        df = _with_first_grid_index(
            df,
            "_rs_next_sample_c",
            F.col("_rs_next_ts"),
            F.lit(False),
            resampled & next_in_block,
            grid_point,
            t_min,
            delta,
        )
        c = F.col("_rs_c")
        # Only the last sample of each bucket emits grid points.  A chunk's last sample
        # is kept too: its bucket may continue in the next chunk.
        df = df.where(~resampled | ~next_in_block | (F.col("_rs_next_sample_c") != c))

        # Pass 2, over the remaining rows of each block (about one per grid point): the
        # next non-empty bucket, and the block's t_max (each segment's last sample
        # carries its maximum).  A chunk's last sample whose bucket continues in the
        # next chunk is followed by its own bucket and emits nothing.  Rows this rule
        # leaves alone stay grouped by chunk.
        in_block = Window.partitionBy(
            *key, "_rs_block", F.when(~resampled, F.col("_rs_chunk"))
        ).orderBy("_rs_c", ts, te)
        whole_block = in_block.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        df = df.withColumn("_rs_next_c", F.lead("_rs_c").over(in_block)).withColumn(
            "_rs_t_max", F.max("_rs_seg_t_max").over(whole_block)
        )
        next_c, t_max = F.col("_rs_next_c"), F.col("_rs_t_max")
        block_end = next_c.isNull()
        # The last block keeps a grid point at t_max when the series ends with a
        # zero-duration sample.
        inclusive = (F.col("_rs_block") == F.col("_rs_nblk")) & (
            F.col("_rs_last.ts") == F.col("_rs_last.te")
        )
        length = F.greatest(F.ceil(((t_max + rate) - t_min) / rate), F.lit(0))
        df = _with_first_grid_index(
            df, "_rs_grid_end", t_max, inclusive, resampled, grid_point, t_min, delta
        )
        n_grid = F.least(F.col("_rs_grid_end"), length)
        # A bucket's last sample emits the grid points up to the next non-empty bucket
        # of its block, or up to the block's end.
        df = df.withColumn(
            "_rs_c_stop", F.when(block_end, n_grid).otherwise(F.least(next_c, n_grid))
        )

        def element(kind: int, t: Column, tend: Column) -> Column:
            return F.struct(
                F.lit(kind).alias("kind"),
                t.cast("double").alias("t"),
                tend.cast("double").alias("tend"),
            )

        c_stop = F.col("_rs_c_stop")
        grid = F.transform(
            F.sequence(c, c_stop - 1),
            lambda k: element(
                1,
                grid_point(k),
                F.when(k == n_grid - 1, t_max).otherwise(grid_point(k + 1)),
            ),
        )
        no_time = F.lit(None)
        # Series this rule leaves alone keep their row (kind 2); of the resampled
        # series only the grid points survive (kind 1).  explode drops the rest.
        elements = F.when(~resampled, F.array(element(2, no_time, no_time))).when(c_stop > c, grid)
        df = df.withColumn("_rs_e", F.explode(elements))

        is_grid = F.col("_rs_e.kind") == 1
        derived = {
            ts: F.col("_rs_e.t").cast(rows.schema[ts].dataType),
            te: F.col("_rs_e.tend").cast(rows.schema[te].dataType),
        }
        return df.select(
            *[
                (
                    F.when(is_grid, derived[col]).otherwise(F.col(col)).alias(col)
                    if col in derived
                    else col
                )
                for col in rows.columns
            ]
        )


def _with_first_grid_index(
    df: DataFrame,
    name: str,
    x: Column,
    strict: Column,
    active: Column,
    grid_point: Callable[[Column], Column],
    t_min: Column,
    delta: Column,
) -> DataFrame:
    """Add *name*: the smallest ``k >= 0`` with ``grid_point(k) >= x`` (``> x`` if *strict*).

    Estimated from ``(x - t_min) / delta`` and corrected by one step against the
    exact grid points; a result that still is not exact raises instead of
    misplacing a sample.  Only evaluated where *active*.
    """

    def reached(k: Column) -> Column:
        g = grid_point(k)
        return F.when(strict, g > x).otherwise(g >= x)

    estimate, k = F.col(f"{name}_estimate"), F.col(name)
    return (
        df.withColumn(
            f"{name}_estimate",
            F.when(
                active,
                F.when(delta > 0, F.greatest(F.ceil((x - t_min) / delta), F.lit(0))).otherwise(
                    F.raise_error(
                        F.lit("resample: sample_rate is below the resolution of the timestamps")
                    )
                ),
            ),
        )
        .withColumn(
            name,
            F.when((estimate > 0) & reached(estimate - 1), estimate - 1)
            .when(~reached(estimate), estimate + 1)
            .otherwise(estimate),
        )
        .withColumn(
            name,
            F.when(~active, F.lit(None))
            .when(reached(k) & ((k == 0) | ~reached(k - 1)), k)
            .otherwise(F.raise_error(F.lit("resample: grid index could not be determined"))),
        )
    )


def _sample_rate(args: tuple, kwargs: dict[str, Any]) -> float | None:
    """The resample rate of a ``resample(...)`` call, or ``None`` if unsupported."""
    if len(args) > 1 or set(kwargs) - {_RATE} or (args and kwargs):
        return None
    rate = args[0] if args else kwargs.get(_RATE, _DEFAULT_RATE)
    if isinstance(rate, bool) or not isinstance(rate, numbers.Real):
        return None
    rate = float(rate)
    return rate if math.isfinite(rate) and rate > 0 else None


def _ignore_nan(col: Column) -> Column:
    """Map NaN to null so ``min`` / ``max`` behave like ``numpy.nanmin`` / ``nanmax``."""
    return F.when(~F.isnan(col), col)
