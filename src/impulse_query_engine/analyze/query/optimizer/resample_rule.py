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

    Grid points are computed as ``t_min + k * delta`` in IEEE double arithmetic.
    numpy builds that fuse this into a multiply-add can differ by one ulp for
    fractional grids (e.g. seconds-as-double with a rate of ``0.1``); integral
    grids are exact.
    """

    name = "resample"

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

        # Every window shares one partitioning and order (the pandas cache's tstart
        # order), so the rows are sorted once and the container partitioning is kept.
        ordered = Window.partitionBy(*key).orderBy(ts, te)
        running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        following = ordered.rowsBetween(Window.currentRow, Window.unboundedFollowing)
        whole = ordered.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        ts_d, te_d, block = F.col("_rs_ts"), F.col("_rs_te"), F.col("_rs_block")
        prev_te = F.lag(te_d).over(ordered)
        df = (
            rows.withColumn("_rs_ts", F.col(ts).cast("double"))
            .withColumn("_rs_te", F.col(te).cast("double"))
            .withColumn("_rs_break", (prev_te.isNull() | ((ts_d - prev_te) != 0)).cast("long"))
            .withColumn("_rs_n", F.count(F.lit(1)).over(whole))
            .withColumn("_rs_last_ts", F.last(ts_d).over(whole))
            .withColumn("_rs_last_te", F.last(te_d).over(whole))
            .withColumn("_rs_block", F.sum("_rs_break").over(running))
        )
        # Block bounds on every row: the block's first tstart and its largest tend.
        # Blocks are numbered in order, so max(struct(±block, tend)) over the rows
        # before / after the current one stays within the current block.
        te_no_nan = _ignore_nan(te_d).alias("te")
        df = (
            df.withColumn(
                "_rs_t_min",
                F.last(F.when(F.col("_rs_break") == 1, ts_d), ignorenulls=True).over(running),
            )
            .withColumn(
                "_rs_t_max",
                F.greatest(
                    F.max(F.struct(block, te_no_nan)).over(running).getField("te"),
                    F.max(F.struct(-block, te_no_nan)).over(following).getField("te"),
                ),
            )
            .withColumn("_rs_nblk", F.max(block).over(whole))
        )

        # numpy.arange(t_min, t_max + rate, rate): length ceil(((t_max + rate) - t_min) / rate),
        # filled as t_min, t_min + rate, then t_min + k * ((t_min + rate) - t_min).
        t_min, t_max = F.col("_rs_t_min"), F.col("_rs_t_max")
        delta = (t_min + rate) - t_min

        def grid_point(k: Column) -> Column:
            return (
                F.when(k == 0, t_min)
                .when(k == 1, t_min + rate)
                .otherwise(t_min + k.cast("double") * delta)
            )

        resampled = F.col(VARIANT_COL).isin(sorted(variants)) & (F.col("_rs_n") >= 2)
        # The last block keeps a grid point at t_max when the series ends with a
        # zero-duration sample.
        inclusive = (block == F.col("_rs_nblk")) & (F.col("_rs_last_ts") == F.col("_rs_last_te"))
        length = F.greatest(F.ceil(((t_max + rate) - t_min) / rate), F.lit(0))

        # Bucket of each sample, and the number of grid points of its block.
        df = _with_first_grid_index(
            df, "_rs_c", ts_d, F.lit(False), resampled, grid_point, t_min, delta
        )
        df = _with_first_grid_index(
            df, "_rs_grid_end", t_max, inclusive, resampled, grid_point, t_min, delta
        )
        c, n_grid = F.col("_rs_c"), F.least(F.col("_rs_grid_end"), length)

        # The last sample of a bucket emits the grid points up to the next non-empty
        # bucket of its block (or up to the block's end).
        next_c, next_block = F.lead(c).over(ordered), F.lead(block).over(ordered)
        df = df.withColumn(
            "_rs_c_stop",
            F.when(next_block.isNull() | (next_block != block), n_grid).when(
                next_c != c, F.least(next_c, n_grid)
            ),
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
