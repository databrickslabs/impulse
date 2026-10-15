"""Pushdown rule computing ``SampleSeries.resample`` natively in Spark."""

from __future__ import annotations

import math
import numbers
from collections.abc import Mapping
from typing import Any

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column

from impulse_query_engine.analyze.metadata.time_series_expression import (
    SeriesType,
    TimeSeriesExpression,
    TimeSeriesOp,
    TimeSeriesSelector,
)

from .context import SeriesContext
from .pushdown import Pushdown, PushdownContext, PushdownRule

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
    ``c, c + 1, …`` up to the next sample's bucket, so only those samples are kept
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

    def row_columns(self, ctx: SeriesContext) -> Mapping[str, Column]:
        grid = _Grid(ctx)
        resampled = ctx.col("resampled")
        return {
            # A single sample is returned as it is.
            "resampled": ctx.n >= 2,
            **grid.first_index("c", ctx.tstart, F.lit(False), resampled),
            **grid.first_index(
                "next_c", ctx.next.tstart, F.lit(False), resampled & _next_in_block(ctx)
            ),
        }

    def keep(self, ctx: SeriesContext) -> Column:
        # The last sample of each bucket.
        return ~ctx.col("resampled") | ~_next_in_block(ctx) | (ctx.col("next_c") != ctx.col("c"))

    def kept_columns(self, ctx: SeriesContext) -> Mapping[str, Column]:
        grid = _Grid(ctx)
        t_max = ctx.block_end
        # The last block keeps a grid point at t_max when the series ends with a
        # zero-duration sample.
        inclusive = ctx.block.is_last & (ctx.last.tstart == ctx.last.tend)
        length = F.greatest(F.ceil(((t_max + grid.rate) - grid.t_min) / grid.rate), F.lit(0))
        n_grid = ctx.col("n_grid")
        return {
            **grid.first_index("grid_end", t_max, inclusive, ctx.col("resampled")),
            "n_grid": F.least(ctx.col("grid_end"), length),
            # Grid points up to the next sample's bucket, or up to the block's end.
            "c_stop": F.when(_next_in_block(ctx), F.least(ctx.col("next_c"), n_grid)).otherwise(
                n_grid
            ),
        }

    def emit(self, ctx: SeriesContext) -> Column:
        grid = _Grid(ctx)
        c, c_stop, n_grid = ctx.col("c"), ctx.col("c_stop"), ctx.col("n_grid")
        points = F.transform(
            F.sequence(c, c_stop - 1),
            lambda k: ctx.element(
                grid.point(k),
                F.when(k == n_grid - 1, ctx.block_end).otherwise(grid.point(k + 1)),
                ctx.value,
            ),
        )
        return F.when(~ctx.col("resampled"), F.array(ctx.element(value=ctx.value))).when(
            c_stop > c, points
        )


def _next_in_block(ctx: SeriesContext) -> Column:
    """The next sample exists and continues the block."""
    return F.coalesce(~ctx.block.next_starts, F.lit(False))


class _Grid:
    """The numpy.arange grid of the sample's block.

    ``numpy.arange(t_min, t_max + rate, rate)`` has length
    ``ceil(((t_max + rate) - t_min) / rate)`` and is filled as ``t_min``,
    ``t_min + rate``, then ``t_min + k * ((t_min + rate) - t_min)``.
    """

    def __init__(self, ctx: SeriesContext):
        self.ctx = ctx
        self.rate = ctx.param(_RATE)
        self.t_min = ctx.block.start
        self.delta = (self.t_min + self.rate) - self.t_min

    def point(self, k: Column) -> Column:
        return (
            F.when(k == 0, self.t_min)
            .when(k == 1, self.t_min + self.rate)
            .otherwise(self.t_min + k.cast("double") * self.delta)
        )

    def first_index(
        self, name: str, x: Column, strict: Column, active: Column
    ) -> dict[str, Column]:
        """Columns ending in *name*: the smallest ``k >= 0`` with ``point(k) >= x`` (``> x`` if *strict*).

        Estimated from ``(x - t_min) / delta`` and corrected by one step against the
        exact grid points; a result that still is not exact raises instead of
        misplacing a sample.  Only evaluated where *active*.
        """

        def reached(k: Column) -> Column:
            g = self.point(k)
            return F.when(strict, g > x).otherwise(g >= x)

        estimate, step = self.ctx.col(f"{name}_estimate"), self.ctx.col(f"{name}_step")
        return {
            f"{name}_estimate": F.when(
                active,
                F.when(
                    self.delta > 0,
                    F.greatest(F.ceil((x - self.t_min) / self.delta), F.lit(0)),
                ).otherwise(
                    F.raise_error(
                        F.lit("resample: sample_rate is below the resolution of the timestamps")
                    )
                ),
            ),
            f"{name}_step": F.when((estimate > 0) & reached(estimate - 1), estimate - 1)
            .when(~reached(estimate), estimate + 1)
            .otherwise(estimate),
            name: F.when(~active, F.lit(None))
            .when(reached(step) & ((step == 0) | ~reached(step - 1)), step)
            .otherwise(F.raise_error(F.lit("resample: grid index could not be determined"))),
        }


def _sample_rate(args: tuple, kwargs: dict[str, Any]) -> float | None:
    """The resample rate of a ``resample(...)`` call, or ``None`` if unsupported."""
    if len(args) > 1 or set(kwargs) - {_RATE} or (args and kwargs):
        return None
    rate = args[0] if args else kwargs.get(_RATE, _DEFAULT_RATE)
    if isinstance(rate, bool) or not isinstance(rate, numbers.Real):
        return None
    rate = float(rate)
    return rate if math.isfinite(rate) and rate > 0 else None
