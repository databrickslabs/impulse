"""Pushdown rule computing ``SampleSeries.resample`` natively in Spark."""

from __future__ import annotations

import math
import numbers
from collections.abc import Mapping
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
        ts, te, val = config.tstart_col, config.tend_col, config.value_col
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

        # All per-channel state comes from windows sharing one partitioning and order
        # (the pandas cache's tstart order), so the rows are sorted once and the
        # incoming container partitioning is kept.
        ordered = Window.partitionBy(*key).orderBy(ts, te)
        running = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        whole = ordered.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        ts_d, te_d = F.col("_rs_ts"), F.col("_rs_te")
        prev_te = F.lag(te_d).over(ordered)
        df = (
            rows.withColumn("_rs_ts", F.col(ts).cast("double"))
            .withColumn("_rs_te", F.col(te).cast("double"))
            .withColumn("_rs_break", (prev_te.isNull() | ((ts_d - prev_te) != 0)).cast("long"))
            .withColumn("_rs_n", F.count(F.lit(1)).over(whole))
            .withColumn("_rs_last_ts", F.last(ts_d).over(whole))
            .withColumn("_rs_last_te", F.last(te_d).over(whole))
            .withColumn("_rs_block", F.sum("_rs_break").over(running))
            .withColumn(
                "_rs_block_end", F.coalesce(F.lead("_rs_break").over(ordered), F.lit(1)) == 1
            )
        )
        # Block bounds as seen on the block's last row: its first tstart, and the
        # largest tend so far within the block (blocks are numbered in order).
        df = (
            df.withColumn(
                "_rs_t_min",
                F.last(F.when(F.col("_rs_break") == 1, ts_d), ignorenulls=True).over(running),
            )
            .withColumn(
                "_rs_t_max",
                F.max(F.struct(F.col("_rs_block"), _ignore_nan(te_d).alias("te")))
                .over(running)
                .getField("te"),
            )
            .withColumn("_rs_nblk", F.max("_rs_block").over(whole))
        )

        # numpy.arange(t_min, t_max + rate, rate): length ceil(((t_max + rate) - t_min) / rate),
        # filled as t_min, t_min + rate, then t_min + k * ((t_min + rate) - t_min).
        t_min, t_max = F.col("_rs_t_min"), F.col("_rs_t_max")
        length = F.ceil(((t_max + rate) - t_min) / rate)
        delta = (t_min + rate) - t_min
        # The last block keeps a grid point at t_max when the series ends with a
        # zero-duration sample.
        inclusive = (F.col("_rs_block") == F.col("_rs_nblk")) & (
            F.col("_rs_last_ts") == F.col("_rs_last_te")
        )
        resampled = F.col(VARIANT_COL).isin(sorted(variants)) & (F.col("_rs_n") >= 2)
        grid_points = F.filter(
            F.transform(
                F.sequence(F.lit(0).cast("long"), length - 1),
                lambda k: F.when(k == 0, t_min)
                .when(k == 1, t_min + rate)
                .otherwise(t_min + k.cast("double") * delta),
            ),
            lambda g: (g < t_max) | (inclusive & (g == t_max)),
        )
        df = df.withColumn(
            "_rs_grid",
            F.when(resampled & F.col("_rs_block_end") & (length > 0), grid_points),
        )

        # One output element per row: the row itself for series this rule leaves
        # alone (kind 2), otherwise the sample (kind 0) plus, on a block's last row,
        # the block's grid points (kind 1).  Fewer than two samples: returned as-is.
        def element(kind: int, t: Column, tend: Column) -> Column:
            return F.struct(
                F.lit(kind).alias("kind"), t.alias("t"), tend.cast("double").alias("tend")
            )

        grid = F.transform(
            F.col("_rs_grid"),
            lambda g, i: element(
                1, g, F.coalesce(F.try_element_at(F.col("_rs_grid"), i + 2), t_max)
            ),
        )
        no_tend = F.lit(None).cast("double")
        elements = (
            F.when(~resampled, F.array(element(2, ts_d, no_tend)))
            .when(
                F.col("_rs_grid").isNotNull(), F.concat(F.array(element(0, ts_d, no_tend)), grid)
            )
            .otherwise(F.array(element(0, ts_d, no_tend)))
        )
        df = df.withColumn("_rs_e", F.explode(elements))
        kind, t = F.col("_rs_e.kind"), F.col("_rs_e.t")

        # Previous-value lookup: each grid point takes the value of the last sample (in
        # channel order) starting at or before it.  Samples sort before grid points at
        # equal times; the struct keeps null values distinguishable from "no sample".
        as_of = (
            Window.partitionBy(*key)
            .orderBy(t, kind, ts, te)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        previous = F.last(F.when(kind == 0, F.struct(F.col(val).alias("value"))), ignorenulls=True)
        df = df.withColumn("_rs_previous", previous.over(as_of)).where(kind != 0)

        is_grid = kind == 1
        derived = {
            ts: t.cast(rows.schema[ts].dataType),
            te: F.col("_rs_e.tend").cast(rows.schema[te].dataType),
            # Beyond the last sample's end the pandas interpolation yields NaN.
            val: F.when(t > F.col("_rs_last_te"), F.lit(None))
            .otherwise(F.col("_rs_previous.value"))
            .cast(rows.schema[val].dataType),
        }
        return df.select(
            *[
                F.when(is_grid, derived[c]).otherwise(F.col(c)).alias(c) if c in derived else c
                for c in rows.columns
            ]
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
