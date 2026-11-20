"""Calculated-channel rows produced in Spark, without the pandas UDF.

When every calculated channel of a solve is a pushdown as a whole
(:meth:`PushdownPlan.native_targets`), its rows are the pushdown's derived rows.  They
must equal what :meth:`DefaultSolver._solve_calculated_channels_udf` emits: values get
the conversion factor for aliased selectors, and the round-trip through float64 and
Arrow is mirrored (integral times via double, NaN as null).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame

from impulse_query_engine.analyze.query.optimizer import VARIANT_COL, PushdownPlan

from .solver_config import SolverConfig

#: ``{variant: {selector_id: [(channel_id, uses_alias), ...]}}``, as returned by
#: :meth:`PushdownPlan.native_targets`.
Targets = Mapping[int, Mapping[int, Sequence[tuple[int, bool]]]]

_CHANNEL_ID, _USES_ALIAS = "_cc_channel_id", "_cc_uses_alias"
_TARGET_TYPE = T.ArrayType(
    T.StructType(
        [T.StructField(_CHANNEL_ID, T.LongType()), T.StructField(_USES_ALIAS, T.BooleanType())]
    )
)


def native_calculated_channel_rows(
    joined_df: DataFrame, schema: T.StructType, targets: Targets, config: SolverConfig
) -> DataFrame:
    """The calculated channels' rows, taken directly from the pushed-down rows.

    Every derived row of *joined_df* is emitted once per calculated channel that
    *targets* builds from its variant and selectors.

    Parameters
    ----------
    joined_df : pyspark.sql.DataFrame
        Derived rows joined with the channel matches (``selector_ids``, optional
        conversion factor); no raw rows.
    schema : pyspark.sql.types.StructType
        Schema of the channel data; gives the output column types.
    targets : Targets
        The calculated channels of each variant and selector.
    config : SolverConfig
        Provides the internal column names.

    Returns
    -------
    pyspark.sql.DataFrame
        ``[container_id, channel_id, tstart, tend, value]``, typed like the UDF's output.
    """
    cid, ch = config.container_id_col, config.channel_id_col
    ts, te, val, conv = (
        config.tstart_col,
        config.tend_col,
        config.value_col,
        config.conversion_factor_col,
    )
    has_conversion = conv in joined_df.columns
    # Rows without targets are dropped by inline.
    rows = joined_df.select(
        cid, ts, te, val, *([conv] if has_conversion else []), F.inline(_channel_targets(targets))
    )
    value = F.col(val).cast("double")
    if has_conversion:
        value = _converted(value, F.col(conv))
    return rows.select(
        cid,
        F.col(_CHANNEL_ID).cast(schema[ch].dataType).alias(ch),
        _as_udf_output(F.col(ts), schema[ts].dataType).alias(ts),
        _as_udf_output(F.col(te), schema[te].dataType).alias(te),
        _as_udf_output(value, T.DoubleType()).alias(val),
    )


def attach_identity(rows: DataFrame, selections: Iterable, channel_id_col: str) -> DataFrame:
    """Add the ``identity`` map of each calculated channel to its rows.

    Identity is constant per channel_id, so it is attached via a channel_id-keyed
    CASE instead of being shipped with every row.
    """
    identity = None
    for cc in selections:
        branch = F.col(channel_id_col) == F.lit(cc.channel_id)
        id_map = _identity_map(cc.identity)
        identity = F.when(branch, id_map) if identity is None else identity.when(branch, id_map)
    return rows.withColumn("identity", identity)


def _channel_targets(targets: Targets) -> Column:
    """The calculated channels a row feeds, by its variant and selectors."""
    by_selector = F.try_element_at(_targets_by_variant(targets), F.col(VARIANT_COL))
    no_targets = F.array().cast(_TARGET_TYPE)
    return F.flatten(
        F.transform(
            PushdownPlan.channel_selector_ids("selector_ids"),
            lambda sid: F.coalesce(F.try_element_at(by_selector, sid), no_targets),
        )
    )


def _targets_by_variant(targets: Targets) -> Column:
    """*targets* as a ``map<variant, map<selector_id, array<target>>>`` literal."""
    return F.create_map(
        *[
            item
            for variant, by_selector in sorted(targets.items())
            for item in (F.lit(variant), _targets_by_selector(by_selector))
        ]
    )


def _targets_by_selector(by_selector: Mapping[int, Sequence[tuple[int, bool]]]) -> Column:
    """One variant's targets as a ``map<selector_id, array<target>>`` literal."""
    return F.create_map(
        *[
            item
            for selector_id, channels in sorted(by_selector.items())
            for item in (
                F.lit(selector_id).cast("long"),
                F.array(*[_target(channel_id, alias) for channel_id, alias in channels]),
            )
        ]
    )


def _target(channel_id: int, uses_alias: bool) -> Column:
    return F.struct(
        F.lit(channel_id).cast("long").alias(_CHANNEL_ID), F.lit(uses_alias).alias(_USES_ALIAS)
    )


def _converted(value: Column, factor: Column) -> Column:
    """*value* times the conversion factor for aliased selectors.

    Like ``TimeSeriesCache.to_sample_series``: a null or NaN factor leaves values as they are.
    """
    applies = F.col(_USES_ALIAS) & factor.isNotNull() & ~F.isnan(factor)
    return F.when(applies, value * factor).otherwise(value)


def _as_udf_output(col: Column, dtype: T.DataType) -> Column:
    """*col* as the UDF emits it: float64, cast back by Arrow with NaN masked as null."""
    col = col.cast("double")
    if isinstance(dtype, T.IntegralType):
        return col.cast(dtype)
    return F.when(F.isnan(col), F.lit(None)).otherwise(col).cast(dtype)


def _identity_map(identity: dict[str, str]) -> Column:
    """A ``map<string,string>`` literal column from an identity dict."""
    return F.create_map(*[F.lit(str(x)) for k, v in identity.items() for x in (k, v)])
