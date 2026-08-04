"""CalculatedChannel: a labeled derived time-series channel."""

import zlib

import pyspark.sql.types as T

from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache


class CalculatedChannel(TimeSeriesExpression):
    """A derived channel: a wrapped ``TimeSeriesExpression`` plus output identity.

    Wraps an expression built from the operator DSL (e.g. ``rpm * 3.6``) that must
    evaluate to a :class:`SampleSeries`.  ``build()`` returns that series' raw
    ``(tstarts, tends, values)`` arrays, which
    :meth:`QueryBuilder.solve_calculated_channels` explodes into narrow
    silver-shaped rows plus an ``identity`` ``MapType`` column.

    Parameters
    ----------
    expr : TimeSeriesExpression
        The wrapped expression; must ``build()`` to a ``SampleSeries``.
    identity : dict of str
        Non-empty identity dict (arbitrary keys, e.g.
        ``{"channel_name": "Eng_RPM", "data_key": "TM"}``), emitted per row and
        used to seed the deterministic :attr:`channel_id`.

    Examples
    --------
    ::

        CalculatedChannel(rpm * 3.6, {"channel_name": "speed_kmh", "data_key": "CALC"})
    """

    _IDENTITY_SEPARATOR = "::"

    def __init__(
        self,
        expr: TimeSeriesExpression,
        identity: dict[str, str],
    ):
        if not identity:
            raise ValueError(
                "CalculatedChannel requires a non-empty identity dict "
                "(e.g. {'channel_name': 'Eng_RPM', 'data_key': 'TM'}); identity "
                "defines the output identifier columns and seeds the "
                "deterministic channel_id."
            )
        # Set _alias eagerly (identity values joined by "::"): a missing _alias
        # would hit TimeSeriesExpression.__getattr__ and return a callable instead
        # of raising. The solve path ignores it; a later .alias(...) overrides it.
        super().__init__(
            alias=self._IDENTITY_SEPARATOR.join(str(v) for v in identity.values()),
            is_single_signal=getattr(expr, "is_single_signal", True),
            requires_udf=getattr(expr, "requires_udf", False),
        )
        self.expr = expr
        self.identity = dict(identity)

    def __str__(self) -> str:
        return f"<CalculatedChannel identity={self.identity}, expr={self.expr}>"

    def canonical_identity(self) -> str:
        """Return a stable string encoding of the identity, used for the id hash.

        Keys are sorted so the encoding (and the derived ``channel_id``) is
        independent of kwarg order.
        """
        return self._IDENTITY_SEPARATOR.join(
            f"{k}={self.identity[k]}" for k in sorted(self.identity)
        )

    @property
    def channel_id(self) -> int:
        """Deterministic output ``channel_id`` derived from the identity.

        A CRC32 of :meth:`canonical_identity` masked to a positive int32, so the
        value is stable across runs/processes and fits both ``IntegerType`` and
        ``LongType`` source ``channel_id`` columns.  Determinism makes writes
        idempotent and joins predictable; sharing this one derivation across the
        query-engine and reporting layers keeps their ids in lockstep.
        """
        return zlib.crc32(self.canonical_identity().encode()) & 0x7FFFFFFF

    def build(self, cache: SeriesCache):
        """Evaluate the wrapped expression and return its raw ``(tstarts, tends, values)``.

        Unlike a typical ``TimeSeriesExpression`` (whose ``build`` yields a
        ``SampleSeries``), this returns the three parallel ``float64`` arrays
        underlying that series, since the calculated-channels solve path consumes
        the raw samples directly.
        """
        series = self.expr.build(cache)
        return series.tstarts, series.tends, series.values

    def dtype(self) -> T.DataType:
        """Spark type of ``build()``'s output: the raw ``(tstarts, tends, values)`` arrays.

        A struct of three arrays mirroring the tuple ``build()`` returns, with
        element types matching the narrow calculated-channels output
        (``tstart``/``tend`` are ``long``, ``value`` is ``double``).
        """
        return T.StructType(
            [
                T.StructField("tstarts", T.ArrayType(T.LongType())),
                T.StructField("tends", T.ArrayType(T.LongType())),
                T.StructField("values", T.ArrayType(T.DoubleType())),
            ]
        )

    def get_selectors(self) -> list[TimeSeriesSelector]:
        return self.expr.get_selectors()

    def get_selector_expr(self):
        return self.expr.get_selector_expr()

    def get_required_tag_exprs(self) -> set[TagExpression]:
        return self.expr.get_required_tag_exprs()

    def required_tags(self) -> set[str]:
        return self.expr.required_tags()
