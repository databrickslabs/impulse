"""PointsInTimeSeriesString: the categorical (string-valued) PointsInTimeSeries."""

from __future__ import annotations

from collections.abc import Sized

import numpy as np
import pyspark.sql.types as T

from .points_in_time import PointsInTime
from .points_in_time_series import PointsInTimeSeries


def _op_unsupported(op: str) -> str:
    """Message for a numeric operation attempted on a categorical string series."""
    return (
        f"{op} is not defined for PointsInTimeSeriesString (categorical values). "
        "Use a numeric POI channel (dtype='double') for arithmetic/ordering/stats, "
        "or == / != to select instants by value."
    )


class PointsInTimeSeriesString(PointsInTimeSeries):
    """A :class:`PointsInTimeSeries` whose values are **strings** (categorical).

    Built for POI channels whose ``value`` is a VARCHAR (defect codes, state
    labels …). It keeps the timestamp axis numeric but holds the values as an
    object/string array instead of ``float64``.

    Only the **equality** operators are meaningful on categories — ``== "code"``
    / ``!= "code"`` return a :class:`PointsInTime` of the matching instants
    (identical to the numeric series: the result is a *set of timestamps*, so the
    value type is irrelevant to it). Ordering (``<``/``>``), arithmetic
    (``+``/``*``/…) and numeric reductions (``mean``/``sum``/``min``/``max``) are
    **not** defined — "the mean of defect codes" is meaningless — and raise
    ``TypeError`` rather than silently coercing to ``NaN``.

    Serialization differs from the numeric series: a ``[ts, value]`` pair cannot
    be a homogeneous ``array<double>`` when the value is a string, so the Spark
    type is a struct of two parallel arrays ``(tstarts: array<double>,
    values: array<string>)`` and :meth:`get_data` returns that ``(ts, values)``
    tuple.
    """

    def __init__(self, tstarts: Sized, values: Sized):
        """Initialize with numeric timestamps and **string** values.

        Parameters
        ----------
        tstarts : Sized
            Array-like of time points (numeric).
        values : Sized
            Array-like of string values, one per time point.
        """
        assert len(tstarts) == len(values)
        # tstarts stay numeric; values are kept as strings (object dtype), not
        # coerced to float64 (which would turn every code into NaN).
        self.tstarts = np.array(tstarts, dtype=np.float64)
        self.values = np.array([None if v is None else str(v) for v in values], dtype=object)

    def dtype(self) -> T.StructType:
        """Spark type: a struct of parallel ``(tstarts, values)`` arrays.

        Unlike the numeric series (``array<array<double>>``), a string value
        can't ride a homogeneous numeric pair array, so timestamps and values are
        emitted as two same-length arrays.
        """
        return T.StructType(
            [
                T.StructField("tstarts", T.ArrayType(T.DoubleType())),
                T.StructField("values", T.ArrayType(T.StringType())),
            ]
        )

    def get_data(self) -> tuple[list, list]:
        """Return ``(tstarts, values)`` as two parallel lists (matches :meth:`dtype`)."""
        if len(self) == 0:
            return ([], [])
        return (self.tstarts.tolist(), [None if v is None else str(v) for v in self.values])

    # --- categorical ops: equality yields instants; everything numeric is barred

    def __eq__(self, other) -> PointsInTime:
        """Return the instants whose value equals ``other`` (string equality)."""
        idx = self.values == other
        return PointsInTime(self.tstarts[idx])

    def __ne__(self, other) -> PointsInTime:
        """Return the instants whose value differs from ``other``."""
        idx = self.values != other
        return PointsInTime(self.tstarts[idx])

    __hash__ = None

    # Numeric operations are undefined on categorical values. Each raises
    # directly (rather than delegating) with a consistent message built by
    # ``_op_unsupported``, so a misuse fails loudly instead of coercing to NaN.
    def __lt__(self, other):
        raise TypeError(_op_unsupported("<"))

    def __le__(self, other):
        raise TypeError(_op_unsupported("<="))

    def __gt__(self, other):
        raise TypeError(_op_unsupported(">"))

    def __ge__(self, other):
        raise TypeError(_op_unsupported(">="))

    def __add__(self, other):
        raise TypeError(_op_unsupported("+"))

    __radd__ = __add__

    def __sub__(self, other):
        raise TypeError(_op_unsupported("-"))

    __rsub__ = __sub__

    def __mul__(self, other):
        raise TypeError(_op_unsupported("*"))

    __rmul__ = __mul__

    def __truediv__(self, other):
        raise TypeError(_op_unsupported("/"))

    __rtruediv__ = __truediv__

    def sum(self):
        raise TypeError(_op_unsupported("sum()"))

    def mean(self):
        raise TypeError(_op_unsupported("mean()"))

    def min(self):
        raise TypeError(_op_unsupported("min()"))

    def max(self):
        raise TypeError(_op_unsupported("max()"))

    def __str__(self) -> str:
        return f"<PointsInTimeSeriesString({self.start_time()}..cnt:{len(self)}..{self.end_time()})>"

    @staticmethod
    def empty() -> "PointsInTimeSeriesString":
        """Return an empty string-valued series."""
        return PointsInTimeSeriesString([], [])


def reject_categorical_inputs(series_list, owner: str, names=None) -> None:
    """Raise if any built input *series* is a categorical (string-valued) series.

    Numeric aggregations (mean/min/max/…) are meaningless on categorical POI
    values, so an aggregator must reject a ``PointsInTimeSeriesString`` passed as
    a value-carrying **input**. Called at ``build`` time — which also runs at plan
    time against the empty cache (a ``dtype="string"`` POI channel builds to an
    empty ``PointsInTimeSeriesString``) — so misuse fails **before** the Spark job
    rather than mid-solve.

    Parameters
    ----------
    series_list : Iterable
        The built input series to check, in input order.
    owner : str
        The aggregator class name, for the error message.
    names : list of str, optional
        Input names parallel to *series_list*; falls back to ``input[i]``.

    Raises
    ------
    TypeError
        If any element is a :class:`PointsInTimeSeriesString`.
    """
    for i, series in enumerate(series_list):
        if isinstance(series, PointsInTimeSeriesString):
            name = names[i] if names and i < len(names) else f"input[{i}]"
            raise TypeError(
                f"{owner} received a categorical (string-valued) POI channel as input "
                f"{name!r}. Numeric aggregation over string values is undefined. Use a "
                "numeric POI channel (dtype='double') as an aggregation input, or use the "
                "string channel only as a gate/event (e.g. channel.where(poi == 'CODE'))."
            )
