"""POI channel selection.

A ``PoiChannelSelector`` treats one ``poi_type`` as a channel: the POI rows of
that type, read as ``(timestamp, value)`` per container, become a
``PointsInTimeSeries`` (a value at each instant).

A POI Channel
- is resolved against the *wide* ``poi`` table with plain column-equality,
- stays **out** of the channel pipeline (``get_selectors() -> []``) and is
  collected via the parallel ``get_poi_channel_selectors()`` walker,
- builds to a ``PointsInTimeSeries`` instead of a ``SampleSeries``.

Row refinements (``network="FD3"`` …) are passed as extra keyword arguments to
``q.poi_channel`` (e.g. ``q.poi_channel("defect", network="FD3")``), mirroring
``q.channel(**kwargs)``; multiple kwargs are ANDed. They are ANDed onto the
``poi_type`` identity and applied as equality predicates on the POI rows.
"""

from __future__ import annotations

import zlib

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.model.series.points_in_time_series import PointsInTimeSeries
from impulse_query_engine.model.series.points_in_time_series_string import (
    PointsInTimeSeriesString,
)

# Accepted ``dtype`` values and the empty series each maps to. ``"double"`` is
# the default (numeric), so POI channels stay numeric unless a categorical value
# type is requested explicitly.
_SERIES_FOR_DTYPE = {
    "double": PointsInTimeSeries,
    "string": PointsInTimeSeriesString,
}


class PoiChannelSelector(TimeSeriesExpression):
    def __init__(self, poi_type: str, dtype: str = "double", row_filters: dict | None = None):
        """
        Initialize a PoiChannelSelector.

        Parameters
        ----------
        poi_type : str
            The ``poi_type`` identifying this channel (e.g. ``"charging_error"``).
        dtype : str, optional
            How to interpret the POI ``value``: ``"double"`` (default, numeric →
            :class:`PointsInTimeSeries`) or ``"string"`` (categorical →
            :class:`PointsInTimeSeriesString`). The source ``value`` column is a
            single string column; this only decides interpretation at build time.
        row_filters : dict or None, optional
            Column-equality refinements ANDed onto the POI rows (e.g.
            ``{"network": "FD3"}``). Built by :meth:`QueryBuilder.poi_channel` from
            its extra keyword arguments (``q.poi_channel("defect", network="FD3")``).
        """
        if dtype not in _SERIES_FOR_DTYPE:
            raise ValueError(
                f"PoiChannelSelector dtype must be one of {sorted(_SERIES_FOR_DTYPE)}; got {dtype!r}."
            )
        self.poi_type = poi_type
        self.value_dtype = dtype
        self._row_filters = dict(row_filters) if row_filters else {}
        TimeSeriesExpression.__init__(self, is_single_signal=True)

    @property
    def row_filters(self) -> dict:
        """The column-equality refinements ANDed onto ``poi_type`` (from kwargs)."""
        return self._row_filters

    @property
    def selector_id(self) -> int:
        """Stable identity over ``(poi_type, dtype, sorted(row_filters))``.

        ``dtype`` is part of the identity because two selectors differing only by
        value interpretation produce different output series types, so they must
        resolve as distinct channels. ``row_filters`` are part of it because two
        selectors differing only by refinement carry different row subsets.
        """
        key = (self.poi_type, self.value_dtype, tuple(sorted(self._row_filters.items())))
        return zlib.crc32(str(key).encode())

    def _empty_series(self):
        """The empty result series matching this selector's ``dtype``."""
        return _SERIES_FOR_DTYPE[self.value_dtype].empty()

    def dtype(self):
        """Spark data type of the selection result.

        ``array<array<double>>`` for numeric (``dtype="double"``), or the
        string series' struct-of-parallel-arrays for ``dtype="string"``. The solve
        UDF serializes the built series via ``get_data()``.
        """
        return self._empty_series().dtype()

    def build(self, cache) -> PointsInTimeSeries:
        """
        Build a PointsInTimeSeries (numeric or string) from this container's POI rows.

        Structurally identical to :meth:`TimeSeriesSelector.build`: ``resolve`` the
        rows this selector matches (POI union rows were stamped with this
        selector's ``selector_id`` in Stage P), read their ``(container_id,
        channel_id)``, then ``load_poi_blob`` with this selector's ``dtype`` so the
        value column is interpreted numerically or kept categorical. Runs inside the
        per-container solve UDF; rows are already resident (no I/O).

        Parameters
        ----------
        cache : SeriesCache
            Per-container cache exposing ``resolve`` and ``load_poi_blob``.

        Returns
        -------
        PointsInTimeSeries or PointsInTimeSeriesString
        """
        candidates = cache.resolve(self)
        if len(candidates) == 0:
            return self._empty_series()
        mid = candidates.container_id.iloc[0]
        cid = candidates.channel_id.iloc[0]
        return cache.load_poi_blob(mid, cid, dtype=self.value_dtype)

    # --- pipeline routing: POI channels are NOT channel-pipeline selectors ---

    def get_selectors(self) -> list:
        """POI channels do not participate in the channel pipeline."""
        return []

    def get_poi_channel_selectors(self) -> list["PoiChannelSelector"]:
        """This node is a POI channel selector."""
        return [self]

    def get_required_tag_exprs(self) -> set:
        """No EAV tag expressions are required (POI is a wide table)."""
        return set()

    def required_tags(self) -> set[str]:
        """No channel tags are required."""
        return set()

    def get_selector_expr(self):
        """POI channels are resolved by the solver, not via a tag selector expr."""
        return None

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self._row_filters:
            refine = ",".join(f"{k}={v}" for k, v in sorted(self._row_filters.items()))
            return f"PoiChannelSelector<{self.poi_type}|{refine}>"
        return f"PoiChannelSelector<{self.poi_type}>"
