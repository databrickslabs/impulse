"""POI channel selection.

A ``PoiChannelSelector`` treats one ``poi_type`` as a channel: the POI rows of
that type, read as ``(timestamp, value)`` per container, become a
``PointsInTimeSeries`` (a value at each instant).

A POI Channel
- is resolved against the *wide* ``poi`` table with plain column-equality,
- stays **out** of the channel pipeline (``get_selectors() -> []``) and is
  collected via the parallel ``get_poi_channel_selectors()`` walker,
- builds to a ``PointsInTimeSeries`` instead of a ``SampleSeries``.

Row refinements (``network="FD3"`` …) are applied via :meth:`having`, kept
separate from the ``poi_type`` identity passed to ``q.poi_channel``.
"""

from __future__ import annotations

import zlib

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.model.series.points_in_time_series import PointsInTimeSeries


class PoiChannelSelector(TimeSeriesExpression):
    def __init__(self, poi_type: str, having: dict | None = None):
        """
        Initialize a PoiChannelSelector.

        Parameters
        ----------
        poi_type : str
            The ``poi_type`` identifying this channel (e.g. ``"charging_error"``).
        having : dict or None, optional
            Column-equality refinements applied to the POI rows (e.g.
            ``{"network": "FD3"}``). Prefer building these via :meth:`having`.
        """
        self.poi_type = poi_type
        self._having = dict(having) if having else {}
        TimeSeriesExpression.__init__(self, is_single_signal=True)

    def having(self, **filters) -> "PoiChannelSelector":
        """
        Return a new selector with extra column-equality row refinements ANDed on.

        Immutable: the receiver is unchanged.

        Parameters
        ----------
        **filters : dict
            ``column == value`` restrictions on the POI rows (e.g. ``network="FD3"``).

        Returns
        -------
        PoiChannelSelector
        """
        return PoiChannelSelector(self.poi_type, {**self._having, **filters})

    @property
    def row_filters(self) -> dict:
        """The column-equality refinements applied on top of ``poi_type``."""
        return self._having

    @property
    def selector_id(self) -> int:
        """Stable identity over ``(poi_type, sorted(having))``."""
        key = (self.poi_type, tuple(sorted(self._having.items())))
        return zlib.crc32(str(key).encode())

    def dtype(self):
        """Spark data type of the selection result: ``array<array<double>>``.

        Matches :meth:`PointsInTimeSeries.dtype`; the solve UDF serializes the
        result via ``get_data()`` (list of ``[ts, value]`` pairs).
        """
        return PointsInTimeSeries.empty().dtype()

    def build(self, cache) -> PointsInTimeSeries:
        """
        Build a PointsInTimeSeries from this container's POI rows.

        Structurally identical to :meth:`TimeSeriesSelector.build`: ``resolve`` the
        rows this selector matches (POI union rows were stamped with this
        selector's ``selector_id`` in Stage P), read their ``(container_id,
        channel_id)``, then ``load_poi_blob``. Runs inside the per-container solve
        UDF; rows are already resident (no I/O).

        Parameters
        ----------
        cache : SeriesCache
            Per-container cache exposing ``resolve`` and ``load_poi_blob``.

        Returns
        -------
        PointsInTimeSeries
        """
        candidates = cache.resolve(self)
        if len(candidates) == 0:
            return PointsInTimeSeries.empty()
        mid = candidates.container_id.iloc[0]
        cid = candidates.channel_id.iloc[0]
        return cache.load_poi_blob(mid, cid)

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
        if self._having:
            refine = ",".join(f"{k}={v}" for k, v in sorted(self._having.items()))
            return f"PoiChannelSelector<{self.poi_type}|{refine}>"
        return f"PoiChannelSelector<{self.poi_type}>"

        #todo should we do it like MetricSelector?? There it's just key?
        # todo we need to decide how the layout looks like that this receives
