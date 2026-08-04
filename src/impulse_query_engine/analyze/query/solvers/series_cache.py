from abc import ABC, abstractmethod

import pandas as pd

from impulse_query_engine.model.series.sample_series import SampleSeries


class SeriesCache(ABC):
    @abstractmethod
    def resolve(self, selection) -> pd.DataFrame:
        """
        Resolve selected tags/metrics to a list of candidates.

        Parameters
        ----------
        selection : Any
            The selection object specifying tags or metrics.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the resolved candidates.
        """
        pass

    # todo discuss why is this necessary
    def resolve_poi(self, selection) -> pd.DataFrame:
        """Return the POI rows matching *selection* for the current container.

        Deliberately **concrete, not** ``@abstractmethod`` (the PR #30 precedent): the
        default returns an empty frame, so every existing cache — including any written
        outside this repo — keeps working unchanged, and
        :class:`~impulse_query_engine.analyze.query.solvers.empty_cache.EmptyTimeSeriesCache`
        inherits it, which is what makes ``build(EmptyTimeSeriesCache())`` a valid,
        data-free type probe for a :class:`PoiSelector`.

        The solver's per-container cache (``TimeSeriesCache`` in ``default_solver.py``)
        overrides this to return the rows that survived the Spark-side ``filter_poi``
        stage, matched by ``selection.selector_id``.

        Parameters
        ----------
        selection : Any
            The asking POI selector (a ``PoiSelector``).  Carries ``selector_id`` and,
            when set, ``_attribute``.

        Returns
        -------
        pandas.DataFrame
            Empty by default.  An overriding cache returns a frame with at least a
            ``ts`` column (integer microseconds) and, when the selector names an
            ``attribute``, that attribute column.
        """
        return pd.DataFrame(columns=["ts"])

    @abstractmethod
    def load_blob(self, mid, cid, uses_alias: bool = False) -> SampleSeries:
        """
        Resolve given mid and cid to a series.

        Parameters
        ----------
        mid : Any
            Container or measurement ID.
        cid : Any
            Channel ID.
        uses_alias : bool, optional
            ``True`` when the calling selector resolves the channel via a
            ``channel_mapping`` alias.  Caches that perform unit conversion
            (e.g. :class:`TimeSeriesCache`) only apply the per-channel
            conversion factor when this is ``True``, so a direct selector
            on the same physical channel always returns raw values.
            Defaults to ``False`` (direct / no-conversion semantics).

        Returns
        -------
        SampleSeries
            The loaded sample series object.
        """
        pass
