from .series_cache import SeriesCache
from impulse_query_engine.model.series.sample_series import SampleSeries


class EmptyTimeSeriesCache(SeriesCache):
    def __init__(self):
        """
        Initialize the EmptyTimeSeriesCache.
        """
        pass

    def resolve(self, selection):
        """
        Return an empty list for any selection.

        Parameters
        ----------
        selection : Any
            The selection object specifying tags or metrics.

        Returns
        -------
        list
            An empty list.
        """
        return []

    def load_blob(self, mid, cid, uses_alias: bool = False):
        """
        Return an empty SampleSeries for any container and channel ID.

        Parameters
        ----------
        mid : Any
            Container or measurement ID.
        cid : Any
            Channel ID.
        uses_alias : bool, optional
            Unused by this cache; accepted for interface compatibility
            with :class:`SeriesCache`.

        Returns
        -------
        SampleSeries
            An empty SampleSeries object.
        """
        return SampleSeries.empty()
