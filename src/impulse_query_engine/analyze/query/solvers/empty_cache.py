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
        selection : TimeSeriesSelector
            The channel selector; ignored by this cache.

        Returns
        -------
        list
            An empty list.
        """
        return []

    def load_blob(self, mid, cid, uses_alias: bool = False, series_type=None, value_type=None):
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
        series_type, value_type : optional
            Accepted for interface compatibility. The empty-series typing for a
            POI selector is handled by ``TimeSeriesSelector.build`` (its length-0
            branch), which returns the correctly typed empty series without
            reaching this method.

        Returns
        -------
        SampleSeries
            An empty SampleSeries object.
        """
        return SampleSeries.empty()
