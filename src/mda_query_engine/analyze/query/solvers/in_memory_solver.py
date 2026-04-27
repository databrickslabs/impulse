from .query_solver import QuerySolver

from .series_cache import SeriesCache


class InMemoryCache(SeriesCache):
    pass


class InMemorySolver(QuerySolver):
    def solve(self, query, channels_df, selections, dtypes=None):
        pass
