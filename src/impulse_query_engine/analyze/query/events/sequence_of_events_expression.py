from __future__ import annotations

import numpy as np

from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache
from impulse_query_engine.model.series.intervals import Intervals


class SequenceOfEventsExpression(TimeSeriesExpression):
    """Determines sequences of events from an ordered list of Intervals-producing expressions.

    Visual timeline (overlapping consecutive events):

        time --->
        event_1: | ------------------- |
        event_2:             | ------------- |
        sequence:| ------------------------- |

    Rule:
    - When the next interval starts before the current one ends
      (next.start <= current.end), they are considered part of the same
      sequence, and the sequence spans from the first event start to the
      next event end.
    """

    def __init__(self, expressions: list[TimeSeriesExpression], max_overlap: float = None):
        """
        Initialize a SequenceOfEventsExpression.

        Parameters
        ----------
        expressions : list of TimeSeriesExpression
            Ordered list of expressions. Each must return Intervals from build().
        max_overlap : float, optional
            Maximum allowed overlap time between consecutive events.
            If not None, intervals whose overlap exceeds this value are skipped.
            Expressed in the same time units as the underlying data
            (e.g. milliseconds-since-epoch timestamps), not seconds or
            any other derived unit.
        """
        if not expressions:
            raise ValueError("SequenceOfEventsExpression requires at least one expression.")
        self.expressions = expressions
        self.max_overlap = max_overlap
        TimeSeriesExpression.__init__(self, is_single_signal=False)

    def __str__(self) -> str:
        """
        Return a string representation of the SequenceOfEventsExpression.

        Returns
        -------
        str
            String representation of the object.
        """
        exprs_str = ", ".join([str(e) for e in self.expressions])
        return f"SequenceOfEventsExpression<[{exprs_str}], max_overlap={self.max_overlap}>"

    def dtype(self):
        """
        Return the Spark data type of the result.

        Returns
        -------
        pyspark.sql.types.ArrayType
            Same dtype as Intervals: ArrayType(ArrayType(DoubleType())).
        """
        return Intervals.empty().dtype()

    def get_required_tag_exprs(self) -> set[TagExpression]:
        """
        Return the union of required tag expressions from all child expressions.

        Returns
        -------
        set of TagExpression
        """
        tags: set[TagExpression] = set()
        for expr in self.expressions:
            tags = tags.union(expr.get_required_tag_exprs())
        return tags

    def required_tags(self) -> set[str]:
        """
        Return the union of required tags from all child expressions.

        Returns
        -------
        set of str
        """
        tags: set[str] = set()
        for expr in self.expressions:
            tags = tags.union(expr.required_tags())
        return tags

    def get_selectors(self) -> list[TimeSeriesSelector]:
        result: list[TimeSeriesSelector] = []
        for expr in self.expressions:
            result.extend(expr.get_selectors())
        return result

    def get_selector_expr(self):
        """
        Return the combined selector expression (OR of all children).

        Returns
        -------
        selector expression or None
        """
        expr = None
        for e in self.expressions:
            child_expr = e.get_selector_expr()
            if expr is None:
                expr = child_expr
            else:
                expr = expr | child_expr
        return expr

    def build(self, cache: SeriesCache) -> Intervals:
        """
        Build the sequence-of-events result.

        For each consecutive pair of Intervals I_i and I_{i+1}, an interval
        (s_{i+1}, e_{i+1}) from I_{i+1} is considered in sequence with a chain
        ending at e_prev iff s_{i+1} <= e_prev.

        Parameters
        ----------
        cache : SeriesCache
            Cache providing time series data.
        Returns
        -------
        Intervals
            Each returned interval spans one complete sequence from its start
            to the end of the last matching event.
        """
        interval_lists = [expr.build(cache) for expr in self.expressions]

        # Short-circuit if any level is empty
        if any(len(iv) == 0 for iv in interval_lists):
            return Intervals.empty()

        # Seed chains from level 0
        chains_starts = interval_lists[0].tstarts.copy()
        chains_ends = interval_lists[0].tends.copy()
        chains_last_starts = interval_lists[0].tstarts.copy()

        # Extend chains through each subsequent level
        for k in range(1, len(interval_lists)):
            next_iv = interval_lists[k]
            new_starts = []
            new_ends = []
            new_last_starts = []

            for i in range(len(chains_starts)):
                e_prev = chains_ends[i]
                s_prev = chains_last_starts[i]
                # next_iv.tstarts is sorted; find all where tstart <= e_prev
                count = int(np.searchsorted(next_iv.tstarts, e_prev, side="right"))
                for j in range(count):
                    # Next event must not start earlier than the previous event
                    if next_iv.tstarts[j] < s_prev:
                        continue
                    overlap = e_prev - next_iv.tstarts[j]
                    if self.max_overlap is not None and overlap > self.max_overlap:
                        continue
                    new_starts.append(chains_starts[i])
                    new_ends.append(next_iv.tends[j])
                    new_last_starts.append(next_iv.tstarts[j])

            if len(new_starts) == 0:
                return Intervals.empty()

            chains_starts = np.array(new_starts, dtype=np.float64)
            chains_ends = np.array(new_ends, dtype=np.float64)
            chains_last_starts = np.array(new_last_starts, dtype=np.float64)

        return Intervals(chains_starts, chains_ends)
