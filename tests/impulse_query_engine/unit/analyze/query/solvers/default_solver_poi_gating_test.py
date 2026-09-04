"""Unit tests for ``DefaultSolver._queryDefaultSolver._query_contains_poi_selections_selections`` (the POI-union gate).
These are Spark-free: they build selectors directly and call the static gate.
"""

# pylint: disable=missing-function-docstring
from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import (
    SeriesType,
    SeriesValueType,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver


def _poi(channel="DTC_count", value_type=SeriesValueType.DOUBLE):
    return TimeSeriesSelector(
        TagSelector("channel_name") == channel,
        series_type=SeriesType.POINTS_IN_TIME,
        value_type=value_type,
    )


def _sample(name="seed"):
    return TimeSeriesSelector(TagSelector(name) == "0")


def test_bare_poi_selection_detected():
    assert DefaultSolver._query_contains_poi_selections([_poi()]) is True


def test_poi_wrapped_in_aggregation_detected():
    # The regression case: .sum()/.count() wrap the POI selector in a TimeSeriesOp.
    assert DefaultSolver._query_contains_poi_selections([_poi().sum()]) is True
    assert DefaultSolver._query_contains_poi_selections([_poi().count()]) is True


def test_string_poi_equality_op_detected():
    poi = _poi(channel="DTC", value_type=SeriesValueType.STRING)
    assert DefaultSolver._query_contains_poi_selections([poi == "P0301"]) is True


def test_mixed_sample_and_wrapped_poi_detected():
    assert DefaultSolver._query_contains_poi_selections([_sample().sum(), _poi().sum()]) is True


def test_sample_only_not_detected():
    assert DefaultSolver._query_contains_poi_selections([_sample()]) is False
    assert DefaultSolver._query_contains_poi_selections([_sample().sum()]) is False


def test_empty_selections_not_detected():
    assert DefaultSolver._query_contains_poi_selections([]) is False


def test_non_expression_entries_ignored():
    # collect_selectors silently skips non-TimeSeriesExpression items.
    assert DefaultSolver._query_contains_poi_selections(["not-an-expression", 42]) is False
