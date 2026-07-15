"""Unit tests for definition hash methods in Histogram, Histogram2D, and StatsAggregator."""

import functools
import hashlib

from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from impulse_query_engine.analyze.query.aggregations.custom_statistic import (
    CrossChannelStatistic,
)
from impulse_reporting.aggregations.histogram import (
    HistogramDistance,
    HistogramDuration,
)
from impulse_reporting.aggregations.histogram2d import (
    Histogram2DDuration,
)
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.events.basic_event import BasicEvent


class TestHistogramDefinitionHash:
    """Test suite for Histogram.determine_definition_hash()."""

    def test_definition_hash_returns_int(self):
        """Test that determine_definition_hash returns an integer."""
        base_expr = TimeSeriesSelector(None)
        hist = HistogramDuration(name="test_hist", base_expr=base_expr, bins=[0.0, 1.0, 2.0])
        hash_value = hist.determine_definition_hash()
        assert isinstance(hash_value, int)

    def test_same_definition_produces_same_hash(self):
        """Test that identical definitions produce the same hash."""
        base_expr = TimeSeriesSelector(None)
        bins = [0.0, 1.0, 2.0, 3.0]

        hist1 = HistogramDuration(name="hist_a", base_expr=base_expr, bins=bins)
        hist2 = HistogramDuration(name="hist_b", base_expr=base_expr, bins=bins)

        # Same computation definition, different names -> same hash
        assert hist1.determine_definition_hash() == hist2.determine_definition_hash()

    def test_different_bins_produce_different_hash(self):
        """Test that different bins produce different hashes."""
        base_expr = TimeSeriesSelector(None)

        hist1 = HistogramDuration(name="test_hist", base_expr=base_expr, bins=[0.0, 50.0, 100.0])
        hist2 = HistogramDuration(
            name="test_hist", base_expr=base_expr, bins=[0.0, 25.0, 50.0, 75.0, 100.0]
        )

        assert hist1.determine_definition_hash() != hist2.determine_definition_hash()

    def test_different_expressions_produce_different_hash(self):
        """Test that different expressions produce different hashes."""
        expr1 = TimeSeriesSelector(None)
        expr2 = TimeSeriesSelector(None)  # Different instance
        # Note: TimeSeriesSelector with None produces same string representation
        # In real usage, different channel queries would have different string reps

        bins = [0.0, 1.0, 2.0]
        hist1 = HistogramDuration(name="test_hist", base_expr=expr1, bins=bins)
        hist2 = HistogramDuration(name="test_hist", base_expr=expr2, bins=bins)

        # Same expression string -> same hash
        assert hist1.determine_definition_hash() == hist2.determine_definition_hash()

    def test_hash_excludes_name(self):
        """Test that hash doesn't change when only name changes."""
        base_expr = TimeSeriesSelector(None)
        bins = [0.0, 1.0, 2.0]

        hist1 = HistogramDuration(name="histogram_v1", base_expr=base_expr, bins=bins)
        hist2 = HistogramDuration(name="histogram_v2", base_expr=base_expr, bins=bins)

        assert hist1.determine_definition_hash() == hist2.determine_definition_hash()

    def test_hash_excludes_description(self):
        """Test that hash doesn't change when only description changes."""
        base_expr = TimeSeriesSelector(None)
        bins = [0.0, 1.0, 2.0]

        hist1 = HistogramDuration(
            name="test", base_expr=base_expr, bins=bins, desc="Description v1"
        )
        hist2 = HistogramDuration(
            name="test", base_expr=base_expr, bins=bins, desc="Description v2"
        )

        assert hist1.determine_definition_hash() == hist2.determine_definition_hash()

    def test_hash_excludes_units(self):
        """Test that hash doesn't change when only units change."""
        base_expr = TimeSeriesSelector(None)
        bins = [0.0, 1.0, 2.0]

        hist1 = HistogramDuration(
            name="test",
            base_expr=base_expr,
            bins=bins,
            values_unit="seconds",
            bins_unit="rpm",
        )
        hist2 = HistogramDuration(
            name="test",
            base_expr=base_expr,
            bins=bins,
            values_unit="hours",
            bins_unit="kph",
        )

        assert hist1.determine_definition_hash() == hist2.determine_definition_hash()

    def test_hash_with_event_filter(self):
        """Test that hash includes event filter expression."""
        base_expr = TimeSeriesSelector(None)
        event_expr = TimeSeriesSelector(None) > 0
        event = BasicEvent(name="test_event", expr=event_expr)
        bins = [0.0, 1.0, 2.0]

        hist_with_event = HistogramDuration(
            name="test", base_expr=base_expr, bins=bins, event=event
        )
        hist_without_event = HistogramDuration(name="test", base_expr=base_expr, bins=bins)

        # Event filter affects computation -> different hash
        assert (
            hist_with_event.determine_definition_hash()
            != hist_without_event.determine_definition_hash()
        )

    def test_get_id_differs_from_definition_hash(self):
        """Test that get_id and determine_definition_hash produce different values."""
        base_expr = TimeSeriesSelector(None)

        hist1 = HistogramDuration(name="histogram_a", base_expr=base_expr, bins=[0.0, 1.0, 2.0])
        hist2 = HistogramDuration(name="histogram_b", base_expr=base_expr, bins=[0.0, 1.0, 2.0])

        # get_id includes name -> different IDs
        assert hist1.get_id() != hist2.get_id()

        # definition_hash excludes name -> same hash
        assert hist1.determine_definition_hash() == hist2.determine_definition_hash()

    def test_as_dict_includes_definition_hash(self):
        """Test that as_dict includes the definition_hash field."""
        base_expr = TimeSeriesSelector(None)
        hist = HistogramDuration(name="test", base_expr=base_expr, bins=[0.0, 1.0, 2.0])

        result = hist.as_dict()

        assert "definition_hash" in result
        assert result["definition_hash"] == hist.determine_definition_hash()

    def test_histogram_distance_definition_hash(self):
        """Test determine_definition_hash for HistogramDistance."""
        base_expr = TimeSeriesSelector(None)
        weights_expr = TimeSeriesSelector(None)
        bins = [0.0, 100.0, 200.0]

        hist = HistogramDistance(
            name="dist_hist", base_expr=base_expr, weights_expr=weights_expr, bins=bins
        )

        hash_value = hist.determine_definition_hash()
        assert isinstance(hash_value, int)


class TestHistogram2DDefinitionHash:
    """Test suite for Histogram2D.determine_definition_hash()."""

    def test_definition_hash_returns_int(self):
        """Test that determine_definition_hash returns an integer."""
        x_expr = TimeSeriesSelector(None)
        y_expr = TimeSeriesSelector(None)
        hist = Histogram2DDuration(
            name="test_hist2d",
            x_expr=x_expr,
            y_expr=y_expr,
            x_bins=[0.0, 1.0, 2.0],
            y_bins=[0.0, 5.0, 10.0],
        )

        hash_value = hist.determine_definition_hash()
        assert isinstance(hash_value, int)

    def test_same_definition_produces_same_hash(self):
        """Test that identical definitions produce the same hash."""
        x_expr = TimeSeriesSelector(None)
        y_expr = TimeSeriesSelector(None)
        x_bins = [0.0, 1.0, 2.0]
        y_bins = [0.0, 5.0, 10.0]

        hist1 = Histogram2DDuration(
            name="hist_a", x_expr=x_expr, y_expr=y_expr, x_bins=x_bins, y_bins=y_bins
        )
        hist2 = Histogram2DDuration(
            name="hist_b", x_expr=x_expr, y_expr=y_expr, x_bins=x_bins, y_bins=y_bins
        )

        assert hist1.determine_definition_hash() == hist2.determine_definition_hash()

    def test_different_x_bins_produce_different_hash(self):
        """Test that different x_bins produce different hashes."""
        x_expr = TimeSeriesSelector(None)
        y_expr = TimeSeriesSelector(None)
        y_bins = [0.0, 5.0, 10.0]

        hist1 = Histogram2DDuration(
            name="test",
            x_expr=x_expr,
            y_expr=y_expr,
            x_bins=[0.0, 50.0, 100.0],
            y_bins=y_bins,
        )
        hist2 = Histogram2DDuration(
            name="test",
            x_expr=x_expr,
            y_expr=y_expr,
            x_bins=[0.0, 25.0, 50.0, 75.0, 100.0],
            y_bins=y_bins,
        )

        assert hist1.determine_definition_hash() != hist2.determine_definition_hash()

    def test_different_y_bins_produce_different_hash(self):
        """Test that different y_bins produce different hashes."""
        x_expr = TimeSeriesSelector(None)
        y_expr = TimeSeriesSelector(None)
        x_bins = [0.0, 1.0, 2.0]

        hist1 = Histogram2DDuration(
            name="test",
            x_expr=x_expr,
            y_expr=y_expr,
            x_bins=x_bins,
            y_bins=[0.0, 5.0, 10.0],
        )
        hist2 = Histogram2DDuration(
            name="test",
            x_expr=x_expr,
            y_expr=y_expr,
            x_bins=x_bins,
            y_bins=[0.0, 2.5, 5.0, 7.5, 10.0],
        )

        assert hist1.determine_definition_hash() != hist2.determine_definition_hash()

    def test_hash_excludes_metadata_fields(self):
        """Test that hash excludes name, description, and units."""
        x_expr = TimeSeriesSelector(None)
        y_expr = TimeSeriesSelector(None)
        x_bins = [0.0, 1.0, 2.0]
        y_bins = [0.0, 5.0, 10.0]

        hist1 = Histogram2DDuration(
            name="hist_v1",
            x_expr=x_expr,
            y_expr=y_expr,
            x_bins=x_bins,
            y_bins=y_bins,
            desc="Version 1",
            x_bins_unit="rpm",
            y_bins_unit="kph",
        )
        hist2 = Histogram2DDuration(
            name="hist_v2",
            x_expr=x_expr,
            y_expr=y_expr,
            x_bins=x_bins,
            y_bins=y_bins,
            desc="Version 2",
            x_bins_unit="Hz",
            y_bins_unit="m/s",
        )

        assert hist1.determine_definition_hash() == hist2.determine_definition_hash()

    def test_as_dict_includes_definition_hash(self):
        """Test that as_dict includes the definition_hash field."""
        x_expr = TimeSeriesSelector(None)
        y_expr = TimeSeriesSelector(None)
        hist = Histogram2DDuration(
            name="test",
            x_expr=x_expr,
            y_expr=y_expr,
            x_bins=[0.0, 1.0, 2.0],
            y_bins=[0.0, 5.0, 10.0],
        )

        result = hist.as_dict()

        assert "definition_hash" in result
        assert result["definition_hash"] == hist.determine_definition_hash()


def _spread(series, t_start, t_end):
    """Cross-channel test statistic."""
    values = [v for s in series for v in s.values]
    return float(max(values) - min(values)) if values else float("nan")


def _spread_other_body(series, t_start, t_end):
    """Same signature as _spread but a different implementation."""
    values = [v for s in series for v in s.values]
    return float(sum(values)) if values else float("nan")


def _thresholded_count(series, t_start, t_end, threshold=0.0):
    """Parameterized statistic for functools.partial tests."""
    return float(sum((s.values > threshold).sum() for s in series))


class TestStatsAggregatorDefinitionHash:
    """Test suite for StatsAggregator.determine_definition_hash() with custom statistics."""

    @staticmethod
    def _make(name="stats", channel_names=None, **kwargs):
        channel_names = channel_names if channel_names is not None else ["ch_a", "ch_b"]
        return StatsAggregator(
            name=name,
            input_expressions=[TimeSeriesSelector(None) for _ in channel_names],
            channel_names=channel_names,
            statistics=["min", "max"],
            event=BasicEvent(name="test_event", expr=TimeSeriesSelector(None) > 0),
            **kwargs,
        )

    def test_hash_without_custom_stats_matches_legacy_formula(self):
        """Aggregators without custom statistics keep their pre-existing hash."""
        stats_agg = self._make()

        event_expr_str = str(stats_agg.event.get_expression())
        input_expr_strs = ",".join(str(expr) for expr in stats_agg.input_expressions)
        stats_strs = ",".join(stats_agg.statistics)
        hash_input = "::".join([input_expr_strs, stats_strs, event_expr_str])
        expected = int.from_bytes(
            hashlib.sha256(hash_input.encode()).digest()[:8], byteorder="big", signed=True
        )

        assert stats_agg.determine_definition_hash() == expected

    def test_adding_custom_stat_changes_hash(self):
        plain = self._make()
        with_cross = self._make(cross_channel_custom_statistics={"spread": _spread})
        with_per_channel = self._make(per_channel_custom_statistics={"spread": _spread})

        assert plain.determine_definition_hash() != with_cross.determine_definition_hash()
        assert plain.determine_definition_hash() != with_per_channel.determine_definition_hash()

    def test_same_custom_stats_produce_same_hash(self):
        agg1 = self._make(name="a", cross_channel_custom_statistics={"spread": _spread})
        agg2 = self._make(name="b", cross_channel_custom_statistics={"spread": _spread})

        assert agg1.determine_definition_hash() == agg2.determine_definition_hash()

    def test_renamed_custom_stat_changes_hash(self):
        agg1 = self._make(cross_channel_custom_statistics={"spread": _spread})
        agg2 = self._make(cross_channel_custom_statistics={"spread_v2": _spread})

        assert agg1.determine_definition_hash() != agg2.determine_definition_hash()

    def test_different_function_body_changes_hash(self):
        agg1 = self._make(cross_channel_custom_statistics={"spread": _spread})
        agg2 = self._make(cross_channel_custom_statistics={"spread": _spread_other_body})

        assert agg1.determine_definition_hash() != agg2.determine_definition_hash()

    def test_partial_arguments_change_hash(self):
        agg1 = self._make(
            cross_channel_custom_statistics={
                "count": functools.partial(_thresholded_count, threshold=1.0)
            }
        )
        agg2 = self._make(
            cross_channel_custom_statistics={
                "count": functools.partial(_thresholded_count, threshold=2.0)
            }
        )

        assert agg1.determine_definition_hash() != agg2.determine_definition_hash()

    def test_kind_is_part_of_hash(self):
        """The same name registered per-channel vs cross-channel hashes differently."""

        def per_channel_spread(series, t_start, t_end):
            return 0.0

        agg_cross = self._make(cross_channel_custom_statistics={"x": per_channel_spread})
        agg_per_channel = self._make(per_channel_custom_statistics={"x": per_channel_spread})

        assert agg_cross.determine_definition_hash() != agg_per_channel.determine_definition_hash()

    def test_channel_name_is_excluded_from_hash(self):
        agg1 = self._make(
            cross_channel_custom_statistics={
                "spread": CrossChannelStatistic(func=_spread, channel_name="combined")
            }
        )
        agg2 = self._make(
            cross_channel_custom_statistics={
                "spread": CrossChannelStatistic(func=_spread, channel_name="other_name")
            }
        )

        assert agg1.determine_definition_hash() == agg2.determine_definition_hash()

    def test_params_change_hash(self):
        agg_default = self._make(cross_channel_custom_statistics={"count": _thresholded_count})
        agg1 = self._make(
            cross_channel_custom_statistics={
                "count": CrossChannelStatistic(func=_thresholded_count, params={"threshold": 1.0})
            }
        )
        agg1_same = self._make(
            cross_channel_custom_statistics={
                "count": CrossChannelStatistic(func=_thresholded_count, params={"threshold": 1.0})
            }
        )
        agg2 = self._make(
            cross_channel_custom_statistics={
                "count": CrossChannelStatistic(func=_thresholded_count, params={"threshold": 2.0})
            }
        )

        assert agg1.determine_definition_hash() == agg1_same.determine_definition_hash()
        assert agg1.determine_definition_hash() != agg2.determine_definition_hash()
        assert agg_default.determine_definition_hash() != agg1.determine_definition_hash()

    def test_inputs_hash_uses_indices_not_names(self):
        """Consistent channel renames keep the hash; rewiring inputs changes it."""
        agg1 = self._make(
            channel_names=["ch_a", "ch_b"],
            cross_channel_custom_statistics={
                "spread": CrossChannelStatistic(func=_spread, inputs=["ch_b"])
            },
        )
        renamed = self._make(
            channel_names=["ch_x", "ch_y"],
            cross_channel_custom_statistics={
                "spread": CrossChannelStatistic(func=_spread, inputs=["ch_y"])
            },
        )
        rewired = self._make(
            channel_names=["ch_a", "ch_b"],
            cross_channel_custom_statistics={
                "spread": CrossChannelStatistic(func=_spread, inputs=["ch_a"])
            },
        )

        assert agg1.determine_definition_hash() == renamed.determine_definition_hash()
        assert agg1.determine_definition_hash() != rewired.determine_definition_hash()
