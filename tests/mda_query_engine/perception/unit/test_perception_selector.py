"""PerceptionSelector contract — `build`, AST round-trip, channel-side stub.

These tests don't need Spark: the selector reads a pandas DataFrame from
the cache and returns an Intervals object.
"""

import numpy as np
import pandas as pd

from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from mda_query_engine.model.series.intervals import Intervals
from mda_query_engine.perception.tsal.perception_selector import (
    PerceptionCache,
    PerceptionSelector,
)


def _make_cache(rows):
    pdf = pd.DataFrame(rows)
    channels_pdf = pd.DataFrame(
        {
            "container_id": pd.Series([], dtype="int64"),
            "channel_id": pd.Series([], dtype="int64"),
            "tstart": pd.Series([], dtype="float64"),
            "tend": pd.Series([], dtype="float64"),
            "value": pd.Series([], dtype="float64"),
        }
    )
    col_map = {
        "cid": "container_id",
        "ch": "channel_id",
        "ts": "tstart",
        "te": "tend",
        "val": "value",
    }
    return PerceptionCache(
        channels_pdf=channels_pdf,
        col_map=col_map,
        object_tracks_pdf=pdf,
    )


class TestPerceptionSelectorBuild:
    def test_string_eq_predicate_matches_rows_and_returns_intervals(self):
        cache = _make_cache(
            [
                {"container_id": 1, "object_id": 10, "frame_ts": 100.0, "detection_class": "cyclist"},
                {"container_id": 1, "object_id": 10, "frame_ts": 200.0, "detection_class": "cyclist"},
                {"container_id": 1, "object_id": 11, "frame_ts": 100.0, "detection_class": "car"},
            ]
        )
        sel = PerceptionSelector("detection_class", "eq", "cyclist")
        result = sel.build(cache)
        assert isinstance(result, Intervals)
        assert len(result) >= 1
        assert result.start_time() == 100.0

    def test_numeric_lt_predicate_filters_correctly(self):
        cache = _make_cache(
            [
                {"container_id": 1, "object_id": 10, "frame_ts": 100.0, "distance_m": 5.0},
                {"container_id": 1, "object_id": 10, "frame_ts": 200.0, "distance_m": 12.0},
            ]
        )
        sel = PerceptionSelector("distance_m", "lt", 8.0)
        result = sel.build(cache)
        assert isinstance(result, Intervals)
        assert result.start_time() == 100.0

    def test_no_matching_rows_returns_empty_intervals(self):
        cache = _make_cache(
            [{"container_id": 1, "object_id": 10, "frame_ts": 100.0, "detection_class": "car"}]
        )
        sel = PerceptionSelector("detection_class", "eq", "cyclist")
        result = sel.build(cache)
        assert len(result) == 0

    def test_consecutive_matched_frames_for_same_object_merge_into_one_interval(self):
        cache = _make_cache(
            [
                {"container_id": 1, "object_id": 10, "frame_ts": 100.0, "detection_class": "cyclist"},
                {"container_id": 1, "object_id": 10, "frame_ts": 200.0, "detection_class": "cyclist"},
                {"container_id": 1, "object_id": 10, "frame_ts": 300.0, "detection_class": "cyclist"},
            ]
        )
        sel = PerceptionSelector("detection_class", "eq", "cyclist")
        result = sel.build(cache)
        assert len(result) == 1
        assert result.start_time() == 100.0
        assert result.end_time() >= 300.0

    def test_build_against_non_perception_cache_returns_empty(self):
        sel = PerceptionSelector("detection_class", "eq", "cyclist")
        from mda_query_engine.analyze.query.solvers.empty_cache import (
            EmptyTimeSeriesCache,
        )

        result = sel.build(EmptyTimeSeriesCache())
        assert isinstance(result, Intervals)
        assert len(result) == 0


class TestPerceptionSelectorChannelStub:
    def test_get_selector_expr_never_matches_channel_rows(self):
        # The selector_expr is what the 6-stage channel filter joins against.
        # Returning F.lit(False) is what prevents a PerceptionSelector from
        # accidentally matching a channel_metrics row. Verify it constructs
        # without error when called with an active SparkContext (the only
        # context in which get_selector_expr is invoked in production).
        #
        # Uses configure_spark_with_delta_pip so the JVM is initialized with
        # the Delta catalog jars — required when this test module runs before
        # any Spark-dependent tests that share the same JVM process.
        import pytest

        pytest.importorskip("pyspark")
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
        from pyspark.sql.column import Column

        # getOrCreate() reuses any existing session (e.g. the root conftest's
        # session-scoped spark), so we must not call spark.stop() here — that
        # would destroy the shared session and break downstream tests.
        configure_spark_with_delta_pip(
            SparkSession.builder.master("local[1]")
        ).getOrCreate()
        sel = PerceptionSelector("detection_class", "eq", "cyclist")
        expr = sel.get_selector_expr()
        assert isinstance(expr, Column)
        # The expression renders as a boolean literal — no column name appears
        assert "false" in str(expr).lower() or "literal" in str(expr).lower()

    def test_required_tags_is_empty(self):
        sel = PerceptionSelector("detection_class", "eq", "cyclist")
        assert sel.required_tags() == set()
        assert sel.get_required_tag_exprs() == set()


class TestPerceptionSelectorASTRoundTrip:
    def test_as_dict_and_from_dict_preserve_predicate(self):
        sel = PerceptionSelector("detection_class", "eq", "cyclist")
        obj = sel.as_dict()
        restored = PerceptionSelector.from_dict(obj)
        assert restored.column == "cyclist".__class__("detection_class")  # str
        assert restored.column == "detection_class"
        assert restored.op == "eq"
        assert restored.value == "cyclist"

    def test_as_dict_includes_type_for_polymorphic_resolution(self):
        sel = PerceptionSelector("distance_m", "lt", 8.0)
        obj = sel.as_dict()
        assert "type" in obj
        assert obj["type"].endswith("PerceptionSelector")

    def test_round_trip_via_timeseriesexpression_from_dict(self):
        sel = PerceptionSelector("azimuth", "eq", "front_left")
        restored = TimeSeriesExpression.from_dict(sel.as_dict())
        assert isinstance(restored, PerceptionSelector)
        assert restored.column == "azimuth"
        assert restored.op == "eq"
        assert restored.value == "front_left"

    def test_get_selectors_returns_self(self):
        sel = PerceptionSelector("detection_class", "eq", "cyclist")
        selectors = sel.get_selectors()
        assert selectors == [sel]
