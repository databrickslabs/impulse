"""Per-object windowing flag on PerceptionSelector — flag propagation,
serialization round-trip, and per-object interval formation in build().
"""

import numpy as np
import pandas as pd
import pytest

from mda_query_engine.model.series.intervals import Intervals

from mda_query_engine.perception.tsal.object_track_accessor import ObjectTrackAccessor
from mda_query_engine.perception.tsal.perception_selector import (
    PerceptionCache,
    PerceptionSelector,
    is_track_scoped,
    perception_selectors_track_scope_values,
)


@pytest.fixture
def ot():
    return ObjectTrackAccessor()


@pytest.fixture
def ot_scoped():
    return ObjectTrackAccessor()(track_scope=True)


def _empty_channels_pdf():
    return pd.DataFrame(
        {
            "container_id": pd.Series([], dtype=np.int64),
            "channel_id": pd.Series([], dtype=np.int64),
            "tstart": pd.Series([], dtype=np.float64),
            "tend": pd.Series([], dtype=np.float64),
            "value": pd.Series([], dtype=np.float64),
        }
    )


COL_MAP = {"cid": "container_id", "ch": "channel_id", "ts": "tstart",
           "te": "tend", "val": "value"}


def _two_object_otp():
    return pd.DataFrame(
        [
            {"container_id": 1, "object_id": 10, "frame_ts": 100.0,
             "detection_class": "cyclist", "azimuth": "front_left",
             "distance_m": 6.0, "confidence": 0.9, "source": "lidar"},
            {"container_id": 1, "object_id": 10, "frame_ts": 200.0,
             "detection_class": "cyclist", "azimuth": "front_left",
             "distance_m": 6.0, "confidence": 0.9, "source": "lidar"},
            {"container_id": 1, "object_id": 20, "frame_ts": 600.0,
             "detection_class": "cyclist", "azimuth": "front_left",
             "distance_m": 7.0, "confidence": 0.85, "source": "lidar"},
            {"container_id": 1, "object_id": 20, "frame_ts": 700.0,
             "detection_class": "cyclist", "azimuth": "front_left",
             "distance_m": 7.0, "confidence": 0.85, "source": "lidar"},
        ]
    )


def _two_object_cache():
    return PerceptionCache(
        channels_pdf=_empty_channels_pdf(),
        col_map=COL_MAP,
        object_tracks_pdf=_two_object_otp(),
        container_bounds=(0.0, 1000.0),
    )


class TestTrackScopeFlagPropagation:
    def test_default_accessor_produces_non_scoped_selectors(self, ot):
        sel = ot.detection_class("cyclist")
        assert sel.track_scope is False

    def test_scoped_accessor_produces_scoped_selectors(self, ot_scoped):
        sel = ot_scoped.detection_class("cyclist")
        assert sel.track_scope is True

    def test_scoped_accessor_numeric_columns_carry_flag(self, ot_scoped):
        sel = ot_scoped.distance_m < 8.0
        assert isinstance(sel, PerceptionSelector)
        assert sel.track_scope is True

    def test_scoped_accessor_source_contains_carries_flag(self, ot_scoped):
        sel = ot_scoped.source_contains("lidar")
        assert sel.track_scope is True

    def test_scoped_call_produces_new_accessor_not_mutating(self, ot):
        scoped = ot(track_scope=True)
        assert scoped is not ot
        assert ot.detection_class("cyclist").track_scope is False
        assert scoped.detection_class("cyclist").track_scope is True


class TestSerializationRoundTrip:
    def test_round_trip_preserves_track_scope_true(self, ot_scoped):
        sel = ot_scoped.detection_class("cyclist")
        d = sel.as_dict()
        assert d["track_scope"] is True
        restored = PerceptionSelector.from_dict(d)
        assert restored.track_scope is True
        assert restored.column == sel.column

    def test_round_trip_preserves_track_scope_false(self, ot):
        sel = ot.detection_class("cyclist")
        d = sel.as_dict()
        assert d["track_scope"] is False
        restored = PerceptionSelector.from_dict(d)
        assert restored.track_scope is False

    def test_missing_key_in_legacy_dict_defaults_to_false(self):
        # AST dicts emitted before this flag existed should still deserialize.
        sel = PerceptionSelector.from_dict(
            {"column": "detection_class", "op": "eq", "value": "cyclist"}
        )
        assert sel.track_scope is False


class TestExpressionIntrospection:
    def test_is_track_scoped_true_for_scoped_expression(self, ot_scoped):
        expr = ot_scoped.detection_class("cyclist") & ot_scoped.azimuth("front_left")
        assert is_track_scoped(expr) is True

    def test_is_track_scoped_false_for_unscoped_expression(self, ot):
        expr = ot.detection_class("cyclist") & ot.azimuth("front_left")
        assert is_track_scoped(expr) is False

    def test_track_scope_values_signal_mixed_predicate(self, ot, ot_scoped):
        mixed = ot_scoped.detection_class("cyclist") & ot.azimuth("front_left")
        assert perception_selectors_track_scope_values(mixed) == {True, False}

    def test_track_scope_values_consistent_predicate_yields_singleton(self, ot_scoped):
        scoped = ot_scoped.detection_class("cyclist") & ot_scoped.azimuth("front_left")
        assert perception_selectors_track_scope_values(scoped) == {True}


class TestBuildRespectsCachePartitioning:
    """build() itself is unchanged — per-object semantics are produced by
    the solver iterating over object_id-filtered caches. These tests pin
    the contract that build() on a single-object cache yields one
    contiguous Intervals per matching object.
    """

    def test_build_on_single_object_cache_yields_one_interval(self, ot_scoped):
        otp = _two_object_otp()
        otp_single = otp[otp["object_id"] == 10].reset_index(drop=True)
        single_cache = PerceptionCache(
            channels_pdf=_empty_channels_pdf(),
            col_map=COL_MAP,
            object_tracks_pdf=otp_single,
            container_bounds=(0.0, 1000.0),
        )
        result = (
            ot_scoped.detection_class("cyclist").alias("c").build(single_cache)
        )
        assert isinstance(result, Intervals)
        assert len(result) == 1
        assert result.starts()[0] == 100.0

    def test_build_on_full_cache_with_two_objects_merges_per_container(self, ot_scoped):
        cache = _two_object_cache()
        # build() with a two-object cache produces one Intervals object —
        # the per-object structure is only visible when the solver loops
        # per object_id.
        result = (
            ot_scoped.detection_class("cyclist").alias("c").build(cache)
        )
        assert isinstance(result, Intervals)
        # Two non-overlapping per-object intervals appear as two intervals
        # in the merged result; the solver's per-object iteration is what
        # threads object_id alongside each.
        assert len(result) == 2
