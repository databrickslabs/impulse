"""Per-container UDF emits one window per (container, object_id) when any
selector in the predicate tree is track-scoped, and falls back to merged
per-container windows otherwise.

Exercises ``_solve_perception_udf`` directly with synthetic pandas inputs —
no Spark session required. End-to-end Spark coverage lives in the
integration suite.
"""

import numpy as np
import pandas as pd
import pytest

from mda_query_engine.perception.events.perception_event import PerceptionEvent
from mda_query_engine.perception.query.perception_solver import PerceptionSolver
from mda_query_engine.perception.tsal.object_track_accessor import ObjectTrackAccessor


@pytest.fixture
def col_map():
    return {
        "cid": "container_id",
        "ch": "channel_id",
        "ts": "tstart",
        "te": "tend",
        "val": "value",
    }


@pytest.fixture
def empty_channels_pdf(col_map):
    return pd.DataFrame(
        {
            col_map["cid"]: pd.Series(dtype=np.int64),
            col_map["ch"]: pd.Series(dtype=np.int64),
            col_map["ts"]: pd.Series(dtype=np.float64),
            col_map["te"]: pd.Series(dtype=np.float64),
            col_map["val"]: pd.Series(dtype=np.float64),
        }
    )


@pytest.fixture
def two_object_tracks_pdf():
    """Two cyclists in non-overlapping time ranges. Per-container windowing
    collapses to two intervals; per-object windowing yields one interval
    per object with the object_id attached.
    """
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


class TestPerObjectWindowing:
    def test_track_scoped_predicate_emits_triples_with_object_id(
        self, col_map, empty_channels_pdf, two_object_tracks_pdf
    ):
        scoped = ObjectTrackAccessor()(track_scope=True)
        selection = scoped.detection_class("cyclist").alias("cyclist_per_obj")

        out_pdf = PerceptionSolver._solve_perception_udf(
            channels_pdf=empty_channels_pdf,
            object_tracks_pdf=two_object_tracks_pdf,
            selections=[selection],
            col_map=col_map,
        )

        assert len(out_pdf) == 1
        windows = out_pdf["cyclist_per_obj"].iloc[0]
        # Two objects → two windows. Each is a [start, end, object_id] triple.
        assert len(windows) == 2
        for win in windows:
            assert len(win) == 3
        object_ids = sorted(int(w[2]) for w in windows)
        assert object_ids == [10, 20]

    def test_non_scoped_predicate_emits_pairs_only(
        self, col_map, empty_channels_pdf, two_object_tracks_pdf
    ):
        ot = ObjectTrackAccessor()
        selection = ot.detection_class("cyclist").alias("cyclist_anywhere")

        out_pdf = PerceptionSolver._solve_perception_udf(
            channels_pdf=empty_channels_pdf,
            object_tracks_pdf=two_object_tracks_pdf,
            selections=[selection],
            col_map=col_map,
        )

        windows = out_pdf["cyclist_anywhere"].iloc[0]
        # Per-container merging produces two intervals; each is a [start, end] pair.
        assert len(windows) == 2
        for win in windows:
            assert len(win) == 2


class TestMixedSelectionTypes:
    def test_track_scoped_and_non_scoped_selections_coexist_in_one_udf_call(
        self, col_map, empty_channels_pdf, two_object_tracks_pdf
    ):
        ot = ObjectTrackAccessor()
        scoped = ot(track_scope=True)
        scoped_sel = scoped.detection_class("cyclist").alias("scoped")
        unscoped_sel = ot.detection_class("cyclist").alias("unscoped")

        out_pdf = PerceptionSolver._solve_perception_udf(
            channels_pdf=empty_channels_pdf,
            object_tracks_pdf=two_object_tracks_pdf,
            selections=[scoped_sel, unscoped_sel],
            col_map=col_map,
        )

        scoped_windows = out_pdf["scoped"].iloc[0]
        unscoped_windows = out_pdf["unscoped"].iloc[0]
        assert all(len(w) == 3 for w in scoped_windows)
        assert all(len(w) == 2 for w in unscoped_windows)


class TestEmptyInputs:
    def test_empty_object_tracks_emits_no_track_scoped_rows(
        self, col_map, empty_channels_pdf
    ):
        scoped = ObjectTrackAccessor()(track_scope=True)
        selection = scoped.detection_class("cyclist").alias("scoped")

        empty_otp = pd.DataFrame(
            columns=[
                "container_id", "object_id", "frame_ts",
                "detection_class", "azimuth", "distance_m",
                "confidence", "source",
            ]
        )
        # Both surfaces empty → UDF returns empty DataFrame (no container_id known).
        out_pdf = PerceptionSolver._solve_perception_udf(
            channels_pdf=empty_channels_pdf,
            object_tracks_pdf=empty_otp,
            selections=[selection],
            col_map=col_map,
        )
        assert len(out_pdf) == 0
