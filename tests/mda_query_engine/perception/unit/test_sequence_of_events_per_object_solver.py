"""SequenceOfEvents over track-scoped perception steps — the
PerceptionSolver per-object UDF loop produces one chained window per
``(container_id, object_id)`` where the same object satisfies every step
in temporal order.

Exercises the per-container UDF (``_solve_perception_udf``) directly with
pandas inputs. End-to-end Spark coverage lives in the integration suite.
"""

import numpy as np
import pandas as pd
import pytest

from mda_reporting.events.sequence_of_events import SequenceOfEvents

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


def _lane_change_otp():
    """One object (10) cleanly transitions lane_offset −1 → 0; another (20)
    only ever sits at lane_offset = 1 so it never satisfies step 1. A
    correct track-scoped sequence solver returns exactly one chained
    window from object 10's transition.
    """
    return pd.DataFrame(
        [
            # Object 10 — transitions left lane to ego lane
            {"container_id": 1, "object_id": 10, "frame_ts": 100.0,
             "detection_class": "car", "azimuth": "front_left",
             "distance_m": 6.0, "confidence": 0.9, "source": "lidar",
             "lane_offset": -1, "relative_velocity_ms": 0.0},
            {"container_id": 1, "object_id": 10, "frame_ts": 200.0,
             "detection_class": "car", "azimuth": "front_left",
             "distance_m": 6.0, "confidence": 0.9, "source": "lidar",
             "lane_offset": -1, "relative_velocity_ms": 0.0},
            {"container_id": 1, "object_id": 10, "frame_ts": 300.0,
             "detection_class": "car", "azimuth": "front_left",
             "distance_m": 6.0, "confidence": 0.9, "source": "lidar",
             "lane_offset": 0, "relative_velocity_ms": 0.0},
            {"container_id": 1, "object_id": 10, "frame_ts": 400.0,
             "detection_class": "car", "azimuth": "front_left",
             "distance_m": 6.0, "confidence": 0.9, "source": "lidar",
             "lane_offset": 0, "relative_velocity_ms": 0.0},
            # Object 20 — always in right lane; only satisfies step 2's
            # value (lane_offset == 0) never, and step 1's value never.
            {"container_id": 1, "object_id": 20, "frame_ts": 100.0,
             "detection_class": "car", "azimuth": "front_right",
             "distance_m": 7.0, "confidence": 0.9, "source": "lidar",
             "lane_offset": 1, "relative_velocity_ms": 0.0},
            {"container_id": 1, "object_id": 20, "frame_ts": 200.0,
             "detection_class": "car", "azimuth": "front_right",
             "distance_m": 7.0, "confidence": 0.9, "source": "lidar",
             "lane_offset": 1, "relative_velocity_ms": 0.0},
        ]
    )


class TestTwoStepTrackScopedSequence:
    def test_emits_one_chained_window_keyed_by_object_id(
        self, col_map, empty_channels_pdf
    ):
        scoped = ObjectTrackAccessor()(track_scope=True)
        # Step 1: car with lane_offset == -1 (in left lane)
        # Step 2: same car with lane_offset == 0  (now in ego lane)
        seq = SequenceOfEvents(
            name="lane_change_left",
            expressions=[
                scoped.lane_offset == -1,
                scoped.lane_offset == 0,
            ],
            max_overlap=10_000.0,
        )
        # Apply alias the way Report registration would.
        selection = seq.get_expression()

        otp = _lane_change_otp()
        out_pdf = PerceptionSolver._solve_perception_udf(
            channels_pdf=empty_channels_pdf,
            object_tracks_pdf=otp,
            selections=[selection],
            col_map=col_map,
        )

        windows = out_pdf[seq.get_name()].iloc[0]
        # Only object 10 satisfies the sequence; object 20 never matches step 1.
        assert len(windows) == 1
        # Triple shape: [start, end, object_id]
        assert len(windows[0]) == 3
        assert int(windows[0][2]) == 10


class TestNonScopedTwoStepSequenceFallsBackToPerContainer:
    def test_no_track_scope_emits_pairs(self, col_map, empty_channels_pdf):
        ot = ObjectTrackAccessor()
        seq = SequenceOfEvents(
            name="lane_change_anywhere",
            expressions=[
                ot.lane_offset == -1,
                ot.lane_offset == 0,
            ],
            max_overlap=10_000.0,
        )
        selection = seq.get_expression()

        otp = _lane_change_otp()
        out_pdf = PerceptionSolver._solve_perception_udf(
            channels_pdf=empty_channels_pdf,
            object_tracks_pdf=otp,
            selections=[selection],
            col_map=col_map,
        )

        windows = out_pdf[seq.get_name()].iloc[0]
        # Each window is just [start, end] — no object_id.
        for win in windows:
            assert len(win) == 2
