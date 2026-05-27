"""Unit tests for mda_query_engine.perception.geometry — pure-Python geometry primitives and
schema-encoding helpers (no Spark)."""

import math

import numpy as np
import pytest

from mda_query_engine.perception.geometry import (
    AZIMUTH_TO_RAD,
    LANE_WIDTH_M,
    azimuth_label_to_xy,
    azimuth_sector,
    ego_yaw_from_global_yaw,
    global_to_ego,
    lane_offset,
    rotation_matrix_from_quat,
    wrap_to_pi,
    yaw_from_quat,
)


# ── rotation_matrix_from_quat ───────────────────────────────────────────────


class TestRotationMatrixFromQuat:
    def test_identity_quaternion_is_identity_matrix(self):
        r = rotation_matrix_from_quat(1.0, 0.0, 0.0, 0.0)
        np.testing.assert_allclose(r, np.eye(3), atol=1e-12)

    def test_90deg_yaw_rotates_x_to_y(self):
        # Rotation by +π/2 around z: x-axis → y-axis.
        c, s = math.cos(math.pi / 4), math.sin(math.pi / 4)
        r = rotation_matrix_from_quat(c, 0.0, 0.0, s)
        x_in_global = r @ np.array([1.0, 0.0, 0.0])
        np.testing.assert_allclose(x_in_global, [0.0, 1.0, 0.0], atol=1e-12)

    def test_orthogonal(self):
        c, s = math.cos(1.234 / 2), math.sin(1.234 / 2)
        r = rotation_matrix_from_quat(c, 0.0, 0.0, s)
        np.testing.assert_allclose(r @ r.T, np.eye(3), atol=1e-12)


# ── global_to_ego ────────────────────────────────────────────────────────────


class TestGlobalToEgo:
    def test_identity_pose_returns_delta(self):
        # Ego at (10, 5, 0) with identity rotation. Global point at (12, 5, 0)
        # → ego frame (2, 0, 0).
        result = global_to_ego(
            point_global=np.array([12.0, 5.0, 0.0]),
            ego_translation=(10.0, 5.0, 0.0),
            ego_rotation_wxyz=(1.0, 0.0, 0.0, 0.0),
        )
        np.testing.assert_allclose(result, [2.0, 0.0, 0.0], atol=1e-12)

    def test_yaw_pi_over_2_swaps_axes_inversely(self):
        # Ego facing global +y (yaw = π/2). Global point ahead of ego at global (0, 1, 0)
        # → in ego frame: x_ego = 1 (forward = global +y), y_ego = 0.
        c, s = math.cos(math.pi / 4), math.sin(math.pi / 4)
        result = global_to_ego(
            point_global=np.array([0.0, 1.0, 0.0]),
            ego_translation=(0.0, 0.0, 0.0),
            ego_rotation_wxyz=(c, 0.0, 0.0, s),
        )
        np.testing.assert_allclose(result, [1.0, 0.0, 0.0], atol=1e-12)


# ── yaw_from_quat ────────────────────────────────────────────────────────────


class TestYawFromQuat:
    def test_identity_returns_zero(self):
        assert yaw_from_quat(1.0, 0.0, 0.0, 0.0) == pytest.approx(0.0, abs=1e-12)

    def test_z_axis_rotation_recovers_yaw(self):
        for angle in (math.pi / 6, math.pi / 3, math.pi / 2, -math.pi / 4, math.pi - 0.1):
            c, s = math.cos(angle / 2), math.sin(angle / 2)
            assert yaw_from_quat(c, 0.0, 0.0, s) == pytest.approx(angle, abs=1e-12)


# ── wrap_to_pi ───────────────────────────────────────────────────────────────


class TestWrapToPi:
    def test_inside_range_unchanged(self):
        assert wrap_to_pi(0.5) == pytest.approx(0.5)
        assert wrap_to_pi(-0.5) == pytest.approx(-0.5)

    def test_above_pi_wraps_negative(self):
        assert wrap_to_pi(3 * math.pi / 2) == pytest.approx(-math.pi / 2, abs=1e-12)

    def test_below_neg_pi_wraps_positive(self):
        assert wrap_to_pi(-3 * math.pi / 2) == pytest.approx(math.pi / 2, abs=1e-12)


# ── ego_yaw_from_global_yaw ──────────────────────────────────────────────────


class TestEgoYawFromGlobalYaw:
    def test_identity_ego_returns_global_yaw(self):
        assert ego_yaw_from_global_yaw(math.pi / 4, (1.0, 0.0, 0.0, 0.0)) == pytest.approx(
            math.pi / 4, abs=1e-12
        )

    def test_wraps_to_negative_pi_pi(self):
        # global_yaw = 3π/2, ego_yaw = 0 → relative = 3π/2 → wrap to -π/2.
        rel = ego_yaw_from_global_yaw(3 * math.pi / 2, (1.0, 0.0, 0.0, 0.0))
        assert rel == pytest.approx(-math.pi / 2, abs=1e-12)

    def test_ego_yaw_subtracted(self):
        # Ego facing global +y (yaw = π/2). Object also facing +y → relative yaw = 0.
        c, s = math.cos(math.pi / 4), math.sin(math.pi / 4)
        assert ego_yaw_from_global_yaw(math.pi / 2, (c, 0.0, 0.0, s)) == pytest.approx(
            0.0, abs=1e-12
        )


# ── azimuth_sector ───────────────────────────────────────────────────────────


class TestAzimuthSector:
    @pytest.mark.parametrize("x,y,expected", [
        (1.0, 0.0, "front"),
        (0.0, 1.0, "left"),
        (-1.0, 0.0, "rear"),
        (0.0, -1.0, "right"),
        (1.0, 1.0, "front_left"),
        (-1.0, 1.0, "rear_left"),
        (-1.0, -1.0, "rear_right"),
        (1.0, -1.0, "front_right"),
    ])
    def test_cardinal_and_diagonal_sectors(self, x, y, expected):
        assert azimuth_sector(x, y) == expected

    @pytest.mark.parametrize("boundary_deg,below_label,above_label", [
        (22.5, "front", "front_left"),
        (67.5, "front_left", "left"),
        (112.5, "left", "rear_left"),
        (-22.5, "front_right", "front"),
        (-67.5, "right", "front_right"),
        (-112.5, "rear_right", "right"),
    ])
    def test_each_sector_boundary(self, boundary_deg, below_label, above_label):
        # 0.1° on either side of every boundary, checking the expected sector
        # both above and below.
        below = math.radians(boundary_deg - 0.1)
        above = math.radians(boundary_deg + 0.1)
        assert azimuth_sector(math.cos(below), math.sin(below)) == below_label
        assert azimuth_sector(math.cos(above), math.sin(above)) == above_label

    def test_rear_boundary_at_180(self):
        # Just beyond 157.5° on either side → rear.
        assert azimuth_sector(math.cos(math.radians(160)), math.sin(math.radians(160))) == "rear"
        assert azimuth_sector(math.cos(math.radians(-160)), math.sin(math.radians(-160))) == "rear"


# ── lane_offset ──────────────────────────────────────────────────────────────


class TestLaneOffset:
    def test_same_lane(self):
        assert lane_offset(0.0) == 0
        assert lane_offset(LANE_WIDTH_M / 2 - 0.1) == 0
        assert lane_offset(-LANE_WIDTH_M / 2 + 0.1) == 0

    def test_left_lane(self):
        assert lane_offset(LANE_WIDTH_M) == 1
        assert lane_offset(LANE_WIDTH_M * 2) == 2

    def test_right_lane(self):
        assert lane_offset(-LANE_WIDTH_M) == -1
        assert lane_offset(-LANE_WIDTH_M * 2) == -2

    def test_clamped_to_two_lanes_each_side(self):
        assert lane_offset(LANE_WIDTH_M * 100) == 2
        assert lane_offset(-LANE_WIDTH_M * 100) == -2

    def test_custom_lane_width(self):
        # With a 4 m lane, 4 m lateral → offset = 1, not 1 (with default 3.5 it'd still round to 1).
        # Use 5 m lateral to distinguish: 5 / 3.5 = 1.43 → 1; 5 / 4.0 = 1.25 → 1; both 1.
        # Use 7 m lateral: 7 / 3.5 = 2 → clamped 2; 7 / 4.0 = 1.75 → 2.
        # Use 6 m lateral: 6 / 3.5 = 1.71 → 2; 6 / 4.0 = 1.5 → 2. Hmm — Python round() banks even.
        # Pick a value that clearly differs: with width=10, 4 m → 0 instead of 1.
        assert lane_offset(4.0, lane_width_m=10.0) == 0
        assert lane_offset(4.0, lane_width_m=3.5) == 1


# ── AZIMUTH_TO_RAD and azimuth_label_to_xy ──────────────────────────────────


class TestAzimuthTable:
    def test_all_eight_labels_present(self):
        expected = {
            "front", "front_left", "left", "rear_left",
            "rear", "rear_right", "right", "front_right",
        }
        assert set(AZIMUTH_TO_RAD) == expected

    def test_front_is_zero_rad(self):
        assert AZIMUTH_TO_RAD["front"] == 0.0

    def test_rear_is_pi(self):
        assert AZIMUTH_TO_RAD["rear"] == pytest.approx(math.pi)

    def test_left_is_plus_pi_over_two(self):
        assert AZIMUTH_TO_RAD["left"] == pytest.approx(math.pi / 2)

    def test_right_is_minus_pi_over_two(self):
        assert AZIMUTH_TO_RAD["right"] == pytest.approx(-math.pi / 2)


class TestAzimuthLabelToXy:
    def test_front_projects_along_x(self):
        x, y = azimuth_label_to_xy(10.0, "front")
        assert x == pytest.approx(10.0)
        assert y == pytest.approx(0.0, abs=1e-9)

    def test_left_projects_along_y(self):
        x, y = azimuth_label_to_xy(10.0, "left")
        assert x == pytest.approx(0.0, abs=1e-9)
        assert y == pytest.approx(10.0)

    def test_right_projects_along_negative_y(self):
        x, y = azimuth_label_to_xy(10.0, "right")
        assert x == pytest.approx(0.0, abs=1e-9)
        assert y == pytest.approx(-10.0)

    def test_rear_projects_along_negative_x(self):
        x, y = azimuth_label_to_xy(10.0, "rear")
        assert x == pytest.approx(-10.0)
        assert y == pytest.approx(0.0, abs=1e-12)

    def test_front_left_is_45deg(self):
        x, y = azimuth_label_to_xy(math.sqrt(2.0), "front_left")
        assert x == pytest.approx(1.0)
        assert y == pytest.approx(1.0)

    def test_unknown_label_falls_back_to_front(self):
        x, y = azimuth_label_to_xy(5.0, "not_a_real_label")
        assert x == pytest.approx(5.0)
        assert y == pytest.approx(0.0, abs=1e-9)

    def test_zero_distance_is_origin_for_every_label(self):
        for label in AZIMUTH_TO_RAD:
            x, y = azimuth_label_to_xy(0.0, label)
            assert x == pytest.approx(0.0, abs=1e-12)
            assert y == pytest.approx(0.0, abs=1e-12)
