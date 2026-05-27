"""Geometry primitives and schema-encoding helpers for LakeVision.

Two kinds of function live here:

1. Pure geometry — quaternion-to-rotation-matrix, global-to-ego transform, yaw
   extraction, angle wrap. Used by any dataset adapter that has to project
   global-frame annotations into the vehicle frame.

2. Schema-encoding helpers — `azimuth_sector` and `lane_offset` produce the
   exact enum values stored in `object_tracks.azimuth` and `object_tracks.lane_offset`.
   Every dataset adapter writes these columns and must encode them consistently,
   so the encoding lives in core rather than being reinvented per adapter.

Frame convention used throughout: x=forward, y=left, z=up. This matches the
LakeVision vehicle-frame definition shared by `object_tracks`, `lidar_object_detections`,
`free_space`, `predicted_trajectories`, and the OpenLABEL `vehicle-iso8855` export.
"""

from __future__ import annotations

import math

import numpy as np


# Standard urban lane width in meters. Used by `lane_offset` to discretize the
# ego-frame lateral offset of a tracked object into a lane index.
LANE_WIDTH_M = 3.5


# ── Pure geometry ────────────────────────────────────────────────────────────


def rotation_matrix_from_quat(qw: float, qx: float, qy: float, qz: float) -> np.ndarray:
    """Rotation matrix that maps vectors expressed in ego frame to global frame.

    For the inverse (global → ego), use ``R.T`` — rotation matrices are orthogonal.
    Input quaternion order is (w, x, y, z), unit-norm assumed.
    """
    return np.array([
        [1 - 2 * (qy * qy + qz * qz), 2 * (qx * qy - qz * qw),     2 * (qx * qz + qy * qw)],
        [2 * (qx * qy + qz * qw),     1 - 2 * (qx * qx + qz * qz), 2 * (qy * qz - qx * qw)],
        [2 * (qx * qz - qy * qw),     2 * (qy * qz + qx * qw),     1 - 2 * (qx * qx + qy * qy)],
    ])


def global_to_ego(
    point_global: np.ndarray,
    ego_translation: tuple[float, float, float] | np.ndarray,
    ego_rotation_wxyz: tuple[float, float, float, float],
) -> np.ndarray:
    """Transform a 3D point from global frame to ego (vehicle) frame.

    Vehicle frame convention: x=forward, y=left, z=up.
    """
    r_ego_to_global = rotation_matrix_from_quat(*ego_rotation_wxyz)
    delta = np.asarray(point_global) - np.asarray(ego_translation)
    return r_ego_to_global.T @ delta


def yaw_from_quat(qw: float, qx: float, qy: float, qz: float) -> float:
    """Z-axis (yaw) angle in radians from a unit quaternion in (w, x, y, z) order."""
    siny_cosp = 2 * (qw * qz + qx * qy)
    cosy_cosp = 1 - 2 * (qy * qy + qz * qz)
    return math.atan2(siny_cosp, cosy_cosp)


def wrap_to_pi(angle_rad: float) -> float:
    """Wrap an angle in radians into the half-open interval [-π, π]."""
    while angle_rad > math.pi:
        angle_rad -= 2 * math.pi
    while angle_rad < -math.pi:
        angle_rad += 2 * math.pi
    return angle_rad


def ego_yaw_from_global_yaw(
    global_yaw_rad: float,
    ego_rotation_wxyz: tuple[float, float, float, float],
) -> float:
    """Yaw of an object relative to ego heading, wrapped to [-π, π]."""
    ego_yaw = yaw_from_quat(*ego_rotation_wxyz)
    return wrap_to_pi(global_yaw_rad - ego_yaw)


# ── Schema-encoding helpers ──────────────────────────────────────────────────


def azimuth_sector(forward_x: float, left_y: float) -> str:
    """Map (forward_x, left_y) ego-frame position to the 8-sector enum used by
    ``object_tracks.azimuth``.

    Conventions:
      forward_x > 0 ⇒ in front of ego
      left_y    > 0 ⇒ to the left of ego
      angle measured counter-clockwise from forward (positive x-axis)

    Sector boundaries are at multiples of 45° with ±22.5° half-width:
        [-22.5°,   22.5°]  → "front"
        ( 22.5°,   67.5°]  → "front_left"
        ( 67.5°,  112.5°]  → "left"
        (112.5°,  157.5°]  → "rear_left"
        (157.5°, 180°] ∪ [-180°, -157.5°) → "rear"
        [-157.5°, -112.5°) → "rear_right"
        [-112.5°,  -67.5°) → "right"
        [ -67.5°,  -22.5°) → "front_right"
    """
    deg = math.degrees(math.atan2(left_y, forward_x))
    if -22.5 <= deg <= 22.5:
        return "front"
    if 22.5 < deg <= 67.5:
        return "front_left"
    if 67.5 < deg <= 112.5:
        return "left"
    if 112.5 < deg <= 157.5:
        return "rear_left"
    if abs(deg) > 157.5:
        return "rear"
    if -157.5 <= deg < -112.5:
        return "rear_right"
    if -112.5 <= deg < -67.5:
        return "right"
    if -67.5 <= deg < -22.5:
        return "front_right"
    return "unknown"


def lane_offset(left_y: float, lane_width_m: float = LANE_WIDTH_M) -> int:
    """Lane index relative to ego (0 = same lane, +1 = left lane, -1 = right). Clamped to [-2, +2]."""
    offset = round(left_y / lane_width_m)
    return max(-2, min(2, int(offset)))


# ── Azimuth label → ego-frame position ───────────────────────────────────────


AZIMUTH_TO_RAD: dict[str, float] = {
    "front":       0.0,
    "front_left":  math.radians(45),
    "left":        math.radians(90),
    "rear_left":   math.radians(135),
    "rear":        math.pi,
    "rear_right":  math.radians(-135),
    "right":       math.radians(-90),
    "front_right": math.radians(-45),
}


def azimuth_label_to_xy(distance_m: float, azimuth_label: str) -> tuple[float, float]:
    """Project an `(distance_m, azimuth_label)` pair onto ego-frame (x, y).

    Inverse of `azimuth_sector` (which discretizes (x, y) → label): given a
    label and a radial distance, returns the (forward_x, left_y) coordinates
    at the center of that azimuth sector. Used for top-down overlays where
    an `object_tracks` row needs to be placed on a LiDAR scatter.
    """
    angle = AZIMUTH_TO_RAD.get(azimuth_label, 0.0)
    return distance_m * math.cos(angle), distance_m * math.sin(angle)
