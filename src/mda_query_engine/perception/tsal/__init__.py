"""TSAL extensions for LakeVision perception predicates.

Perception predicates compose with scalar channel predicates inside one
TimeSeriesExpression. The leaf node is ``PerceptionSelector``; the user-facing
attribute proxy is ``ObjectTrackAccessor``.
"""

from .object_track_accessor import ObjectTrackAccessor
from .perception_selector import PerceptionCache, PerceptionSelector

__all__ = ["ObjectTrackAccessor", "PerceptionCache", "PerceptionSelector"]
