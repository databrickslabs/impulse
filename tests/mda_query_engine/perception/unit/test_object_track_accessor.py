"""ObjectTrackAccessor — each call returns a PerceptionSelector, and the
predicate composition operators (`&`, `|`, `~`) build the expected AST.
"""

import operator

import pytest

from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesOp,
)
from mda_query_engine.perception.tsal.object_track_accessor import ObjectTrackAccessor
from mda_query_engine.perception.tsal.perception_selector import PerceptionSelector


@pytest.fixture
def ot():
    return ObjectTrackAccessor()


class TestStringColumnAccessors:
    def test_detection_class_call_returns_eq_selector(self, ot):
        sel = ot.detection_class("cyclist")
        assert isinstance(sel, PerceptionSelector)
        assert sel.column == "detection_class"
        assert sel.op == "eq"
        assert sel.value == "cyclist"

    def test_azimuth_call_returns_eq_selector(self, ot):
        sel = ot.azimuth("front_left")
        assert isinstance(sel, PerceptionSelector)
        assert sel.column == "azimuth"
        assert sel.op == "eq"
        assert sel.value == "front_left"

    def test_source_contains_returns_contains_selector(self, ot):
        sel = ot.source_contains("lidar")
        assert isinstance(sel, PerceptionSelector)
        assert sel.column == "source"
        assert sel.op == "contains"
        assert sel.value == "lidar"


class TestNumericColumnAccessors:
    def test_distance_m_lt_returns_lt_selector(self, ot):
        sel = ot.distance_m < 8.0
        assert isinstance(sel, PerceptionSelector)
        assert sel.column == "distance_m"
        assert sel.op == "lt"
        assert sel.value == 8.0

    def test_confidence_gt_returns_gt_selector(self, ot):
        sel = ot.confidence > 0.7
        assert isinstance(sel, PerceptionSelector)
        assert sel.column == "confidence"
        assert sel.op == "gt"
        assert sel.value == 0.7

    def test_relative_velocity_le_returns_le_selector(self, ot):
        sel = ot.relative_velocity_ms <= -2.0
        assert sel.column == "relative_velocity_ms"
        assert sel.op == "le"


class TestUnknownAttributeRaises:
    def test_unknown_column_raises_attribute_error(self, ot):
        with pytest.raises(AttributeError, match="no column or helper"):
            ot.totally_not_a_column


class TestPredicateComposition:
    def test_and_builds_time_series_op_with_operator_and(self, ot):
        compound = ot.detection_class("cyclist") & ot.azimuth("front_left")
        assert isinstance(compound, TimeSeriesOp)
        assert compound.operation is operator.and_

    def test_or_builds_time_series_op_with_operator_or(self, ot):
        compound = ot.detection_class("cyclist") | ot.detection_class("pedestrian")
        assert isinstance(compound, TimeSeriesOp)
        assert compound.operation is operator.or_

    def test_compound_predicate_collects_both_perception_leaves(self, ot):
        compound = ot.detection_class("cyclist") & ot.azimuth("front_left")
        leaves = compound.get_selectors()
        assert len(leaves) == 2
        assert all(isinstance(s, PerceptionSelector) for s in leaves)
        columns = sorted(s.column for s in leaves)
        assert columns == ["azimuth", "detection_class"]
