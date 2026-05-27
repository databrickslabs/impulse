"""SequenceOfEvents track-scope agreement validation.

A sequence whose perception steps disagree on ``track_scope`` is
ambiguous — the chain solver can't simultaneously enforce per-object
identity (which requires per-object cache iteration) and per-container
collapsing. The LakeVision-side validator catches this before
registration. Channel-only sequences and consistent sequences pass.
"""

import pytest

from mda_reporting.events.sequence_of_events import SequenceOfEvents

from mda_query_engine.perception.events import validate_sequence_of_events_track_scope
from mda_query_engine.perception.tsal.object_track_accessor import ObjectTrackAccessor


@pytest.fixture
def ot():
    return ObjectTrackAccessor()


@pytest.fixture
def ot_scoped():
    return ObjectTrackAccessor()(track_scope=True)


class TestMixedTrackScopeStepsRaise:
    def test_two_perception_steps_one_scoped_one_unscoped_raises(self, ot, ot_scoped):
        seq = SequenceOfEvents(
            name="mixed_steps",
            expressions=[
                ot_scoped.detection_class("cyclist"),
                ot.detection_class("pedestrian"),
            ],
        )
        with pytest.raises(ValueError, match="track_scope"):
            validate_sequence_of_events_track_scope(seq)

    def test_step_internally_mixed_raises(self, ot, ot_scoped):
        # A single step whose AND tree mixes scoped + unscoped is itself
        # ambiguous and must raise.
        seq = SequenceOfEvents(
            name="step_mixed",
            expressions=[
                ot_scoped.detection_class("cyclist") & ot.azimuth("front_left"),
            ],
        )
        with pytest.raises(ValueError, match="track-scoped"):
            validate_sequence_of_events_track_scope(seq)


class TestConsistentSequencesPass:
    def test_all_scoped_steps_pass(self, ot_scoped):
        seq = SequenceOfEvents(
            name="all_scoped",
            expressions=[
                ot_scoped.detection_class("cyclist"),
                ot_scoped.detection_class("cyclist") & (ot_scoped.distance_m < 8.0),
            ],
            max_overlap=2_000_000.0,
        )
        validate_sequence_of_events_track_scope(seq)  # no raise

    def test_all_unscoped_steps_pass(self, ot):
        seq = SequenceOfEvents(
            name="all_unscoped",
            expressions=[
                ot.detection_class("cyclist"),
                ot.detection_class("pedestrian"),
            ],
        )
        validate_sequence_of_events_track_scope(seq)  # no raise


class TestChannelOnlySequencesUnaffected:
    def test_no_perception_steps_no_validation_concern(self):
        # Build a sequence whose steps are all non-perception expressions.
        # We need a TimeSeriesExpression that has no perception selectors.
        from mda_query_engine.analyze.metadata.time_series_expression import (
            TimeSeriesExpression,
        )

        class _ChannelOnlyExpr(TimeSeriesExpression):
            def __init__(self):
                super().__init__(is_single_signal=False)

            def __str__(self):
                return "_ChannelOnlyExpr()"

            def get_required_tag_exprs(self):
                return set()

            def required_tags(self):
                return set()

            def get_selectors(self):
                return []

            def get_selector_expr(self):
                return None

            def build(self, cache):
                return None

            def dtype(self):
                return None

        seq = SequenceOfEvents(
            name="channel_only",
            expressions=[_ChannelOnlyExpr(), _ChannelOnlyExpr()],
        )
        validate_sequence_of_events_track_scope(seq)  # no raise


class TestMixedChannelAndConsistentPerceptionSteps:
    def test_channel_plus_all_scoped_perception_steps_pass(self, ot_scoped):
        # Channel-only steps in a mixed sequence have no track_scope
        # constraint; consistent perception steps pass.
        from mda_query_engine.analyze.metadata.time_series_expression import (
            TimeSeriesExpression,
        )

        class _ChannelOnlyExpr(TimeSeriesExpression):
            def __init__(self):
                super().__init__(is_single_signal=False)

            def __str__(self):
                return "_ChannelOnlyExpr()"

            def get_required_tag_exprs(self):
                return set()

            def required_tags(self):
                return set()

            def get_selectors(self):
                return []

            def get_selector_expr(self):
                return None

            def build(self, cache):
                return None

            def dtype(self):
                return None

        seq = SequenceOfEvents(
            name="mixed_channel_and_scoped_perception",
            expressions=[
                _ChannelOnlyExpr(),
                ot_scoped.detection_class("cyclist"),
            ],
        )
        validate_sequence_of_events_track_scope(seq)  # no raise
