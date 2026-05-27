"""BasicEvent regression — the per-object windowing decision in the
LakeVision layer does not change Core's BasicEvent contract.

Scalar-only ``BasicEvent`` instances continue to write rows whose shape
matches ``EVENT_INSTANCE_FACT_SCHEMA`` exactly, with no extra column and
no side-car involvement. Core consumers (no LakeVision in scope) see the
fact table unchanged from before the LakeVision extension landed.

Requires the Spark + delta-pip toolchain (CI / FEVM workspace).
"""

import pytest


pytestmark = pytest.mark.usefixtures("spark")


class TestCoreFactSchemaUnchanged:
    def test_event_instance_fact_schema_field_names_unchanged(self):
        from mda_reporting.persist.fact_schema import EVENT_INSTANCE_FACT_SCHEMA

        # The schema must continue to be five columns, no object_id on Core.
        assert EVENT_INSTANCE_FACT_SCHEMA.fieldNames() == [
            "container_id",
            "event_instance_id",
            "event_id",
            "start_ts",
            "end_ts",
        ]


class TestBasicEventDoesNotEmitSideCarRows:
    def test_basic_event_type_is_not_perception_event(self):
        """The Core BasicEvent type identifier does not match the LakeVision
        PerceptionEvent type identifier. The fact-table writer routes
        per-event by ``get_event_type_str()``, so a BasicEvent never enters
        the side-car materializer.
        """
        from mda_query_engine.analyze.metadata.time_series_expression import (
            TimeSeriesExpression,
        )
        from mda_reporting.events.basic_event import BasicEvent

        # Build a trivial dummy expression — the test only inspects the
        # event type string, never solves the expression.
        class _DummyExpr(TimeSeriesExpression):
            def __init__(self):
                super().__init__(is_single_signal=False)

            def __str__(self):
                return "_DummyExpr()"

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

        evt = BasicEvent(name="speed_over_30", expr=_DummyExpr())
        assert evt.get_event_type_str() != "PERCEPTION_EVENT"
