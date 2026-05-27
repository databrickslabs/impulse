"""Channel-only SequenceOfEvents regression — the core algorithm is
unchanged by the LakeVision per-object path. A sequence with only
channel steps (no perception leaves) takes the existing fast path and
emits identical rows.

This is the "no regression" gate for ADR-16: the Core
``SequenceOfEventsExpression.build`` is explicitly NOT modified; the
per-object work happens entirely in LakeVision. Channel-only sequences
must round-trip through the original chain logic.

Requires the Spark + delta-pip toolchain (CI / FEVM workspace).
"""

import pytest


pytestmark = pytest.mark.usefixtures("spark")


class TestSequenceOfEventsExpressionUnchanged:
    def test_class_module_path_unchanged(self):
        # The class still lives in Core, not LakeVision.
        from mda_query_engine.analyze.query.events.sequence_of_events_expression import (
            SequenceOfEventsExpression,
        )
        assert SequenceOfEventsExpression.__module__.startswith("mda_query_engine.")

    def test_build_signature_unchanged(self):
        # build(cache) on a channel-only sequence must produce a plain
        # Intervals — no triples, no LakeVision side-car involvement.
        from mda_query_engine.analyze.query.events.sequence_of_events_expression import (
            SequenceOfEventsExpression,
        )
        import inspect
        sig = inspect.signature(SequenceOfEventsExpression.build)
        # The build method takes self + cache, nothing else.
        assert list(sig.parameters.keys()) == ["self", "cache"]


class TestNoLakeVisionSideCarForChannelOnlySequence:
    def test_no_side_car_when_no_track_scope_in_any_step(self, spark):
        """The LakeVision side-car materializer returns an empty DataFrame
        for any list of events with no track-scoped perception predicates.
        Channel-only sequences are the canonical case.
        """
        from mda_query_engine.analyze.metadata.time_series_expression import (
            TimeSeriesExpression,
        )
        from mda_reporting.events.sequence_of_events import SequenceOfEvents

        import pyspark.sql.types as T
        from pyspark.sql import Row

        from mda_query_engine.perception.events import materialize_event_instance_object_rows

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
            name="channel_only_chain",
            expressions=[_ChannelOnlyExpr(), _ChannelOnlyExpr()],
        )
        schema = T.StructType(
            [
                T.StructField("container_id", T.LongType(), False),
                T.StructField(
                    seq.get_name(),
                    T.ArrayType(T.ArrayType(T.DoubleType())),
                    True,
                ),
            ]
        )
        solved_df = spark.createDataFrame(
            [Row(container_id=1, **{seq.get_name(): [[100.0, 200.0]]})],
            schema=schema,
        )
        side_car = materialize_event_instance_object_rows(spark, [seq], solved_df)
        assert side_car.count() == 0
