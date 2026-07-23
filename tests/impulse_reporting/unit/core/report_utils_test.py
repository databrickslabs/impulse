"""Unit tests for report_utils helper functions."""

from enum import Enum
from unittest.mock import MagicMock, create_autospec, patch

import pyspark.sql.functions as F
import pytest
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame

from impulse_reporting.core.report import Report
from impulse_reporting.core.report_utils import (
    build_batches,
    build_metadata_dfs,
    dispatch_events,
    group_selectables_by_type,
    merge_changed_unchanged,
    persist_dimensions_full,
    persist_dimensions_incremental,
    persist_facts_full,
    persist_facts_incremental,
    split_by_hash_change,
)


class TestBuildBatches:
    """Tests for the selector-aware best-fit-decreasing bin packing."""

    def test_empty_expressions(self):
        """Empty input returns empty output."""
        assert build_batches([], batch_size=10) == []

    def test_single_expression_fits(self):
        """Single expression always fits in one batch."""
        expr = MagicMock()
        sel = MagicMock()
        expr.get_selectors.return_value = [sel]

        result = build_batches([expr], batch_size=1)
        assert len(result) == 1
        assert result[0] == [expr]

    def test_all_fit_single_batch(self):
        """All expressions fit in one batch when total selectors <= batch_size."""
        sel_a = MagicMock()
        sel_b = MagicMock()

        expr1 = MagicMock()
        expr1.get_selectors.return_value = [sel_a]

        expr2 = MagicMock()
        expr2.get_selectors.return_value = [sel_b]

        result = build_batches([expr1, expr2], batch_size=2)
        assert len(result) == 1
        assert set(result[0]) == {expr1, expr2}

    def test_shared_selectors_grouped(self):
        """Expressions sharing selectors are grouped together."""
        shared_sel = MagicMock()

        expr1 = MagicMock()
        expr1.get_selectors.return_value = [shared_sel]

        expr2 = MagicMock()
        expr2.get_selectors.return_value = [shared_sel]

        result = build_batches([expr1, expr2], batch_size=1)
        # Both share the same selector, so they fit in one batch
        assert len(result) == 1
        assert set(result[0]) == {expr1, expr2}

    def test_split_into_multiple_batches(self):
        """Expressions with distinct selectors are split when they exceed batch_size."""
        sel_a = MagicMock()
        sel_b = MagicMock()
        sel_c = MagicMock()

        expr1 = MagicMock()
        expr1.get_selectors.return_value = [sel_a]

        expr2 = MagicMock()
        expr2.get_selectors.return_value = [sel_b]

        expr3 = MagicMock()
        expr3.get_selectors.return_value = [sel_c]

        result = build_batches([expr1, expr2, expr3], batch_size=1)
        # Each has a unique selector, batch_size=1 → 3 batches
        assert len(result) == 3

    def test_overlap_preference(self):
        """Expressions with overlapping selectors are placed in same batch."""
        sel_a = MagicMock()
        sel_b = MagicMock()

        expr1 = MagicMock()
        expr1.get_selectors.return_value = [sel_a, sel_b]

        expr2 = MagicMock()
        expr2.get_selectors.return_value = [sel_a]

        expr3 = MagicMock()
        expr3.get_selectors.return_value = [sel_b]

        result = build_batches([expr1, expr2, expr3], batch_size=2)
        # All share selectors through expr1, and batch_size=2 can hold {sel_a, sel_b}
        assert len(result) == 1

    def test_batch_size_respected(self):
        """No batch exceeds the selector limit."""
        selectors = [MagicMock() for _ in range(10)]
        expressions = []
        for sel in selectors:
            expr = MagicMock()
            expr.get_selectors.return_value = [sel]
            expressions.append(expr)

        result = build_batches(expressions, batch_size=3)
        # 10 unique selectors, batch_size=3 → at least 4 batches
        assert len(result) >= 4
        # Each batch has at most 3 unique selectors
        for batch in result:
            unique_sels = set()
            for expr in batch:
                for s in expr.get_selectors():
                    unique_sels.add(id(s))
            assert len(unique_sels) <= 3

    def test_oversized_single_expression_gets_own_batch(self):
        """An expression with more selectors than batch_size still gets its own batch."""
        # 5 selectors but batch_size=3
        selectors = [MagicMock() for _ in range(5)]
        expr = MagicMock()
        expr.get_selectors.return_value = selectors

        result = build_batches([expr], batch_size=3)
        assert len(result) == 1
        assert result[0] == [expr]

    def test_plan_example_two_batches(self):
        """Reproduces the 5-expression plan example and verifies grouping.

        ExprA uses {S1,S2,S3,S4,S5}      (5 selectors)
        ExprB uses {S5,S6,S7,S8,S9,S10}   (6 selectors)
        ExprC uses {S1,S2,S3,S4}           (4 selectors)
        ExprD uses {S6,S7,S8,S9}           (4 selectors)
        ExprE uses {S20}                   (1 selector)
        batch_size=7 → Batch1={B,D,E}, Batch2={A,C}
        """
        s = [MagicMock() for _ in range(21)]  # s[1]..s[10], s[20]

        exprA = MagicMock()
        exprA.get_selectors.return_value = [s[1], s[2], s[3], s[4], s[5]]

        exprB = MagicMock()
        exprB.get_selectors.return_value = [s[5], s[6], s[7], s[8], s[9], s[10]]

        exprC = MagicMock()
        exprC.get_selectors.return_value = [s[1], s[2], s[3], s[4]]

        exprD = MagicMock()
        exprD.get_selectors.return_value = [s[6], s[7], s[8], s[9]]

        exprE = MagicMock()
        exprE.get_selectors.return_value = [s[20]]

        result = build_batches([exprA, exprB, exprC, exprD, exprE], batch_size=7)

        assert len(result) == 2

        batch_sets = [set(b) for b in result]
        batch_AC = {exprA, exprC}
        batch_BDE = {exprB, exprD, exprE}
        assert batch_AC in batch_sets
        assert batch_BDE in batch_sets

    def test_single_expression_no_selectors(self):
        """Expression with no selectors returns a single batch."""
        expr = MagicMock()
        expr.get_selectors.return_value = []

        result = build_batches([expr], batch_size=5)
        assert len(result) == 1
        assert result[0] == [expr]


# ============================================================================
# Tests: split_by_hash_change
# ============================================================================
class TestSplitByHashChange:
    """Tests for split_by_hash_change helper."""

    def test_sinkless_all_items_go_to_changed_events(self):
        """When sink=None (sinkless), all items are placed in changed_by_type."""
        item1, item2 = MagicMock(), MagicMock()
        item1.get_id.return_value = 1
        item2.get_id.return_value = 2

        changed, unchanged, changed_ids = split_by_hash_change(
            items_by_type={"BASIC_EVENT": [item1, item2]},
            type_enum=MagicMock(),
            sink=None,
            spark=MagicMock(),
            hash_comparator=MagicMock(),
            kind="event",
        )

        assert changed == {"BASIC_EVENT": [item1, item2]}
        assert unchanged == {}
        assert changed_ids == {"BASIC_EVENT": [1, 2]}

    def test_sinkless_all_items_go_to_changed_aggregations(self):
        """When sink=None, aggregations are also all placed in changed_by_type."""
        item = MagicMock()
        item.get_id.return_value = 99

        changed, unchanged, changed_ids = split_by_hash_change(
            items_by_type={"HISTOGRAM": [item]},
            type_enum=MagicMock(),
            sink=None,
            spark=MagicMock(),
            hash_comparator=MagicMock(),
            kind="aggregation",
        )

        assert changed == {"HISTOGRAM": [item]}
        assert unchanged == {}
        assert changed_ids == {"HISTOGRAM": [99]}

    def test_empty_item_list_is_skipped(self):
        """A type entry with an empty list produces no output entries."""
        changed, unchanged, changed_ids = split_by_hash_change(
            items_by_type={"BASIC_EVENT": []},
            type_enum=MagicMock(),
            sink=None,
            spark=MagicMock(),
            hash_comparator=MagicMock(),
        )

        assert changed == {}
        assert unchanged == {}
        assert changed_ids == {}

    def test_with_sink_delegates_to_group_events_by_hash_change(self):
        """When sink is provided, delegates to hash_comparator.group_events_by_hash_change."""
        item_changed, item_unchanged = MagicMock(), MagicMock()
        item_changed.get_id.return_value = 10
        item_unchanged.get_id.return_value = 20

        mock_sink = MagicMock()
        mock_sink.config.get_output_uri_dimension_table.return_value = "catalog.gold.some_dim"

        mock_comparator = MagicMock()
        mock_comparator.group_events_by_hash_change.return_value = (
            [item_changed],
            [item_unchanged],
        )

        changed, unchanged, changed_ids = split_by_hash_change(
            items_by_type={"BASIC_EVENT": [item_changed, item_unchanged]},
            type_enum=MagicMock(),
            sink=mock_sink,
            spark=MagicMock(),
            hash_comparator=mock_comparator,
            kind="event",
        )

        mock_comparator.group_events_by_hash_change.assert_called_once()
        assert changed == {"BASIC_EVENT": [item_changed]}
        assert unchanged == {"BASIC_EVENT": [item_unchanged]}
        assert changed_ids == {"BASIC_EVENT": [10]}

    def test_with_sink_delegates_to_group_aggregations_by_hash_change(self):
        """When kind="aggregation", group_aggregations_by_hash_change is called."""
        item = MagicMock()
        item.get_id.return_value = 42

        mock_sink = MagicMock()
        mock_sink.config.get_output_uri_dimension_table.return_value = "catalog.gold.agg_dim"

        mock_comparator = MagicMock()
        mock_comparator.group_aggregations_by_hash_change.return_value = ([item], [])

        changed, unchanged, changed_ids = split_by_hash_change(
            items_by_type={"HISTOGRAM": [item]},
            type_enum=MagicMock(),
            sink=mock_sink,
            spark=MagicMock(),
            hash_comparator=mock_comparator,
            kind="aggregation",
        )

        mock_comparator.group_aggregations_by_hash_change.assert_called_once()
        assert changed == {"HISTOGRAM": [item]}
        assert changed_ids == {"HISTOGRAM": [42]}

    def test_with_sink_all_unchanged_produces_no_changed_entry(self):
        """When hash shows nothing changed, changed_by_type is empty."""
        item = MagicMock()

        mock_sink = MagicMock()
        mock_sink.config.get_output_uri_dimension_table.return_value = "catalog.gold.dim"

        mock_comparator = MagicMock()
        mock_comparator.group_events_by_hash_change.return_value = ([], [item])

        changed, unchanged, changed_ids = split_by_hash_change(
            items_by_type={"BASIC_EVENT": [item]},
            type_enum=MagicMock(),
            sink=mock_sink,
            spark=MagicMock(),
            hash_comparator=mock_comparator,
            kind="event",
        )

        assert changed == {}
        assert unchanged == {"BASIC_EVENT": [item]}
        assert changed_ids == {}

    def test_with_sink_multiple_types(self):
        """Multiple types are each processed independently."""
        event1, event2 = MagicMock(), MagicMock()
        event1.get_id.return_value = 1
        event2.get_id.return_value = 2

        mock_sink = MagicMock()
        mock_sink.config.get_output_uri_dimension_table.return_value = "catalog.gold.dim"

        call_count = [0]

        def side_effect(items, _table):
            call_count[0] += 1
            return (items, [])

        mock_comparator = MagicMock()
        mock_comparator.group_events_by_hash_change.side_effect = side_effect

        changed, unchanged, changed_ids = split_by_hash_change(
            items_by_type={
                "BASIC_EVENT": [event1],
                "SEQUENCE_OF_EVENTS": [event2],
            },
            type_enum=MagicMock(),
            sink=mock_sink,
            spark=MagicMock(),
            hash_comparator=mock_comparator,
            kind="event",
        )

        assert call_count[0] == 2
        assert "BASIC_EVENT" in changed
        assert "SEQUENCE_OF_EVENTS" in changed

    def test_unknown_kind_raises(self):
        """An unsupported `kind` is rejected before any comparison runs."""
        with pytest.raises(ValueError, match="Unsupported kind 'bogus'"):
            split_by_hash_change(
                items_by_type={"BASIC_EVENT": [MagicMock()]},
                type_enum=MagicMock(),
                sink=MagicMock(),
                spark=MagicMock(),
                hash_comparator=MagicMock(),
                kind="bogus",
            )


# ============================================================================
# Tests: dispatch_events
# ============================================================================
class TestDispatchEvents:
    """Tests for dispatch_events helper."""

    def _make_solvable_cls(self, result_df, meta_df):
        """Return a fake event class (not a container event subclass)."""
        _result = result_df
        _meta = meta_df

        class FakeSolvableCls:
            @classmethod
            def determine_events(cls, spark, events, **kwargs):
                return _result

            @classmethod
            def determine_metadata_df(cls, spark, events):
                return _meta

        return FakeSolvableCls

    def _make_container_cls(self, base_cls, result_df, meta_df):
        """Return a fake event class that IS a subclass of base_cls."""
        _result = result_df
        _meta = meta_df

        class FakeContainerCls(base_cls):
            @classmethod
            def determine_events(cls, spark, events, **kwargs):
                return _result

            @classmethod
            def determine_metadata_df(cls, spark, events):
                return _meta

        return FakeContainerCls

    def test_empty_events_by_type_returns_empty(self):
        """Empty input produces empty output dict."""
        event_dfs = dispatch_events(
            spark=MagicMock(),
            events_by_type={},
            type_enum=MagicMock(),
            solved_df=None,
            query=MagicMock(),
            solver=MagicMock(),
            pre_filtered_containers_df=None,
            container_event_cls=object,
        )
        assert event_dfs == {}

    def test_solvable_event_receives_solved_df(self):
        """Non-container event types are called with solved_df keyword arg."""
        mock_solved = MagicMock(spec=DataFrame)
        mock_result = MagicMock(spec=DataFrame)
        mock_meta = MagicMock(spec=DataFrame)
        mock_event = MagicMock()

        class FakeContainerBase:
            pass

        received_kwargs = {}

        class FakeSolvableCls:
            @classmethod
            def determine_events(cls, spark, events, **kwargs):
                received_kwargs.update(kwargs)
                return mock_result

            @classmethod
            def determine_metadata_df(cls, spark, events):
                return mock_meta

        mock_type_enum = MagicMock()
        mock_type_enum.__getitem__.return_value.value = FakeSolvableCls

        event_dfs = dispatch_events(
            spark=MagicMock(),
            events_by_type={"BASIC_EVENT": [mock_event]},
            type_enum=mock_type_enum,
            solved_df=mock_solved,
            query=MagicMock(),
            solver=MagicMock(),
            pre_filtered_containers_df=MagicMock(spec=DataFrame),
            container_event_cls=FakeContainerBase,
        )

        assert received_kwargs == {"solved_df": mock_solved}
        assert event_dfs["BASIC_EVENT"] is mock_result

    def test_container_event_receives_query_and_solver(self):
        """ContainerEvent subclasses receive query/solver/pre_filtered kwargs."""
        mock_result = MagicMock(spec=DataFrame)
        mock_meta = MagicMock(spec=DataFrame)
        mock_query = MagicMock()
        mock_solver = MagicMock()
        mock_pre_filtered = MagicMock(spec=DataFrame)
        mock_event = MagicMock()

        class FakeContainerBase:
            pass

        received_kwargs = {}

        class FakeContainerCls(FakeContainerBase):
            @classmethod
            def determine_events(cls, spark, events, **kwargs):
                received_kwargs.update(kwargs)
                return mock_result

            @classmethod
            def determine_metadata_df(cls, spark, events):
                return mock_meta

        mock_type_enum = MagicMock()
        mock_type_enum.__getitem__.return_value.value = FakeContainerCls

        event_dfs = dispatch_events(
            spark=MagicMock(),
            events_by_type={"CONTAINER_EVENT": [mock_event]},
            type_enum=mock_type_enum,
            solved_df=MagicMock(spec=DataFrame),
            query=mock_query,
            solver=mock_solver,
            pre_filtered_containers_df=mock_pre_filtered,
            container_event_cls=FakeContainerBase,
        )

        assert received_kwargs["query"] is mock_query
        assert received_kwargs["solver"] is mock_solver
        assert received_kwargs["pre_filtered_containers_df"] is mock_pre_filtered
        assert "solved_df" not in received_kwargs
        assert event_dfs["CONTAINER_EVENT"] is mock_result

    def test_empty_event_list_for_type_is_skipped(self):
        """Types with empty event lists produce no output entries."""
        mock_type_enum = MagicMock()

        event_dfs = dispatch_events(
            spark=MagicMock(),
            events_by_type={"BASIC_EVENT": []},
            type_enum=mock_type_enum,
            solved_df=None,
            query=MagicMock(),
            solver=MagicMock(),
            pre_filtered_containers_df=None,
            container_event_cls=object,
        )

        assert "BASIC_EVENT" not in event_dfs

    def test_metadata_not_computed_in_dispatch(self):
        """dispatch_events does not compute metadata (done separately in determine_report)."""

        class FakeContainerBase:
            pass

        meta_calls = []

        class FakeCls:
            @classmethod
            def determine_events(cls, spark, events, **kwargs):
                return MagicMock(spec=DataFrame)

            @classmethod
            def determine_metadata_df(cls, spark, events):
                meta_calls.append(len(events))
                return MagicMock(spec=DataFrame)

        mock_type_enum = MagicMock()
        mock_type_enum.__getitem__.return_value.value = FakeCls

        event1, event2 = MagicMock(), MagicMock()

        dispatch_events(
            spark=MagicMock(),
            events_by_type={
                "TYPE_A": [event1],
                "TYPE_B": [event1, event2],
            },
            type_enum=mock_type_enum,
            solved_df=None,
            query=MagicMock(),
            solver=MagicMock(),
            pre_filtered_containers_df=None,
            container_event_cls=FakeContainerBase,
        )

        assert len(meta_calls) == 0


# ============================================================================
# Tests: Report._solve_expressions_batched
# ============================================================================
_SINK_CONFIG = {
    "source": {
        "container_metrics_table": "spark_catalog.silver.container_metrics",
        "channel_metrics_table": "spark_catalog.silver.channel_metrics",
        "channels_uri": "spark_catalog.silver.channels",
    },
    "unity_sink": {
        "catalog": "spark_catalog",
        "schema": "gold",
        "table_prefix": "evaluation",
    },
    "query_engine": {"solver": "KeyValueStoreSolver"},
}

_SINKLESS_CONFIG = {
    "source": {
        "container_metrics_table": "spark_catalog.silver.container_metrics",
        "channel_metrics_table": "spark_catalog.silver.channel_metrics",
        "channels_uri": "spark_catalog.silver.channels",
    },
    "query_engine": {"solver": "KeyValueStoreSolver"},
}


def _build_report_for_solve(spark, config_dict):
    """Build a Report instance with mocked internals for _solve_expressions_batched tests."""
    with (
        patch.object(Report, "create_measurement_db"),
        patch.object(Report, "create_query_builder"),
        patch.object(Report, "create_solver"),
        patch.object(Report, "create_sink"),
    ):
        report = Report(
            name="test_report",
            spark=spark,
            workspace_client=create_autospec(WorkspaceClient),
            config=config_dict,
        )

    report.solver = MagicMock()
    report.solver.config.container_id_col = "container_id"
    return report


class TestSolveExpressionsBatched:
    """Tests for Report._solve_expressions_batched()."""

    def test_empty_expressions_returns_none(self, spark):
        """No expressions → None returned immediately."""
        report = _build_report_for_solve(spark, _SINKLESS_CONFIG)
        assert report._solve_expressions_batched([]) is None

    def test_single_batch_sinkless_creates_temp_view(self, spark):
        """Without a sink, the batch result is registered as a Spark temp view."""
        report = _build_report_for_solve(spark, _SINKLESS_CONFIG)

        mock_batch_df = MagicMock(spec=DataFrame)
        mock_table_df = MagicMock(spec=DataFrame)
        report.query = MagicMock()
        report.query.select.return_value.solve.return_value = mock_batch_df
        report.spark = MagicMock()
        report.spark.table.return_value = mock_table_df

        expr = MagicMock()
        expr.get_selectors.return_value = [MagicMock()]

        result = report._solve_expressions_batched([expr])

        mock_batch_df.createOrReplaceTempView.assert_called_once()
        view_name = mock_batch_df.createOrReplaceTempView.call_args[0][0]
        assert view_name.startswith("__impulse_temp_")

        report.spark.table.assert_called_once_with(view_name)
        assert result is mock_table_df

    def test_single_batch_with_sink_writes_delta_table(self, spark):
        """With a sink, the batch result is persisted as a Delta table."""
        report = _build_report_for_solve(spark, _SINK_CONFIG)

        mock_batch_df = MagicMock(spec=DataFrame)
        mock_table_df = MagicMock(spec=DataFrame)
        report.query = MagicMock()
        report.query.select.return_value.solve.return_value = mock_batch_df
        report.spark = MagicMock()
        report.spark.table.return_value = mock_table_df

        expr = MagicMock()
        expr.get_selectors.return_value = [MagicMock()]

        result = report._solve_expressions_batched([expr])

        write_chain = mock_batch_df.write.format.return_value.mode.return_value
        write_chain.saveAsTable.assert_called_once()

        fq_name = write_chain.saveAsTable.call_args[0][0]
        assert "__impulse_temp_" in fq_name
        assert "spark_catalog" in fq_name
        assert "gold" in fq_name

        report.spark.table.assert_called_once_with(fq_name)
        assert result is mock_table_df

    def test_multiple_batches_joined_on_container_id(self, spark):
        """Multiple batches produce a full-outer join on the container_id column."""
        report = _build_report_for_solve(spark, _SINKLESS_CONFIG)

        mock_df1 = MagicMock(spec=DataFrame)
        mock_df2 = MagicMock(spec=DataFrame)
        mock_joined = MagicMock(spec=DataFrame)
        mock_df1.join.return_value = mock_joined

        table_returns = [mock_df1, mock_df2]
        call_idx = [0]

        def fake_table(name):
            df = table_returns[call_idx[0]]
            call_idx[0] += 1
            return df

        report.query = MagicMock()
        report.query.select.return_value.solve.return_value = MagicMock(spec=DataFrame)
        report.spark = MagicMock()
        report.spark.table.side_effect = fake_table

        sel_a, sel_b = MagicMock(), MagicMock()
        expr1, expr2 = MagicMock(), MagicMock()
        expr1.get_selectors.return_value = [sel_a]
        expr2.get_selectors.return_value = [sel_b]

        with patch(
            "impulse_reporting.core.report_utils.build_batches", return_value=[[expr1], [expr2]]
        ):
            result = report._solve_expressions_batched([expr1, expr2])

        mock_df1.join.assert_called_once_with(mock_df2, on="container_id", how="full_outer")
        assert result is mock_joined

    def test_each_call_generates_unique_run_id(self, spark):
        """Two consecutive calls produce different temp view name prefixes (unique run_id)."""
        report = _build_report_for_solve(spark, _SINKLESS_CONFIG)

        mock_batch_df = MagicMock(spec=DataFrame)
        report.query = MagicMock()
        report.query.select.return_value.solve.return_value = mock_batch_df
        report.spark = MagicMock()
        report.spark.table.return_value = MagicMock(spec=DataFrame)

        view_names = []
        mock_batch_df.createOrReplaceTempView.side_effect = lambda name: view_names.append(name)

        expr = MagicMock()
        expr.get_selectors.return_value = [MagicMock()]

        report._solve_expressions_batched([expr])
        report._solve_expressions_batched([expr])

        assert len(view_names) == 2
        assert view_names[0] != view_names[1]

    def test_query_select_called_with_batch_expressions(self, spark):
        """The solver is invoked via query.select(*batch_exprs).solve(...)."""
        report = _build_report_for_solve(spark, _SINKLESS_CONFIG)

        mock_batch_df = MagicMock(spec=DataFrame)
        report.query = MagicMock()
        report.query.select.return_value.solve.return_value = mock_batch_df
        report.spark = MagicMock()
        report.spark.table.return_value = MagicMock(spec=DataFrame)

        sel = MagicMock()
        expr = MagicMock()
        expr.get_selectors.return_value = [sel]

        report._solve_expressions_batched([expr])

        report.query.select.assert_called_once_with(expr)
        report.query.select.return_value.solve.assert_called_once()


# ============================================================================
# Fixtures for the generic entity orchestration/persistence helpers
# ============================================================================
class _FooEntity:
    pass


class _BarEntity:
    pass


class _FakeType(Enum):
    """Two-member entity type-enum mirroring ChannelType's resolver shape.

    FOO and BAR intentionally share a fact table (``shared_fact``) but have
    distinct dimension tables, so grouping-by-output-table is exercised.
    """

    FOO = _FooEntity
    BAR = _BarEntity

    def get_fact_table_name(self):
        return "shared_fact"

    def get_dimension_table_name(self):
        return {"FOO": "foo_dim", "BAR": "bar_dim"}[self.name]

    @classmethod
    def get_any_for_fact_table(cls, table_name):
        for member in cls:
            if member.get_fact_table_name() == table_name:
                return member
        raise ValueError(f"no type for fact table {table_name}")

    @classmethod
    def get_any_for_dimension_table(cls, table_name):
        for member in cls:
            if member.get_dimension_table_name() == table_name:
                return member
        raise ValueError(f"no type for dimension table {table_name}")


class TestGroupSelectablesByType:
    def test_buckets_by_isinstance(self):
        foo1, bar, foo2 = _FooEntity(), _BarEntity(), _FooEntity()
        result = group_selectables_by_type([foo1, bar, foo2], _FakeType)
        assert result["FOO"] == [foo1, foo2]
        assert result["BAR"] == [bar]

    def test_every_member_gets_a_bucket_even_when_empty(self):
        result = group_selectables_by_type([], _FakeType)
        assert set(result) == {"FOO", "BAR"}
        assert result["FOO"] == [] and result["BAR"] == []

    def test_item_matching_no_type_is_dropped(self):
        # An item that isn't an instance of any member's class lands in no bucket.
        result = group_selectables_by_type([object()], _FakeType)
        assert result == {"FOO": [], "BAR": []}


class TestMergeChangedUnchanged:
    def test_only_changed(self):
        df = MagicMock()
        result = merge_changed_unchanged({"FOO": df}, {})
        assert result == {"FOO": {"changed": df, "unchanged": None}}

    def test_only_unchanged(self):
        df = MagicMock()
        result = merge_changed_unchanged({}, {"FOO": df})
        assert result == {"FOO": {"changed": None, "unchanged": df}}

    def test_both_sides_present(self):
        c, u = MagicMock(), MagicMock()
        result = merge_changed_unchanged({"FOO": c}, {"FOO": u})
        assert result == {"FOO": {"changed": c, "unchanged": u}}


class TestBuildMetadataDfs:
    def test_non_empty_types_build_metadata(self):
        meta = MagicMock()
        cls = MagicMock()
        cls.determine_metadata_df.return_value = meta
        type_enum = MagicMock()
        type_enum.__getitem__.return_value.value = cls
        spark = MagicMock()
        items = [MagicMock()]

        result = build_metadata_dfs({"FOO": items}, type_enum, spark)

        cls.determine_metadata_df.assert_called_once_with(spark, items)
        assert result == {"FOO": meta}

    def test_empty_types_skipped(self):
        type_enum = MagicMock()
        result = build_metadata_dfs({"FOO": []}, type_enum, MagicMock())
        assert result == {}


class TestPersistFactsFull:
    def test_groups_by_table_and_flattens_dict_and_bare(self):
        bare, changed, unchanged = MagicMock(), MagicMock(), MagicMock()
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        factory = MagicMock()
        factory.create_writer.return_value = writer

        persist_facts_full(
            {"FOO": bare, "BAR": {"changed": changed, "unchanged": unchanged}},
            _FakeType,
            factory,
        )

        # FOO + BAR share "shared_fact" → a single write with all three dfs.
        writer.write.assert_called_once()
        written = writer.write.call_args.args[0]
        assert written == [bare, changed, unchanged]

    def test_none_entries_dropped(self):
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        factory = MagicMock()
        factory.create_writer.return_value = writer

        persist_facts_full({"FOO": {"changed": None, "unchanged": None}}, _FakeType, factory)
        # Nothing to write → no write call.
        writer.write.assert_not_called()


class TestPersistDimensionsFull:
    def test_writes_each_dimension_table(self):
        foo_meta, bar_meta = MagicMock(), MagicMock()
        writer = MagicMock()
        writer.extract_metadata_schema_and_output_uri.return_value = ("schema", "uri")
        factory = MagicMock()
        factory.create_writer.return_value = writer

        persist_dimensions_full({"FOO": foo_meta, "BAR": bar_meta}, _FakeType, factory)

        # Distinct dim tables (foo_dim, bar_dim) → one write each.
        assert writer.write.call_count == 2


class TestPersistFactsIncremental:
    def _patch_factory(self, writer):
        factory = MagicMock()
        factory.create_writer.return_value = writer
        return patch(
            "impulse_reporting.persist.report_storage.WriterFactory", return_value=factory
        )

    @staticmethod
    def _delete_condition_strs(call):
        """Stringify the delete_conditions Columns of a merge_incremental call."""
        return sorted(str(c) for c in call.kwargs["delete_conditions"])

    def test_changed_and_unchanged_merged_in_one_call(self):
        # Changed (all containers) + unchanged (processed) union into a single
        # MERGE; delete scope covers both updated containers and changed ids.
        changed, unchanged, unioned = MagicMock(), MagicMock(), MagicMock()
        changed.unionByName.return_value = unioned
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        sink = MagicMock()

        with self._patch_factory(writer):
            persist_facts_incremental(
                {"FOO": {"changed": changed, "unchanged": unchanged}},
                _FakeType,
                sink,
                lambda df, _schema: df,
                id_column="channel_id",
                merge_keys=["container_id", "channel_id", "tstart"],
                changed_ids={"FOO": [1, 2]},
                processed_container_ids=[10, 20],
                updated_container_ids=[10, 20],
            )

        sink.replace_by_ids.assert_not_called()  # method no longer exists on the sink
        sink.merge_incremental.assert_called_once()
        call = sink.merge_incremental.call_args
        # Source is the changed ∪ unchanged union; keys forwarded.
        changed.unionByName.assert_called_once_with(unchanged)
        assert call.args == (unioned, "uri", ["container_id", "channel_id", "tstart"])
        assert self._delete_condition_strs(call) == sorted(
            [
                str(F.col("target.container_id").isin([10, 20])),
                str(F.col("target.channel_id").isin([1, 2])),
            ]
        )

    def test_new_only_container_merges_without_container_delete(self):
        # A brand-new container (processed but not updated) must still be inserted,
        # but carries NO container-scoped delete (it has no gold rows to prune).
        unchanged = MagicMock()
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        sink = MagicMock()

        with self._patch_factory(writer):
            persist_facts_incremental(
                {"FOO": {"changed": None, "unchanged": unchanged}},
                _FakeType,
                sink,
                lambda df, _schema: df,
                id_column="channel_id",
                merge_keys=["channel_id"],
                changed_ids={},
                processed_container_ids=[10],
                updated_container_ids=[],
            )

        sink.merge_incremental.assert_called_once()
        call = sink.merge_incremental.call_args
        assert call.args == (unchanged, "uri", ["channel_id"])
        # Nothing to delete: new container, no changed ids.
        assert self._delete_condition_strs(call) == []

    def test_changed_only_scopes_delete_by_changed_ids(self):
        # No processed containers, but a changed definition → still merges,
        # deleting stale rows only for the changed entity ids.
        changed = MagicMock()
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        sink = MagicMock()

        with self._patch_factory(writer):
            persist_facts_incremental(
                {"FOO": {"changed": changed, "unchanged": None}},
                _FakeType,
                sink,
                lambda df, _schema: df,
                id_column="channel_id",
                merge_keys=["channel_id"],
                changed_ids={"FOO": [7]},
                processed_container_ids=[],
                updated_container_ids=[],
            )

        sink.merge_incremental.assert_called_once()
        call = sink.merge_incremental.call_args
        assert call.args == (changed, "uri", ["channel_id"])
        assert self._delete_condition_strs(call) == [str(F.col("target.channel_id").isin([7]))]

    def test_changed_skipped_when_type_not_in_changed_ids(self):
        # Changed df present but not listed in changed_ids and nothing processed
        # → no work → skipped entirely.
        changed = MagicMock()
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        sink = MagicMock()

        with self._patch_factory(writer):
            persist_facts_incremental(
                {"FOO": {"changed": changed, "unchanged": None}},
                _FakeType,
                sink,
                lambda df, _schema: df,
                id_column="channel_id",
                merge_keys=["channel_id"],
                changed_ids={},
                processed_container_ids=[],
                updated_container_ids=[],
            )

        sink.merge_incremental.assert_not_called()

    def test_none_value_is_a_noop(self):
        # A type whose fact value is None triggers no merge.
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        sink = MagicMock()

        with self._patch_factory(writer):
            persist_facts_incremental(
                {"FOO": None},
                _FakeType,
                sink,
                lambda df, _schema: df,
                id_column="channel_id",
                merge_keys=["channel_id"],
                changed_ids={},
                processed_container_ids=[10],
                updated_container_ids=[10],
            )

        sink.merge_incremental.assert_not_called()

    def test_no_processed_and_no_changed_is_skipped(self):
        # Unchanged df but nothing processed and nothing changed → skipped,
        # keeping gold byte-identical.
        unchanged = MagicMock()
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        sink = MagicMock()

        with self._patch_factory(writer):
            persist_facts_incremental(
                {"FOO": {"changed": None, "unchanged": unchanged}},
                _FakeType,
                sink,
                lambda df, _schema: df,
                id_column="channel_id",
                merge_keys=["channel_id"],
                changed_ids={},
                processed_container_ids=[],
                updated_container_ids=[],
            )

        sink.merge_incremental.assert_not_called()

    def test_bare_df_is_merged(self):
        bare = MagicMock()
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        sink = MagicMock()

        with self._patch_factory(writer):
            persist_facts_incremental(
                {"FOO": bare},
                _FakeType,
                sink,
                lambda df, _schema: df,
                id_column="channel_id",
                merge_keys=["channel_id"],
                changed_ids={},
                processed_container_ids=[10],
                updated_container_ids=[10],
            )

        sink.merge_incremental.assert_called_once()
        call = sink.merge_incremental.call_args
        assert call.args == (bare, "uri", ["channel_id"])
        assert self._delete_condition_strs(call) == [str(F.col("target.container_id").isin([10]))]

    def test_callable_merge_keys_resolved_per_type(self):
        unchanged = MagicMock()
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        sink = MagicMock()

        def keys_for(entity_type):
            return ["container_id", entity_type.name.lower()]

        with self._patch_factory(writer):
            persist_facts_incremental(
                {"FOO": {"changed": None, "unchanged": unchanged}},
                _FakeType,
                sink,
                lambda df, _schema: df,
                id_column="id",
                merge_keys=keys_for,
                changed_ids={},
                processed_container_ids=[10],
                updated_container_ids=[10],
            )

        sink.merge_incremental.assert_called_once()
        assert sink.merge_incremental.call_args.args == (
            unchanged,
            "uri",
            ["container_id", "foo"],
        )

    def test_custom_container_id_col_used_in_delete_scope(self):
        bare = MagicMock()
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        sink = MagicMock()

        with self._patch_factory(writer):
            persist_facts_incremental(
                {"FOO": bare},
                _FakeType,
                sink,
                lambda df, _schema: df,
                id_column="channel_id",
                merge_keys=["channel_id"],
                changed_ids={},
                processed_container_ids=[10],
                updated_container_ids=[10],
                container_id_col="cont_id",
            )

        call = sink.merge_incremental.call_args
        assert self._delete_condition_strs(call) == [str(F.col("target.cont_id").isin([10]))]

    def test_shared_table_changed_types_union_into_one_merge(self):
        # FOO and BAR share `shared_fact`: their changed DFs union into a single
        # MERGE with the combined changed-id delete scope.
        foo_changed, bar_changed, unioned = MagicMock(), MagicMock(), MagicMock()
        foo_changed.unionByName.return_value = unioned
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        sink = MagicMock()

        with self._patch_factory(writer):
            persist_facts_incremental(
                {
                    "FOO": {"changed": foo_changed, "unchanged": None},
                    "BAR": {"changed": bar_changed, "unchanged": None},
                },
                _FakeType,
                sink,
                lambda df, _schema: df,
                id_column="visual_id",
                merge_keys=["visual_id"],
                changed_ids={"FOO": [1], "BAR": [2]},
                processed_container_ids=[],
                updated_container_ids=[],
            )

        # Single MERGE over the shared table with the unioned DF + combined ids.
        foo_changed.unionByName.assert_called_once_with(bar_changed)
        sink.merge_incremental.assert_called_once()
        call = sink.merge_incremental.call_args
        assert call.args == (unioned, "uri", ["visual_id"])
        assert self._delete_condition_strs(call) == [str(F.col("target.visual_id").isin([1, 2]))]

    def test_shared_table_unchanged_types_union_into_one_merge(self):
        # FOO and BAR unchanged dfs both target `shared_fact` → unioned into a
        # single MERGE (no per-type clobber), scoped to updated containers.
        foo_unchanged, bar_unchanged, unioned = MagicMock(), MagicMock(), MagicMock()
        foo_unchanged.unionByName.return_value = unioned
        writer = MagicMock()
        writer.extract_fact_schema_and_output_uri.return_value = ("schema", "uri")
        sink = MagicMock()

        with self._patch_factory(writer):
            persist_facts_incremental(
                {
                    "FOO": {"changed": None, "unchanged": foo_unchanged},
                    "BAR": {"changed": None, "unchanged": bar_unchanged},
                },
                _FakeType,
                sink,
                lambda df, _schema: df,
                id_column="visual_id",
                merge_keys=["visual_id"],
                changed_ids={},
                processed_container_ids=[10],
                updated_container_ids=[10],
            )

        foo_unchanged.unionByName.assert_called_once_with(bar_unchanged)
        sink.merge_incremental.assert_called_once()
        call = sink.merge_incremental.call_args
        assert call.args == (unioned, "uri", ["visual_id"])
        assert self._delete_condition_strs(call) == [str(F.col("target.container_id").isin([10]))]


class TestPersistDimensionsIncremental:
    def test_upserts_each_type_with_merge_keys(self):
        foo_meta = MagicMock()
        writer = MagicMock()
        writer.extract_metadata_schema_and_output_uri.return_value = ("schema", "uri")
        factory = MagicMock()
        factory.create_writer.return_value = writer
        sink = MagicMock()

        with patch("impulse_reporting.persist.report_storage.WriterFactory", return_value=factory):
            persist_dimensions_incremental(
                {"FOO": foo_meta},
                _FakeType,
                sink,
                lambda df, _schema: df,
                merge_keys=["channel_id"],
            )

        sink.upsert.assert_called_once_with(foo_meta, "uri", ["channel_id"])
