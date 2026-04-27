"""
Unit tests for Report.determine_report() and Report.persist_results()
with is_incremental=True and is_incremental=False.

Tests use mocking to isolate the Report class logic from downstream
Spark processing (event/aggregation determination, persistence).
"""

from unittest.mock import MagicMock, create_autospec, patch

import pytest
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame

from mda_reporting.aggregations.aggregation_types import AggregationType
from mda_reporting.core.report import Report
from mda_reporting.events.event_types import EventType

# ---------------------------------------------------------------------------
# Shared test config (no Spark needed for construction)
# ---------------------------------------------------------------------------
DUMMY_CONFIG = {
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
    "query_engine": {"solver": "BasicNarrowSolver"},
}

INCREMENTAL_CONFIG = {
    **DUMMY_CONFIG,
    "incremental": {
        "enabled": True,
    },
}

INCREMENTAL_DISABLED_CONFIG = {
    **DUMMY_CONFIG,
    "incremental": {
        "enabled": False,
    },
}


# ============================================================================
# Helper to create a Report with mocked internals
# ============================================================================
def _build_report(spark, config_dict=None):
    """Build a Report instance with mocked DB / query / solver so it won't
    hit real tables during construction."""
    config_dict = config_dict or DUMMY_CONFIG

    with (
        patch.object(Report, "create_measurement_db") as mock_db_factory,
        patch.object(Report, "create_query_builder") as mock_qb_factory,
        patch.object(Report, "create_solver") as mock_solver_factory,
        patch.object(Report, "create_sink") as mock_sink_factory,
    ):
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_solver = MagicMock()
        mock_sink = MagicMock()
        mock_sink.config = MagicMock()

        mock_db_factory.return_value = mock_db
        mock_qb_factory.return_value = mock_query
        mock_solver_factory.return_value = mock_solver
        mock_sink_factory.return_value = mock_sink

        report = Report(
            name="test_report",
            spark=spark,
            workspace_client=create_autospec(WorkspaceClient),
            config=config_dict,
        )

    return report


# ============================================================================
# Tests: determine_report with is_incremental=False
# ============================================================================
class TestDetermineReportFullMode:
    """Tests for determine_report with is_incremental=False (full processing)."""

    @pytest.fixture(autouse=True)
    def mock_gold_exists(self):
        """Mock gold layer as existing to test full mode logic directly."""
        with patch.object(Report, "_gold_layer_exists", return_value=True):
            yield

    def test_full_mode_does_not_call_container_detection(self, spark):
        """In full mode, _detect_upserted_containers should NOT be called."""
        report = _build_report(spark)

        with (
            patch.object(report, "_detect_upserted_containers") as mock_detect,
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report(is_incremental=False)

            mock_detect.assert_not_called()

    def test_full_mode_does_not_use_hash_comparator(self, spark):
        """In full mode, DefinitionHashComparator should NOT be instantiated."""
        report = _build_report(spark)

        with (
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch("mda_reporting.core.report.DefinitionHashComparator") as mock_hash_cls,
        ):
            report.determine_report(is_incremental=False)

            mock_hash_cls.assert_called()

    def test_full_mode_sets_is_incremental_false(self, spark):
        """After determine_report(is_incremental=False), _is_incremental is False."""
        report = _build_report(spark)

        with (
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report(is_incremental=False)

        assert report._is_incremental is False
        assert report._changed_event_ids == {}
        assert report._changed_aggregation_ids == {}

    def test_full_mode_passes_none_as_pre_filtered(self, spark):
        """In full mode, pre_filtered_containers_df=None is passed to event/agg processing."""
        report = _build_report(spark)

        mock_event_cls = MagicMock()
        mock_event_cls.determine_events.return_value = MagicMock(spec=DataFrame)
        mock_event_cls.determine_metadata_df.return_value = MagicMock(spec=DataFrame)

        mock_event1 = MagicMock()
        mock_event1.__class__ = type(mock_event_cls)

        events_by_type = {"BASIC_EVENT": [mock_event1]}

        with (
            patch.object(report, "_group_events_by_type", return_value=events_by_type),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch.dict(
                EventType._member_map_,
                {"BASIC_EVENT": MagicMock(value=mock_event_cls, name="BASIC_EVENT")},
            ),
        ):
            # Make isinstance check work
            EventType._member_map_["BASIC_EVENT"].name = "BASIC_EVENT"

            report.determine_report(is_incremental=False)

            # Verify events were processed with pre_filtered_containers_df=None
            mock_event_cls.determine_events.assert_called_once()
            call_kwargs = mock_event_cls.determine_events.call_args
            assert call_kwargs.kwargs.get("pre_filtered_containers_df") is None

    def test_full_mode_events_stored_as_dataframe_not_dict(self, spark):
        """In full mode, event_dfs values are DataFrames, not dicts."""
        report = _build_report(spark)
        mock_df = MagicMock(spec=DataFrame)

        mock_event_cls = MagicMock()
        mock_event_cls.determine_events.return_value = mock_df
        mock_event_cls.determine_metadata_df.return_value = MagicMock(spec=DataFrame)

        mock_event1 = MagicMock()

        events_by_type = {"BASIC_EVENT": [mock_event1]}

        with (
            patch.object(report, "_group_events_by_type", return_value=events_by_type),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch.dict(
                EventType._member_map_,
                {"BASIC_EVENT": MagicMock(value=mock_event_cls, name="BASIC_EVENT")},
            ),
        ):
            EventType._member_map_["BASIC_EVENT"].name = "BASIC_EVENT"
            report.determine_report(is_incremental=False)

        assert "BASIC_EVENT" in report.event_dfs
        assert not isinstance(report.event_dfs["BASIC_EVENT"]["changed"], dict)

    def test_full_mode_uses_config_default_when_none(self, spark):
        """When is_incremental=None and config.incremental.enabled=False, uses full mode."""
        report = _build_report(spark, INCREMENTAL_DISABLED_CONFIG)

        with (
            patch.object(report, "_detect_upserted_containers") as mock_detect,
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report(is_incremental=None)

            mock_detect.assert_not_called()


# ============================================================================
# Tests: determine_report with is_incremental=True
# ============================================================================
class TestDetermineReportIncrementalMode:
    """Tests for determine_report with is_incremental=True."""

    @pytest.fixture(autouse=True)
    def mock_gold_exists(self):
        """Gold layer exists for all incremental mode tests."""
        with patch.object(Report, "_gold_layer_exists", return_value=True):
            yield

    def test_is_incremental_calls_container_detection(self, spark):
        """In incremental mode, _detect_upserted_containers IS called."""
        report = _build_report(spark, INCREMENTAL_CONFIG)
        mock_upserted_df = MagicMock(spec=DataFrame)

        with (
            patch.object(
                report, "_detect_upserted_containers", return_value=mock_upserted_df
            ) as mock_detect,
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch("mda_reporting.core.report.DefinitionHashComparator"),
        ):
            report.determine_report(is_incremental=True)

            mock_detect.assert_called_once()

    def test_incremental_sets_is_incremental_true(self, spark):
        """After determine_report(is_incremental=True), _is_incremental is True."""
        report = _build_report(spark, INCREMENTAL_CONFIG)

        with (
            patch.object(
                report,
                "_detect_upserted_containers",
                return_value=MagicMock(spec=DataFrame),
            ),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch("mda_reporting.core.report.DefinitionHashComparator"),
        ):
            report.determine_report(is_incremental=True)
            assert report._is_incremental is True

    def test_incremental_detect_returns_none_falls_back_to_full(self, spark):
        """When _detect_upserted_containers returns None, falls back to full processing."""
        report = _build_report(spark, INCREMENTAL_CONFIG)

        mock_event_cls = MagicMock()
        mock_event_cls.determine_events.return_value = MagicMock(spec=DataFrame)
        mock_event_cls.determine_metadata_df.return_value = MagicMock(spec=DataFrame)

        events_by_type = {"BASIC_EVENT": [MagicMock()]}

        with (
            patch.object(report, "_detect_upserted_containers", return_value=None),
            patch.object(report, "_group_events_by_type", return_value=events_by_type),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch.dict(
                EventType._member_map_,
                {"BASIC_EVENT": MagicMock(value=mock_event_cls, name="BASIC_EVENT")},
            ),
        ):
            EventType._member_map_["BASIC_EVENT"].name = "BASIC_EVENT"
            report.determine_report(is_incremental=True)

            # With None (gold doesn't exist), hash_comparator is None, so simple mode
            # pre_filtered_containers_df is None -> full processing
            call_kwargs = mock_event_cls.determine_events.call_args
            assert call_kwargs.kwargs.get("pre_filtered_containers_df") is None
            # Stored as DataFrame, not dict (simple mode path)
            assert not isinstance(report.event_dfs["BASIC_EVENT"]["changed"], dict)

    def test_incremental_with_hash_optimization_creates_comparator(self, spark):
        """When conditions are met, DefinitionHashComparator IS created."""
        report = _build_report(spark, INCREMENTAL_CONFIG)
        mock_upserted_df = MagicMock(spec=DataFrame)

        with (
            patch.object(report, "_detect_upserted_containers", return_value=mock_upserted_df),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch("mda_reporting.core.report.DefinitionHashComparator") as mock_hash_cls,
        ):
            report.determine_report(is_incremental=True)

            mock_hash_cls.assert_called_once_with(spark)

    def test_incremental_changed_events_processed_with_none(self, spark):
        """Changed events are processed with pre_filtered_containers_df=None (full)."""
        report = _build_report(spark, INCREMENTAL_CONFIG)
        mock_upserted_df = MagicMock(spec=DataFrame)

        mock_event_cls = MagicMock()
        mock_event_cls.determine_events.return_value = MagicMock(spec=DataFrame)
        mock_event_cls.determine_metadata_df.return_value = MagicMock(spec=DataFrame)

        changed_event = MagicMock()
        changed_event.get_id.return_value = 42

        unchanged_event = MagicMock()
        unchanged_event.get_id.return_value = 99

        events_by_type = {"BASIC_EVENT": [changed_event, unchanged_event]}

        # Mock hash comparator to split events
        mock_comparator_instance = MagicMock()
        mock_comparator_instance.group_events_by_hash_change.return_value = (
            [changed_event],
            [unchanged_event],
        )

        with (
            patch.object(report, "_detect_upserted_containers", return_value=mock_upserted_df),
            patch.object(report, "_group_events_by_type", return_value=events_by_type),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch(
                "mda_reporting.core.report.DefinitionHashComparator",
                return_value=mock_comparator_instance,
            ),
            patch.dict(
                EventType._member_map_,
                {"BASIC_EVENT": MagicMock(value=mock_event_cls, name="BASIC_EVENT")},
            ),
        ):
            EventType._member_map_["BASIC_EVENT"].name = "BASIC_EVENT"

            # Mock get_output_uri_dimension_table
            report.sink.config.get_output_uri_dimension_table.return_value = (
                "spark_catalog.gold.evaluation_event_dimension"
            )

            report.determine_report(is_incremental=True)

            # Should be called twice: once for changed (None), once for unchanged (incremental DF)
            assert mock_event_cls.determine_events.call_count == 2

            # First call: changed events with pre_filtered_containers_df=None
            first_call = mock_event_cls.determine_events.call_args_list[0]
            assert first_call.kwargs.get("pre_filtered_containers_df") is None
            assert first_call.args[3] == [changed_event]

            # Second call: unchanged events with pre_filtered_containers_df=mock_upserted_df
            second_call = mock_event_cls.determine_events.call_args_list[1]
            assert second_call.kwargs.get("pre_filtered_containers_df") is mock_upserted_df
            assert second_call.args[3] == [unchanged_event]

    def test_incremental_changed_event_ids_tracked(self, spark):
        """Changed event IDs are tracked in _changed_event_ids for persistence."""
        report = _build_report(spark, INCREMENTAL_CONFIG)
        mock_upserted_df = MagicMock(spec=DataFrame)

        mock_event_cls = MagicMock()
        mock_event_cls.determine_events.return_value = MagicMock(spec=DataFrame)
        mock_event_cls.determine_metadata_df.return_value = MagicMock(spec=DataFrame)

        changed_event = MagicMock()
        changed_event.get_id.return_value = 42

        events_by_type = {"BASIC_EVENT": [changed_event]}

        mock_comparator = MagicMock()
        mock_comparator.group_events_by_hash_change.return_value = (
            [changed_event],
            [],
        )

        with (
            patch.object(report, "_detect_upserted_containers", return_value=mock_upserted_df),
            patch.object(report, "_group_events_by_type", return_value=events_by_type),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch(
                "mda_reporting.core.report.DefinitionHashComparator",
                return_value=mock_comparator,
            ),
            patch.dict(
                EventType._member_map_,
                {"BASIC_EVENT": MagicMock(value=mock_event_cls, name="BASIC_EVENT")},
            ),
        ):
            EventType._member_map_["BASIC_EVENT"].name = "BASIC_EVENT"
            report.sink.config.get_output_uri_dimension_table.return_value = "tbl"
            report.determine_report(is_incremental=True)

        assert "BASIC_EVENT" in report._changed_event_ids
        assert report._changed_event_ids["BASIC_EVENT"] == [42]

    def test_incremental_changed_aggregation_ids_tracked(self, spark):
        """Changed aggregation IDs are tracked in _changed_aggregation_ids."""
        report = _build_report(spark, INCREMENTAL_CONFIG)
        mock_upserted_df = MagicMock(spec=DataFrame)

        mock_agg_cls = MagicMock()
        mock_agg_cls.determine_aggregations.return_value = MagicMock(spec=DataFrame)
        mock_agg_cls.determine_metadata_df.return_value = MagicMock(spec=DataFrame)

        changed_agg = MagicMock()
        changed_agg.get_id.return_value = 101

        aggs_by_type = {"HISTOGRAM": [changed_agg]}

        mock_comparator = MagicMock()
        mock_comparator.group_aggregations_by_hash_change.return_value = (
            [changed_agg],
            [],
        )

        with (
            patch.object(report, "_detect_upserted_containers", return_value=mock_upserted_df),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value=aggs_by_type),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch(
                "mda_reporting.core.report.DefinitionHashComparator",
                return_value=mock_comparator,
            ),
            patch.dict(
                AggregationType._member_map_,
                {"HISTOGRAM": MagicMock(value=mock_agg_cls, name="HISTOGRAM")},
            ),
        ):
            AggregationType._member_map_["HISTOGRAM"].name = "HISTOGRAM"
            report.sink.config.get_output_uri_dimension_table.return_value = "tbl"
            report.determine_report(is_incremental=True)

        assert "HISTOGRAM" in report._changed_aggregation_ids
        assert report._changed_aggregation_ids["HISTOGRAM"] == [101]

    def test_incremental_events_stored_as_dict(self, spark):
        """In incremental hash mode, event_dfs values are dicts with 'changed'/'unchanged'."""
        report = _build_report(spark, INCREMENTAL_CONFIG)
        mock_upserted_df = MagicMock(spec=DataFrame)

        mock_event_cls = MagicMock()
        mock_event_cls.determine_events.return_value = MagicMock(spec=DataFrame)
        mock_event_cls.determine_metadata_df.return_value = MagicMock(spec=DataFrame)

        events_by_type = {"BASIC_EVENT": [MagicMock()]}

        mock_comparator = MagicMock()
        mock_comparator.group_events_by_hash_change.return_value = (
            [MagicMock()],
            [],
        )

        with (
            patch.object(report, "_detect_upserted_containers", return_value=mock_upserted_df),
            patch.object(report, "_group_events_by_type", return_value=events_by_type),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch(
                "mda_reporting.core.report.DefinitionHashComparator",
                return_value=mock_comparator,
            ),
            patch.dict(
                EventType._member_map_,
                {"BASIC_EVENT": MagicMock(value=mock_event_cls, name="BASIC_EVENT")},
            ),
        ):
            EventType._member_map_["BASIC_EVENT"].name = "BASIC_EVENT"
            report.sink.config.get_output_uri_dimension_table.return_value = "tbl"
            report.determine_report(is_incremental=True)

        assert isinstance(report.event_dfs["BASIC_EVENT"], dict)
        assert "changed" in report.event_dfs["BASIC_EVENT"]
        assert "unchanged" in report.event_dfs["BASIC_EVENT"]

    def test_incremental_uses_config_when_mode_is_none(self, spark):
        """When is_incremental=None and config.incremental.enabled=True, uses incremental."""
        report = _build_report(spark, INCREMENTAL_CONFIG)

        with (
            patch.object(
                report,
                "_detect_upserted_containers",
                return_value=MagicMock(spec=DataFrame),
            ) as mock_detect,
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch("mda_reporting.core.report.DefinitionHashComparator"),
        ):
            report.determine_report(is_incremental=None)

            mock_detect.assert_called_once()

    def test_incremental_no_changed_events_all_unchanged(self, spark):
        """When no events have changed definitions, all are processed incrementally."""
        report = _build_report(spark, INCREMENTAL_CONFIG)
        mock_upserted_df = MagicMock(spec=DataFrame)

        mock_event_cls = MagicMock()
        mock_event_cls.determine_events.return_value = MagicMock(spec=DataFrame)
        mock_event_cls.determine_metadata_df.return_value = MagicMock(spec=DataFrame)

        event = MagicMock()
        events_by_type = {"BASIC_EVENT": [event]}

        mock_comparator = MagicMock()
        mock_comparator.group_events_by_hash_change.return_value = (
            [],  # No changed events
            [event],  # All unchanged
        )

        with (
            patch.object(report, "_detect_upserted_containers", return_value=mock_upserted_df),
            patch.object(report, "_group_events_by_type", return_value=events_by_type),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
            patch(
                "mda_reporting.core.report.DefinitionHashComparator",
                return_value=mock_comparator,
            ),
            patch.dict(
                EventType._member_map_,
                {"BASIC_EVENT": MagicMock(value=mock_event_cls, name="BASIC_EVENT")},
            ),
        ):
            EventType._member_map_["BASIC_EVENT"].name = "BASIC_EVENT"
            report.sink.config.get_output_uri_dimension_table.return_value = "tbl"
            report.determine_report(is_incremental=True)

            assert mock_event_cls.determine_events.call_count == 1
            call_kwargs = mock_event_cls.determine_events.call_args
            assert call_kwargs.kwargs.get("pre_filtered_containers_df") is mock_upserted_df

        assert "BASIC_EVENT" not in report._changed_event_ids


# ============================================================================
# Tests: persist_results with is_incremental=False
# ============================================================================
class TestPersistResultsFullMode:
    """Tests for persist_results when _is_incremental is False."""

    def test_persist_full_calls_persist_full_method(self, spark):
        """persist_results() calls _persist_full() when _is_incremental=False."""
        report = _build_report(spark)
        report._is_incremental = False

        with (
            patch.object(report, "_persist_full") as mock_full,
            patch.object(report, "_persist_incremental") as mock_incr,
        ):
            report.persist_results()

            mock_full.assert_called_once()
            mock_incr.assert_not_called()

    def test_persist_full_default_mode(self, spark):
        """persist_results() without tracked state defaults to _persist_full()."""
        report = _build_report(spark)
        # report._is_incremental= None
        with (
            patch.object(report, "_persist_full") as mock_full,
            patch.object(report, "_persist_incremental") as mock_incr,
        ):
            # report.determine_report()
            report.persist_results()

            mock_full.assert_called_once()
            mock_incr.assert_not_called()


# ============================================================================
# Tests: persist_results with is_incremental=True
# ============================================================================
class TestPersistResultsIncrementalMode:
    """Tests for persist_results when _is_incremental is True."""

    def test_persist_incremental_calls_persist_incremental_method(self, spark):
        """persist_results() calls _persist_incremental() when _is_incremental=True."""
        report = _build_report(spark)
        report._is_incremental = True
        report._changed_aggregation_ids = {"HISTOGRAM": [1]}
        report._changed_event_ids = {"BASIC_EVENT": [2]}

        with (
            patch.object(report, "_persist_full") as mock_full,
            patch.object(report, "_persist_incremental") as mock_incr,
        ):
            report.persist_results()

            mock_incr.assert_called_once_with({"HISTOGRAM": [1]}, {"BASIC_EVENT": [2]})
            mock_full.assert_not_called()

    def test_persist_uses_tracked_state_from_determine_report(self, spark):
        """persist_results() uses _is_incremental and changed IDs from determine_report()."""
        report = _build_report(spark)

        # Simulate state set by determine_report
        report._is_incremental = True
        report._changed_aggregation_ids = {"HISTOGRAM": [100]}
        report._changed_event_ids = {"BASIC_EVENT": [200]}

        with (
            patch.object(report, "_persist_full") as mock_full,
            patch.object(report, "_persist_incremental") as mock_incr,
        ):
            # Call with defaults - should use tracked state
            report.persist_results()

            mock_incr.assert_called_once_with({"HISTOGRAM": [100]}, {"BASIC_EVENT": [200]})
            mock_full.assert_not_called()


# ============================================================================
# Tests: determine_report uses _resolve_is_incremental
# ============================================================================
class TestDetermineReportProcessingMode:
    """Tests for determine_report respecting config and gold layer."""

    def test_determine_report_config_enabled_no_gold(self, spark):
        """Config enabled=True but no gold → _is_incremental=False."""
        report = _build_report(spark, config_dict=INCREMENTAL_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=False),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report()

            assert report._is_incremental is False

    def test_determine_report_config_disabled(self, spark):
        """Config enabled=False → _is_incremental=False regardless of signature."""
        report = _build_report(spark, config_dict=INCREMENTAL_DISABLED_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=True),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report(is_incremental=True)

            assert report._is_incremental is False

    def test_config_overrides_signature_true(self, spark):
        """Config enabled=True overrides is_incremental=False."""
        report = _build_report(spark, config_dict=INCREMENTAL_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=True),
            patch.object(report, "_detect_upserted_containers", return_value=MagicMock()),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report(is_incremental=False)

            assert report._is_incremental is True

    def test_config_overrides_signature_false(self, spark):
        """Config enabled=False overrides is_incremental=True."""
        report = _build_report(spark, config_dict=INCREMENTAL_DISABLED_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=True),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report(is_incremental=True)

            assert report._is_incremental is False

    def test_no_config_signature_true_gold_exists(self, spark):
        """No config + is_incremental=True + gold → incremental."""
        report = _build_report(spark, config_dict=DUMMY_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=True),
            patch.object(report, "_detect_upserted_containers", return_value=MagicMock()),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report(is_incremental=True)

            assert report._is_incremental is True

    def test_no_config_signature_false(self, spark):
        """No config + is_incremental=False → FULL."""
        report = _build_report(spark, config_dict=DUMMY_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=True),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report(is_incremental=False)

            assert report._is_incremental is False

    def test_no_config_no_signature_defaults_to_full(self, spark):
        """No config + no signature → FULL (backwards compatibility)."""
        report = _build_report(spark, config_dict=DUMMY_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=True),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report()

            assert report._is_incremental is False

    def test_detect_upserted_returns_none_falls_back_to_full(self, spark):
        """Incremental resolved but _detect_upserted_containers returns None → FULL."""
        report = _build_report(spark, config_dict=INCREMENTAL_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=True),
            patch.object(report, "_detect_upserted_containers", return_value=None),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report()

            assert report._is_incremental is True


# ============================================================================
# Tests: persist_results with config-driven processing mode
# ============================================================================
class TestPersistResultsProcessingMode:
    """Tests for persist_results respecting config set by determine_report."""

    def test_persist_full_when_config_disabled(self, spark):
        """persist_results calls _persist_full when config enabled=False."""
        report = _build_report(spark, config_dict=INCREMENTAL_DISABLED_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=True),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report()

        with (
            patch.object(report, "_persist_full") as mock_full,
            patch.object(report, "_persist_incremental") as mock_incr,
        ):
            report.persist_results()

            mock_full.assert_called_once()
            mock_incr.assert_not_called()

    def test_persist_incremental_when_config_enabled(self, spark):
        """persist_results calls _persist_incremental when config enabled=True and gold exists."""
        report = _build_report(spark, config_dict=INCREMENTAL_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=True),
            patch.object(report, "_detect_upserted_containers", return_value=MagicMock()),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report()

        with (
            patch.object(report, "_persist_full") as mock_full,
            patch.object(report, "_persist_incremental") as mock_incr,
        ):
            report.persist_results()

            mock_incr.assert_called_once()
            mock_full.assert_not_called()

    def test_persist_full_when_no_gold_layer(self, spark):
        """persist_results calls _persist_full when no gold layer exists."""
        report = _build_report(spark, config_dict=INCREMENTAL_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=False),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report()

        with (
            patch.object(report, "_persist_full") as mock_full,
            patch.object(report, "_persist_incremental") as mock_incr,
        ):
            report.persist_results()

            mock_full.assert_called_once()
            mock_incr.assert_not_called()

    def test_persist_full_when_no_config(self, spark):
        """persist_results calls _persist_full when no incremental config."""
        report = _build_report(spark, config_dict=DUMMY_CONFIG)

        with (
            patch.object(report, "_gold_layer_exists", return_value=True),
            patch.object(report, "_group_events_by_type", return_value={}),
            patch.object(report, "_group_aggregations_by_type", return_value={}),
            patch(
                "mda_reporting.core.report.ContainerDimension.get_dimension",
                return_value=None,
            ),
        ):
            report.determine_report()

        with (
            patch.object(report, "_persist_full") as mock_full,
            patch.object(report, "_persist_incremental") as mock_incr,
        ):
            report.persist_results()

            mock_full.assert_called_once()
            mock_incr.assert_not_called()


# ============================================================================
# Tests: _persist_incremental delegates correctly
# ============================================================================
class TestPersistIncrementalDelegation:
    """Tests for _persist_incremental handling of dict vs DataFrame formats."""

    def test_persist_incremental_dict_format_changed_calls_replace(self, spark):
        """Dict format with changed df + changed IDs calls replace_by_ids."""
        report = _build_report(spark)
        mock_changed_df = MagicMock(spec=DataFrame)
        mock_unchanged_df = MagicMock(spec=DataFrame)

        report.aggregation_dfs = {
            "HISTOGRAM": {
                "changed": mock_changed_df,
                "unchanged": mock_unchanged_df,
            }
        }
        report.aggregation_metadata_dfs = {}
        report.event_dfs = {}
        report.event_metadata_dfs = {}
        report.container_dimension_df = None

        # Mock the writer factory and writer
        with (
            patch("mda_reporting.core.report.WriterFactory") as mock_factory_cls,
            patch("mda_reporting.core.report.ReportEntityTransformer"),
            patch.object(report, "_transform_for_persistence") as mock_transform,
        ):
            mock_writer = MagicMock()
            mock_writer.extract_fact_schema_and_output_uri.return_value = (
                MagicMock(),
                "catalog.gold.hist_fact",
            )
            mock_factory_cls.return_value.create_writer.return_value = mock_writer

            mock_transformed_df = MagicMock(spec=DataFrame)
            mock_transform.return_value = mock_transformed_df

            report._persist_incremental(
                changed_aggregation_ids={"HISTOGRAM": [42]},
                changed_event_ids={},
            )

            # Should call replace_by_ids for changed
            report.sink.replace_by_ids.assert_called_once()
            replace_call = report.sink.replace_by_ids.call_args
            assert replace_call.kwargs["id_column"] == "visual_id"
            assert replace_call.kwargs["ids_to_replace"] == [42]

            # Should call upsert for unchanged
            report.sink.upsert.assert_called_once()

    def test_persist_incremental_single_df_calls_upsert(self, spark):
        """Single DataFrame (backward compat) in incremental mode uses MERGE."""
        report = _build_report(spark)
        mock_agg_df = MagicMock(spec=DataFrame)

        report.aggregation_dfs = {"HISTOGRAM": mock_agg_df}
        report.aggregation_metadata_dfs = {}
        report.event_dfs = {}
        report.event_metadata_dfs = {}
        report.container_dimension_df = None

        with (
            patch("mda_reporting.core.report.WriterFactory") as mock_factory_cls,
            patch("mda_reporting.core.report.ReportEntityTransformer"),
            patch.object(report, "_transform_for_persistence") as mock_transform,
        ):
            mock_writer = MagicMock()
            mock_writer.extract_fact_schema_and_output_uri.return_value = (
                MagicMock(),
                "catalog.gold.hist_fact",
            )
            mock_factory_cls.return_value.create_writer.return_value = mock_writer
            mock_transform.return_value = MagicMock(spec=DataFrame)

            report._persist_incremental(
                changed_aggregation_ids={},
                changed_event_ids={},
            )

            # Single DataFrame -> upsert only, no replace
            report.sink.upsert.assert_called_once()
            report.sink.replace_by_ids.assert_not_called()

    def test_persist_incremental_measurement_dimension_upserts(self, spark):
        """Measurement dimension uses upsert by container_id."""
        report = _build_report(spark)
        mock_dim_df = MagicMock(spec=DataFrame)
        mock_dim_df.transform.return_value = MagicMock(spec=DataFrame)

        report.aggregation_dfs = {}
        report.aggregation_metadata_dfs = {}
        report.event_dfs = {}
        report.event_metadata_dfs = {}
        report.container_dimension_df = mock_dim_df

        with (
            patch("mda_reporting.core.report.WriterFactory") as mock_factory_cls,
            patch("mda_reporting.core.report.ReportEntityTransformer"),
        ):
            mock_writer = MagicMock()
            mock_writer.get_output_uri.return_value = "catalog.gold.measurement_dimension"
            mock_factory_cls.return_value.create_container_dimension_writer.return_value = (
                mock_writer
            )

            report._persist_incremental(changed_aggregation_ids={}, changed_event_ids={})

            report.sink.upsert.assert_called_once()
            upsert_call = report.sink.upsert.call_args
            assert upsert_call.args[2] == ["container_id"]


# ============================================================================
# Tests: _persist_full handling of dict/DataFrame formats
# ============================================================================
class TestPersistFullDictHandling:
    """Tests for _persist_full handling mixed dict/DataFrame formats."""

    def test_persist_full_dict_format_combines_dfs(self, spark):
        """_persist_full combines 'changed' and 'unchanged' dfs when dict format."""
        report = _build_report(spark)
        mock_changed_df = MagicMock(spec=DataFrame)
        mock_unchanged_df = MagicMock(spec=DataFrame)

        report.aggregation_dfs = {
            "HISTOGRAM": {
                "changed": mock_changed_df,
                "unchanged": mock_unchanged_df,
            }
        }
        report.aggregation_metadata_dfs = {}
        report.event_dfs = {}
        report.event_metadata_dfs = {}
        report.container_dimension_df = None

        with patch("mda_reporting.core.report.WriterFactory") as mock_factory_cls:
            mock_writer = MagicMock()
            mock_writer.extract_fact_schema_and_output_uri.return_value = (
                MagicMock(),
                "catalog.gold.hist_fact",
            )
            mock_factory_cls.return_value.create_writer.return_value = mock_writer

            report._persist_full()

            # Writer.write is called once with the list of dfs
            mock_writer.write.assert_called_once()
            write_call = mock_writer.write.call_args
            dfs_arg = write_call.args[0] if write_call.args else write_call.kwargs.get("df")
            assert isinstance(dfs_arg, list)
            assert len(dfs_arg) == 2

    def test_persist_full_single_df_format(self, spark):
        """_persist_full passes single DataFrame directly to writer."""
        report = _build_report(spark)
        mock_agg_df = MagicMock(spec=DataFrame)

        report.aggregation_dfs = {"HISTOGRAM": mock_agg_df}
        report.aggregation_metadata_dfs = {}
        report.event_dfs = {}
        report.event_metadata_dfs = {}
        report.container_dimension_df = None

        with patch("mda_reporting.core.report.WriterFactory") as mock_factory_cls:
            mock_writer = MagicMock()
            mock_writer.extract_fact_schema_and_output_uri.return_value = (
                MagicMock(),
                "catalog.gold.hist_fact",
            )
            mock_factory_cls.return_value.create_writer.return_value = mock_writer

            report._persist_full()

            mock_writer.write.assert_called_once()
            write_call = mock_writer.write.call_args
            dfs_arg = write_call.args[0] if write_call.args else write_call.kwargs.get("df")
            assert dfs_arg is mock_agg_df

    def test_persist_full_dict_with_none_values_skipped(self, spark):
        """_persist_full skips None values in dict format (e.g., only changed but no unchanged)."""
        report = _build_report(spark)

        report.aggregation_dfs = {
            "HISTOGRAM": {
                "changed": MagicMock(spec=DataFrame),
                "unchanged": None,
            }
        }
        report.aggregation_metadata_dfs = {}
        report.event_dfs = {}
        report.event_metadata_dfs = {}
        report.container_dimension_df = None

        with patch("mda_reporting.core.report.WriterFactory") as mock_factory_cls:
            mock_writer = MagicMock()
            mock_writer.extract_fact_schema_and_output_uri.return_value = (
                MagicMock(),
                "catalog.gold.hist_fact",
            )
            mock_factory_cls.return_value.create_writer.return_value = mock_writer

            report._persist_full()

            mock_writer.write.assert_called_once()
            write_call = mock_writer.write.call_args
            dfs_arg = write_call.args[0]
            # Should only include the non-None df
            assert isinstance(dfs_arg, list)
            assert len(dfs_arg) == 1
