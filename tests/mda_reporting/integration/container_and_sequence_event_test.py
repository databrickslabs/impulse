"""Integration tests for ContainerEvent with end-to-end Report usage."""

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from mda_reporting.config.config_parser import (
    Comparator,
    ContainerFilters,
    MdaConfig,
    MeasurementDimensions,
    MetricFilter,
    QueryEngine,
    Solvers,
    Source,
    UnitySink,
)
from mda_reporting.core.report import Report
from mda_reporting.events.container_event import ContainerEvent
from mda_reporting.events.sequence_of_events import SequenceOfEvents
from tests.conftest import spark


def test_container_event_in_report(spark, basic_narrow_db):
    """Test ContainerEvent integration within a Report.

    Verifies that:
    - Three container events are created (one for each measurement)
    - All event start and end times match measurement start and end times
    - All event_instance_ids are -1
    """
    # Create report configuration
    mda_config = MdaConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver.container_metrics",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(
            catalog="spark_catalog",
            schema="gold",
            table_prefix="container_event_test",
        ),
        container_filters=ContainerFilters(
            metric_filters=[
                [
                    MetricFilter(
                        column_name="vehicle_key",
                        comparator=Comparator.EQ,
                        value="Seat_Leon",
                    ),
                    MetricFilter(
                        column_name="start_dt",
                        comparator=Comparator.GE,
                        value="2025-07-03T07:00:00.000Z",
                    ),
                ]
            ]
        ),
        query_engine=QueryEngine(solver=Solvers.KEY_VALUE_STORE_SOLVER),
        measurement_dimensions=[
            MeasurementDimensions.CONTAINER_ID,
            MeasurementDimensions.START_TS,
            MeasurementDimensions.STOP_TS,
        ],
    )

    # Create report and add container event
    my_report = Report(
        name="container_event_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(mda_config),
    )

    # Add a ContainerEvent
    container_evt = ContainerEvent(name="full_measurement", desc="Full measurement duration")
    my_report.add_event(container_evt)

    # Determine report content
    my_report.determine_report()

    # Access event DataFrames
    event_dfs = my_report.event_dfs
    assert "CONTAINER_EVENT" in event_dfs, "CONTAINER_EVENT should be in event_dfs"

    # Get the event fact DataFrame
    event_fact_df = event_dfs["CONTAINER_EVENT"]["changed"]
    assert event_fact_df is not None, "Event fact DataFrame should not be None"

    # Collect rows for assertions
    event_rows = event_fact_df.collect()

    # Assertion 1: Three container events (one for each measurement)
    assert len(event_rows) == 3, f"Expected 3 event instances, got {len(event_rows)}"

    # Assertion 2: Event start and end times match container timestamps
    # Expected container data from container_metrics.csv:
    # Container 1: start_ts=1751528502708, stop_ts=1751528610253
    # Container 2: start_ts=1751528501483, stop_ts=1751528610235
    # Container 3: start_ts=1751528500169, stop_ts=1751528610252
    expected_containers = {
        1: {"start_ts": 1751528502708, "end_ts": 1751528610253},
        2: {"start_ts": 1751528501483, "end_ts": 1751528610235},
        3: {"start_ts": 1751528500169, "end_ts": 1751528610252},
    }

    for row in event_rows:
        container_id = row.container_id
        assert container_id in expected_containers, f"Unexpected container_id: {container_id}"

        expected = expected_containers[container_id]
        assert row.start_ts == expected["start_ts"], (
            f"Container {container_id}: expected start_ts={expected['start_ts']}, "
            f"got {row.start_ts}"
        )
        assert row.end_ts == expected["end_ts"], (
            f"Container {container_id}: expected end_ts={expected['end_ts']}, " f"got {row.end_ts}"
        )

    # Verify event metadata DataFrame
    event_metadata_dfs = my_report.event_metadata_dfs
    assert (
        "CONTAINER_EVENT" in event_metadata_dfs
    ), "CONTAINER_EVENT should be in event_metadata_dfs"

    event_dim_df = event_metadata_dfs["CONTAINER_EVENT"]
    assert event_dim_df is not None, "Event dimension DataFrame should not be None"

    dim_rows = event_dim_df.collect()
    assert len(dim_rows) == 1, f"Expected 1 event definition, got {len(dim_rows)}"

    dim_row = dim_rows[0]
    assert dim_row.event_name == "full_measurement"
    assert dim_row.event_expression == "NA"
    assert dim_row.required_channels is None


def test_container_event_with_basic_event(spark, basic_narrow_db):
    """Test ContainerEvent alongside BasicEvent in the same report.

    Verifies that both event types can coexist and write to the same tables.
    """
    mda_config = MdaConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver.container_metrics",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(
            catalog="spark_catalog",
            schema="gold",
            table_prefix="mixed_events_test",
        ),
        container_filters=ContainerFilters(
            metric_filters=[
                [
                    MetricFilter(
                        column_name="vehicle_key",
                        comparator=Comparator.EQ,
                        value="Seat_Leon",
                    ),
                    MetricFilter(
                        column_name="start_dt",
                        comparator=Comparator.GE,
                        value="2025-07-03T07:00:00.000Z",
                    ),
                ]
            ]
        ),
        query_engine=QueryEngine(solver=Solvers.KEY_VALUE_STORE_SOLVER),
        measurement_dimensions=[
            MeasurementDimensions.CONTAINER_ID,
            MeasurementDimensions.START_TS,
            MeasurementDimensions.STOP_TS,
        ],
    )

    my_report = Report(
        name="mixed_events_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(mda_config),
    )

    # Add a ContainerEvent
    container_evt = ContainerEvent(name="container_duration")
    my_report.add_event(container_evt)

    # Add a BasicEvent
    from mda_reporting.events.basic_event import BasicEvent

    eng_rpm = my_report.get_db().query.channel(channel_name="Engine RPM")
    basic_evt = BasicEvent(name="high_rpm", expr=eng_rpm > 800)
    my_report.add_event(basic_evt)

    # Determine report content
    my_report.determine_report()
    my_report.persist_results()
    event_dfs = my_report.event_dfs

    # Both event types should exist
    assert "CONTAINER_EVENT" in event_dfs
    assert "BASIC_EVENT" in event_dfs

    # BasicEvent instances: should have non-negative event_instance_id (CRC32 hashes)
    basic_rows = event_dfs["BASIC_EVENT"]["changed"].collect()
    assert len(basic_rows) > 0, "Should have at least one basic event instance"
    for row in basic_rows:
        assert (
            row.event_instance_id >= 0
        ), f"BasicEvent should have non-negative event_instance_id, got {row.event_instance_id}"

    # Event metadata: should have 2 event definitions
    event_metadata_dfs = my_report.event_metadata_dfs

    container_dim = event_metadata_dfs["CONTAINER_EVENT"].collect()
    basic_dim = event_metadata_dfs["BASIC_EVENT"].collect()

    assert len(container_dim) == 1
    assert len(basic_dim) == 1

    assert container_dim[0].event_name == "container_duration"
    assert basic_dim[0].event_name == "high_rpm"


def test_sequence_of_events_without_max_overlap_in_report(spark, basic_narrow_db):
    """Test SequenceOfEvents integration in Report without max_overlap filtering."""
    mda_config = MdaConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver.container_metrics",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(
            catalog="spark_catalog",
            schema="gold",
            table_prefix="sequence_events_test",
        ),
        container_filters=ContainerFilters(
            metric_filters=[
                [
                    MetricFilter(
                        column_name="vehicle_key",
                        comparator=Comparator.EQ,
                        value="Seat_Leon",
                    ),
                    MetricFilter(
                        column_name="start_dt",
                        comparator=Comparator.GE,
                        value="2025-07-03T07:00:00.000Z",
                    ),
                ]
            ]
        ),
        query_engine=QueryEngine(solver=Solvers.KEY_VALUE_STORE_SOLVER),
        measurement_dimensions=[
            MeasurementDimensions.CONTAINER_ID,
            MeasurementDimensions.START_TS,
            MeasurementDimensions.STOP_TS,
        ],
    )

    my_report = Report(
        name="sequence_event_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(mda_config),
    )

    veh_spd = my_report.get_db().query.channel(channel_name="Vehicle Speed Sensor")

    sequence_evt = SequenceOfEvents(
        name="speed_transition",
        expressions=[
            (veh_spd > 0) & (veh_spd < 15),
            (veh_spd > 9) & (veh_spd < 18),
        ],
    )
    my_report.add_event(sequence_evt)

    my_report.determine_report()

    event_dfs = my_report.event_dfs

    assert "SEQUENCE_OF_EVENTS" in event_dfs
    sequence_df = event_dfs["SEQUENCE_OF_EVENTS"]["changed"]
    assert sequence_df is not None
    assert "container_id" in sequence_df.columns
    assert "start_ts" in sequence_df.columns
    assert "end_ts" in sequence_df.columns

    # Container 1 should have at least 3 sequence event instances
    container_1_count = sequence_df.filter(sequence_df.container_id == 1).count()
    assert container_1_count == 3, f"Expected 3 events for container_id=1, got {container_1_count}"

    event_metadata_dfs = my_report.event_metadata_dfs
    assert "SEQUENCE_OF_EVENTS" in event_metadata_dfs
    dim_rows = event_metadata_dfs["SEQUENCE_OF_EVENTS"].collect()
    assert len(dim_rows) == 1
    assert dim_rows[0].event_name == "speed_transition"
    assert dim_rows[0].event_type == "SEQUENCE_OF_EVENTS"


def test_report_rejects_two_container_events():
    """Adding two ContainerEvents to the same report should raise a ValueError."""
    DUMMY_CONFIG = {
        "source": {
            "container_metrics_table": "avl_databricks_mvp.silver.container_metric",
            "channel_metrics_table": "avl_databricks_mvp.silver.channel_metric",
            "channels_uri": "avl_databricks_mvp.silver.channel_data",
        },
        "unity_sink": {
            "catalog": "test_catalog",
            "schema": "test_schema",
            "table_prefix": "test_prefix",
        },
        "container_filters": {
            "metric_filters": [
                [
                    {"column_name": "uut_id", "comparator": "==", "value": "123"},
                    {
                        "column_name": "start_ts",
                        "comparator": ">=",
                        "value": "2025-04-27T05:20:54.000Z",
                    },
                    {
                        "column_name": "end_ts",
                        "comparator": "<=",
                        "value": "2025-04-27T05:21:00.000Z",
                    },
                ]
            ]
        },
    }

    report = Report(
        name="test_report",
        spark=None,
        workspace_client=create_autospec(WorkspaceClient),
        config=DUMMY_CONFIG,
    )

    event1 = ContainerEvent(name="container_event_1", desc="First container event")
    event2 = ContainerEvent(name="container_event_2", desc="Second container event")

    report.add_event(event1)

    with pytest.raises(ValueError, match="Only one ContainerEvent is allowed per report"):
        report.add_event(event2)
