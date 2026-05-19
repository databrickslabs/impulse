"""Helper functions for integration tests to reduce code duplication."""

from unittest.mock import create_autospec

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from impulse_reporting.aggregations.histogram import (
    HistogramCustomWeights,
    HistogramDistance,
    HistogramDuration,
)
from impulse_reporting.aggregations.histogram2d import (
    Histogram2DCustomWeights,
    Histogram2DDistance,
    Histogram2DDuration,
)
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.config.config_parser import (
    ImpulseConfig,
    Source,
    UnitySink,
)
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.event import Event


def create_default_report(
    spark: SparkSession, report_name: str = "my_report"
) -> tuple[Report, dict]:
    """
    Create a report with default configuration and return the report along with channel expressions.

    Parameters
    ----------
    spark : SparkSession
        Spark session for data processing.
    report_name : str, optional
        Name of the report (default is "my_report").

    Returns
    -------
    tuple
        A tuple containing:
        - report: Report object with default configuration
        - channels: dict with channel expressions:
            - 'engine_rpm': Engine RPM channel
            - 'vehicle_speed': Vehicle Speed Sensor channel
            - 'weights': Vehicle Speed Sensor channel (for weights)
            - 'veh_spd_event_expr': vehicle speed > 1 expression
    """
    config = ImpulseConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver.container_metrics",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(
            catalog="spark_catalog",
            schema="gold",
            table_prefix="evaluation",
        ),
    )

    report = Report(
        name=report_name,
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(config),
    )

    # Definition of relevant channels
    query = report.get_db().query
    engine_rpm = query.channel(channel_name="Engine RPM")
    vehicle_speed = query.channel(channel_name="Vehicle Speed Sensor")
    weights = query.channel(channel_name="Vehicle Speed Sensor")  # weights channel
    veh_spd_event_expr = vehicle_speed > 1

    channels = {
        "engine_rpm": engine_rpm,
        "vehicle_speed": vehicle_speed,
        "weights": weights,
        "veh_spd_event_expr": veh_spd_event_expr,
    }

    return report, channels


def add_histograms_aggregations(
    report: Report,
    engine_rpm,
    vehicle_speed,
    weights,
    event: Event | None = None,
    page_number: int = 1,
    stats_aggregator_config: dict | None = None,
) -> Report:
    """
    Add all standard histogram aggregations and optionally a StatsAggregator to a report.

    This function adds:
    - 1D histograms: RPM, Speed, Custom Weights, Distance
    - 2D histograms: RPM vs Speed (Duration, Custom Weights, Distance)
    - StatsAggregator (optional): If stats_aggregator_config is provided

    Parameters
    ----------
    report : Report
        The report object to add aggregations to.
    engine_rpm : TimeSeriesExpression
        Engine RPM channel expression.
    vehicle_speed : TimeSeriesExpression
        Vehicle Speed Sensor channel expression.
    weights : TimeSeriesExpression
        Weights channel expression.
    event : Event, optional
        Optional event to filter aggregations (default is None).
    page_number : int, optional
        Page number to add aggregations to (default is 1).
    stats_aggregator_config : dict, optional
        Configuration for StatsAggregator. If provided, should contain:
        - name: str - Name of the stats aggregator
        - input_expressions: list - List of channel expressions
        - statistics: list - List of statistics to compute (e.g., ["min", "max", "mean"])
        - channel_names: list - List of signal names
        - desc: str (optional) - Description
        - values_unit: str (optional) - Unit of values
        - event: Event (optional) - Event to filter by (defaults to the event parameter)

    Returns
    -------
    Report
        The report object with aggregations added.
    """
    # Definition of page
    my_first_page = Page(page_number=page_number)
    report.add_page(my_first_page)

    # Common bin definitions
    rpm_bins = [float(i) for i in range(0, 8000, 250)]
    speed_bins = [float(i) for i in range(0, 300, 1)]
    x_bins = rpm_bins
    y_bins = speed_bins

    # RPM histogram
    hist1 = HistogramDuration(
        name="rpm_hist_p1",
        base_expr=engine_rpm,
        bins=rpm_bins,
        event=event,
    )
    my_first_page.add_aggregation(hist1)

    # Speed histogram
    hist2 = HistogramDuration(
        name="speed_hist_p1",
        base_expr=vehicle_speed,
        bins=speed_bins,
        event=event,
    )
    my_first_page.add_aggregation(hist2)

    # Custom weights histogram
    hist_custom = HistogramCustomWeights(
        name="custom_weights_hist_p1",
        base_expr=engine_rpm,
        weights_expr=weights,
        bins=rpm_bins,
        event=event,
    )
    my_first_page.add_aggregation(hist_custom)

    # Distance histogram
    hist_distance = HistogramDistance(
        name="distance_hist_p1",
        base_expr=engine_rpm,
        weights_expr=weights,
        bins=rpm_bins,
        event=event,
    )
    my_first_page.add_aggregation(hist_distance)

    # 2D histogram (Duration)
    hist2d = Histogram2DDuration(
        name="rpm_vs_speed_hist",
        x_expr=engine_rpm,
        y_expr=vehicle_speed,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    my_first_page.add_aggregation(hist2d)

    # 2D histogram with custom weights
    hist2d_custom = Histogram2DCustomWeights(
        name="rpm_vs_speed_custom_weights_hist",
        x_expr=engine_rpm,
        y_expr=vehicle_speed,
        weights_expr=weights,
        x_bins=x_bins,
        y_bins=y_bins,
        event=event,
    )
    my_first_page.add_aggregation(hist2d_custom)

    # 2D histogram weighted by distance
    hist2d_distance = Histogram2DDistance(
        name="rpm_vs_speed_distance_hist",
        x_expr=engine_rpm,
        y_expr=vehicle_speed,
        weights_expr=weights,
        x_bins=x_bins,
        y_bins=y_bins,
        event=event,
    )
    my_first_page.add_aggregation(hist2d_distance)

    # Add StatsAggregator if config is provided
    if stats_aggregator_config is not None:
        stats_event = stats_aggregator_config.get("event", event)
        stats_agg = StatsAggregator(
            name=stats_aggregator_config["name"],
            input_expressions=stats_aggregator_config["input_expressions"],
            statistics=stats_aggregator_config["statistics"],
            event=stats_event,
            desc=stats_aggregator_config.get("desc", ""),
            channel_names=stats_aggregator_config["channel_names"],
            values_unit=stats_aggregator_config.get("values_unit", ""),
        )
        my_first_page.add_aggregation(stats_agg)

    return report
