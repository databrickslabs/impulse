"""The benchmark report on the Renumics fleet telemetry (100 trips, epoch-µs timestamps).

A mix of the report features: container, interval, point and time-window events, 1D and 2D
histograms, statistics per event, point values, calculated channels, and statistics of 40
signals resampled to 1 Hz in 4 aggregations of 10 (the resamples can be pushed down to Spark).
Channel batching is Impulse's default: all ~50 channels of a trip in one solve.

Bump `BENCHMARK_VERSION` on any change of the report or its config: runs only compare with
runs of the same version.
"""

import math

from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.aggregations.histogram2d import Histogram2DDuration
from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.channels.calculated_channel import CalculatedChannel
from impulse_reporting.core.page import Page
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.events.container_event import ContainerEvent
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent
from impulse_reporting.events.time_window_event import TimeWindowEvent

BENCHMARK_VERSION = 2

US = 1e6  # Timestamps are epoch microseconds.

MEASUREMENT_DIMENSIONS = [
    "container_id",
    "vehicle_key",
    "manufacturer",
    "powertrain_type",
    "start_ts",
    "stop_ts",
]

# Signals per StatsAggregator of the 1 Hz overview.
OVERVIEW_GROUP = 10

# 40 signals at ~100 Hz, logged in every trip.
WIDE_SIGNALS = [
    *[f"Car.{s}" for s in ["ax", "ay", "az", "YawRate", "YawVel", "YawAcc"]],
    *[f"Car.{s}" for s in ["PitchVel", "PitchAcc", "RollAcc"]],
    *[f"Car.Aero.Frc_1.{a}" for a in "xyz"],
    *[f"Vhcl.PoI.{s}" for s in ["ax", "ay", "az", "vx", "vy", "vz"]],
    *[f"Vhcl.PoI.{s}_1" for s in ["ax", "ay", "az", "vx", "vy", "vz"]],
    *[f"Vhcl.{w}.t{a}" for w in ["FL", "FR"] for a in "xyz"],
    *[f"PwrD.Susp.Total{w}" for w in ["", "FL", "FR", "RL", "RR"]],
    *[f"PwrL.{s}" for s in ["Susp", "Tire", "Aero"]],
    *[f"Vhcl.{s}" for s in ["YawRate", "RollVel"]],
]


def _bins(start, stop, step):
    return [float(start + i * step) for i in range(round((stop - start) / step) + 1)]


def build_report(report):
    """Adds the benchmark's events, calculated channels and pages to `report`."""
    query = report.get_db().query
    ch = lambda name: query.channel(channel_name=name)

    # Signals. The speed carries implausible spikes > 2000 m/s, the battery temperature is
    # logged in Kelvin and the SOC is 0 while the BMS initialises.
    v = ch("Vhcl.v")
    speed_kmh = v.where(v < 100) * 3.6
    current, voltage = ch("PT.BattHV.Current"), ch("PT.PwrSupply.HV1.Voltage")
    soc = ch("PT.BCU.BattHV.SOC")
    soc = soc.where(soc > 0)
    batt_temp_c = ch("PT.BattHV.Temp") - 273.15
    hv_power_kw = voltage * current / 1000
    ax, ay = ch("Car.ax"), ch("Car.ay")
    engine_rpm = ch("PT.Engine.rotv") * (60 / (2 * math.pi))
    gas, brake, steer = ch("VC.Gas"), ch("VC.Brake"), ch("Vhcl.Steer.Ang")
    distance_km = ch("Vhcl.Distance") / 1000

    # Events.
    trip = ContainerEvent(name="trip", desc="Whole trip")
    driving = BasicEvent(
        name="driving_cycle",
        expr=(speed_kmh > 1).merge_intervals(30 * US).filter(60 * US),
        desc="Moving > 1 km/h; stops < 30 s merged, cycles >= 60 s",
    )
    charging = BasicEvent(
        name="charging",
        expr=((current < -10) & (v < 0.1)).filter(60 * US),
        desc="HV current < -10 A at standstill for at least 60 s",
    )
    recuperation = BasicEvent(
        name="recuperation",
        expr=(current < -5) & (speed_kmh > 5),
        desc="HV current < -5 A above 5 km/h",
    )
    slow_down = PointsInTimeEvent(
        name="slow_down", expr=(speed_kmh > 50).end_points(), desc="Speed drops to 50 km/h"
    )
    five_min = TimeWindowEvent(name="five_min", window_length=300 * US, desc="5-minute windows")
    for event in [trip, driving, charging, recuperation, slow_down, five_min]:
        report.add_event(event)

    for name, expr in [("hv_power_kw", hv_power_kw), ("engine_rpm", engine_rpm)]:
        report.add_calculated_channel(
            CalculatedChannel(name=name, expr=expr, identity={"channel_name": name})
        )

    # Driving.
    page = Page(page_number=1)
    page.add_aggregation(
        HistogramDuration(
            name="speed_hist",
            base_expr=speed_kmh,
            bins=_bins(0, 260, 10),
            event=driving,
            channel_name="speed_kmh",
            bins_unit="km/h",
            values_unit="us",
        )
    )
    page.add_aggregation(
        HistogramDuration(
            name="accel_hist",
            base_expr=ax,
            bins=_bins(-10, 10, 0.5),
            event=trip,
            channel_name="Car.ax",
            bins_unit="m/s^2",
            values_unit="us",
        )
    )
    page.add_aggregation(
        Histogram2DDuration(
            name="speed_accel",
            x_expr=speed_kmh,
            y_expr=ax,
            x_bins=_bins(0, 260, 10),
            y_bins=_bins(-10, 10, 0.5),
            event=driving,
            x_channel_name="speed_kmh",
            y_channel_name="Car.ax",
            values_unit="us",
        )
    )
    page.add_aggregation(
        Histogram2DDuration(
            name="rpm_speed",
            x_expr=engine_rpm,
            y_expr=speed_kmh,
            x_bins=_bins(0, 8000, 250),
            y_bins=_bins(0, 260, 10),
            event=trip,
            x_channel_name="engine_rpm",
            y_channel_name="speed_kmh",
            values_unit="us",
        )
    )
    page.add_aggregation(
        Histogram2DDuration(
            name="rpm_gas",
            x_expr=engine_rpm,
            y_expr=gas,
            x_bins=_bins(0, 8000, 250),
            y_bins=_bins(0, 1, 0.1),
            event=trip,
            x_channel_name="engine_rpm",
            y_channel_name="VC.Gas",
            values_unit="us",
        )
    )
    driving_signals = {
        "speed_kmh": speed_kmh,
        "Car.ax": ax,
        "Car.ay": ay,
        "distance_km": distance_km,
        "engine_rpm": engine_rpm,
        "VC.Gas": gas,
        "VC.Brake": brake,
        "Vhcl.Steer.Ang": steer,
    }
    page.add_aggregation(
        StatsAggregator(
            name="driving_stats",
            input_expressions=list(driving_signals.values()),
            channel_names=list(driving_signals),
            statistics=["min", "mean", "max"],
            event=driving,
        )
    )
    page.add_aggregation(
        StatsAggregator(
            name="window_stats",
            input_expressions=[speed_kmh, engine_rpm],
            channel_names=["speed_kmh", "engine_rpm"],
            statistics=["mean", "max"],
            event=five_min,
        )
    )
    page.add_aggregation(
        PointValueAggregator(
            name="slow_down_values",
            input_expressions=[speed_kmh, ax, engine_rpm],
            channel_names=["speed_kmh", "Car.ax", "engine_rpm"],
            event=slow_down,
        )
    )
    report.add_page(page)

    # HV battery (electric and hybrid vehicles).
    page = Page(page_number=2)
    battery_signals = {
        "soc": soc,
        "hv_power_kw": hv_power_kw,
        "current_a": current,
        "voltage_v": voltage,
        "batt_temp_c": batt_temp_c,
    }
    page.add_aggregation(
        StatsAggregator(
            name="charging_stats",
            input_expressions=list(battery_signals.values()),
            channel_names=list(battery_signals),
            statistics=["min", "mean", "max"],
            event=charging,
        )
    )
    for name, event in [
        ("charging_power_hist", charging),
        ("recuperation_power_hist", recuperation),
    ]:
        page.add_aggregation(
            HistogramDuration(
                name=name,
                base_expr=hv_power_kw,
                bins=_bins(-400, 400, 20),
                event=event,
                channel_name="hv_power_kw",
                bins_unit="kW",
                values_unit="us",
            )
        )
    report.add_page(page)

    # Overview of 40 signals at 1 Hz, in aggregations of 10.
    page = Page(page_number=3)
    for i in range(0, len(WIDE_SIGNALS), OVERVIEW_GROUP):
        names = WIDE_SIGNALS[i : i + OVERVIEW_GROUP]
        page.add_aggregation(
            StatsAggregator(
                name=f"trip_overview_1hz_{i // OVERVIEW_GROUP}",
                input_expressions=[ch(name).resample(1 * US) for name in names],
                channel_names=names,
                statistics=["min", "mean", "max"],
                event=trip,
            )
        )
    report.add_page(page)
    return report
