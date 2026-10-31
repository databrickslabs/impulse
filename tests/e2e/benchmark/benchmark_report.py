"""The benchmark reports on the Renumics fleet telemetry (100 trips, epoch-µs timestamps).

- `build_report` (`full`, `incremental_noop`, `incremental_delta`): a mix of the report features,
  ~50 channels per trip in one solve.
- `build_wide_report` (`wide_report`): 150 signals, many events and histograms, a chain of
  calculated channels.
- `build_udf_report` (`python_udfs`): Python extensions (`benchmark_udfs.py`).

Bump `BENCHMARK_VERSION` on any change of a report, its config or the scenarios.
"""

import math

import benchmark_udfs as udfs
import numpy as np
from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesExpression
from impulse_query_engine.analyze.query.aggregations.custom_statistic import (
    CrossChannelStatistic,
    PerChannelStatistic,
)

from impulse_reporting.aggregations.histogram import (
    HistogramCustomWeights,
    HistogramDistance,
    HistogramDuration,
)
from impulse_reporting.aggregations.histogram2d import Histogram2DDistance, Histogram2DDuration
from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.channels.calculated_channel import CalculatedChannel
from impulse_reporting.core.page import Page
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.events.container_event import ContainerEvent
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent
from impulse_reporting.events.time_window_event import TimeWindowEvent

BENCHMARK_VERSION = 3

# Scenarios of each run task (by task key), in order, and their schema suffix.
TASKS = {
    "run": ("", ["full", "incremental_noop", "incremental_delta"]),
    "run_wide": ("_wide", ["wide_report"]),
    "run_udfs": ("_udfs", ["python_udfs"]),
}
SCENARIOS = [scenario for _, scenarios in TASKS.values() for scenario in scenarios]

# Scenarios per compute. Serverless skips `wide_report`: its workers run out of memory.
COMPUTE_SCENARIOS = {
    "cluster": SCENARIOS,
    "serverless": [s for s in SCENARIOS if s != "wide_report"],
}

# Container filters of a scenario, by compute, added to the compute's; without, all trips. Trip
# 39 (4.35 h) runs the wide report's calculated channels out of executor memory.
SCENARIO_FILTERS = {
    "cluster": {"wide_report": [{"column_name": "container_id", "comparator": "!=", "value": 39}]},
    "serverless": {
        "python_udfs": [{"column_name": "container_id", "comparator": "<", "value": 30}]
    },
}

# Trips re-ingested before `incremental_delta`: 5 %, of all powertrains and of typical lengths.
DELTA_CONTAINERS = [7, 16, 28, 45, 73]

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
    # Drop zero-length intervals at a trip's end: their end points duplicate the previous ones.
    slow_down = PointsInTimeEvent(
        name="slow_down",
        expr=(speed_kmh > 50).filter(1).end_points(),
        desc="Speed drops to 50 km/h",
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


# The wide report: statistics of the selected signals in chunks (an aggregation is solved in one
# batch), and threshold events, histograms and statistics for a subset.
WIDE_SIGNAL_COUNT = 150
WIDE_STATS_CHUNK = 50
WIDE_THRESHOLD_SIGNALS = 60
WIDE_DISTANCE_SIGNALS = 20
WIDE_CHAIN_LENGTH = 20


def wide_signal_stats(spark, source):
    """The signals logged in every trip, sorted by name, with medians over the trips of their
    `channel_metrics` (`std`, `min`, `max`, `pz10`, `pz90`). Deterministic, as `setup` checks
    the dataset."""
    rows = spark.sql(f"""
        SELECT channel_name AS name, median(std) AS std, median(min) AS lo, median(max) AS hi,
               median(pz10) AS p10, median(pz90) AS p90
        FROM {source}channel_metrics
        GROUP BY channel_name
        HAVING count(DISTINCT container_id) = (SELECT count(*) FROM {source}container_metrics)
        ORDER BY channel_name
        """).collect()
    return [r.asDict() for r in rows]


def _round(x):
    return float(f"{x:.6g}")


def build_wide_report(report, signals, max_channels=None):
    """Adds the wide report to `report`; `signals` as returned by `wide_signal_stats`.

    With `max_channels` (`max_channels_per_batch`), the statistics chunks and the chain are
    shortened to fit a batch, as an aggregation is solved in one batch.
    """
    stats_chunk = min(WIDE_STATS_CHUNK, max_channels or WIDE_STATS_CHUNK)
    chain_length = min(WIDE_CHAIN_LENGTH, (max_channels or WIDE_CHAIN_LENGTH + 1) - 1)
    query = report.get_db().query
    ch = lambda name: query.channel(channel_name=name)
    varying = [
        s
        for s in signals
        if s["std"] and s["hi"] > s["lo"] and s["p90"] is not None and s["p90"] > s["p10"]
    ]
    # WIDE_SIGNAL_COUNT of the varying signals, evenly spaced over the sorted names.
    n = min(WIDE_SIGNAL_COUNT, len(varying))
    varying = [varying[i * len(varying) // n] for i in range(n)]
    pick = lambda n, offset=0: [
        varying[(offset + i * len(varying) // n) % len(varying)] for i in range(n)
    ]

    trip = ContainerEvent(name="trip", desc="Whole trip")
    report.add_event(trip)

    # Statistics of the selected signals over the trip.
    page = Page(page_number=1)
    names = [s["name"] for s in varying]
    for i in range(0, len(names), stats_chunk):
        chunk = names[i : i + stats_chunk]
        page.add_aggregation(
            StatsAggregator(
                name=f"signal_stats_{i // stats_chunk:02d}",
                input_expressions=[ch(n) for n in chunk],
                channel_names=chunk,
                statistics=["start", "end", "min", "max", "mean"],
                event=trip,
            )
        )
    report.add_page(page)

    # Limit events (above p90, below p10) with stats of 5 signals, and a histogram per signal.
    page = Page(page_number=2)
    index = {s["name"]: i for i, s in enumerate(varying)}
    for i, s in enumerate(pick(WIDE_THRESHOLD_SIGNALS)):
        signal = ch(s["name"])
        group = [varying[(index[s["name"]] + k) % len(varying)]["name"] for k in range(5)]
        for side, expr in [
            ("high", signal > _round(s["p90"])),
            ("low", signal < _round(s["p10"])),
        ]:
            event = BasicEvent(
                name=f"limit_{i:02d}_{side}",
                expr=expr.debounce(0.5 * US).merge_intervals(2 * US),
                desc=f"{s['name']} {'above p90' if side == 'high' else 'below p10'}",
            )
            report.add_event(event)
            page.add_aggregation(
                StatsAggregator(
                    name=f"limit_{i:02d}_{side}_stats",
                    input_expressions=[ch(n) for n in group],
                    channel_names=group,
                    statistics=["min", "max", "mean"],
                    event=event,
                )
            )
        page.add_aggregation(
            HistogramDuration(
                name=f"limit_{i:02d}_hist",
                base_expr=signal,
                bins=[_round(b) for b in np.linspace(s["lo"], s["hi"], 101)],
                event=trip,
                channel_name=s["name"],
                values_unit="us",
            )
        )
    report.add_page(page)

    # Distance-weighted and custom-weighted histograms.
    page = Page(page_number=3)
    distance = ch("Vhcl.Distance")
    bins = lambda s, n=21: [_round(b) for b in np.linspace(s["lo"], s["hi"], n)]
    distance_signals = pick(
        WIDE_DISTANCE_SIGNALS, offset=len(varying) // (2 * WIDE_DISTANCE_SIGNALS)
    )
    for i, s in enumerate(distance_signals):
        page.add_aggregation(
            HistogramDistance(
                name=f"distance_hist_{i:02d}",
                base_expr=ch(s["name"]),
                weights_expr=distance,
                bins=bins(s),
                event=trip,
                channel_name=s["name"],
                weights_channel_name="Vhcl.Distance",
            )
        )
    for i in range(4):
        x, y = distance_signals[2 * i], distance_signals[2 * i + 1]
        page.add_aggregation(
            Histogram2DDistance(
                name=f"distance_2d_{i}",
                x_expr=ch(x["name"]),
                y_expr=ch(y["name"]),
                weights_expr=distance,
                x_bins=bins(x),
                y_bins=bins(y),
                event=trip,
                x_channel_name=x["name"],
                y_channel_name=y["name"],
            )
        )
    v = ch("Vhcl.v")
    hv_power_kw = ch("PT.PwrSupply.HV1.Voltage") * ch("PT.BattHV.Current") / 1000
    for name, base, base_bins in [
        ("speed_kmh", v.where(v < 100) * 3.6, _bins(0, 260, 10)),
        ("engine_rpm", ch("PT.Engine.rotv") * (60 / (2 * math.pi)), _bins(0, 8000, 250)),
    ]:
        page.add_aggregation(
            HistogramCustomWeights(
                name=f"{name}_by_hv_power",
                base_expr=base,
                weights_expr=hv_power_kw,
                bins=base_bins,
                event=trip,
                channel_name=name,
                weights_channel_name="hv_power_kw",
            )
        )
    report.add_page(page)

    # A chain of calculated channels, each on the previous one, and their statistics.
    page = Page(page_number=4)
    chain_inputs = pick(chain_length + 1, offset=1)
    expr = ch(chain_inputs[0]["name"]) - ch(chain_inputs[1]["name"])
    chain = []
    for i in range(chain_length):
        if i:
            expr = expr * 0.5 + ch(chain_inputs[i + 1]["name"])
        name = f"chain_{i:02d}"
        report.add_calculated_channel(
            CalculatedChannel(name=name, expr=expr, identity={"channel_name": name})
        )
        chain.append((name, expr))
    page.add_aggregation(
        StatsAggregator(
            name="chain_stats",
            input_expressions=[e for _, e in chain],
            channel_names=[n for n, _ in chain],
            statistics=["min", "max", "mean"],
            event=trip,
        )
    )
    report.add_page(page)
    return report


def build_udf_report(report):
    """Adds the report of Python extensions (`benchmark_udfs`) to `report`."""
    query = report.get_db().query
    ch = lambda name: query.channel(channel_name=name)
    udf = TimeSeriesExpression.udf

    v = ch("Vhcl.v")
    speed_kmh = v.where(v < 100) * 3.6
    brake = ch("VC.Brake")
    current, voltage = ch("PT.BattHV.Current"), ch("PT.PwrSupply.HV1.Voltage")
    signals = {
        "accel_smooth": udf(udfs.smoothed_gradient)(v),
        "ax_lowpass": udf(udfs.lowpass)(ch("Car.ax")),
        "energy_kwh": udf(udfs.cumulative_energy_kwh)(voltage, current),
        "brake_applications": udf(udfs.rising_edge_count)(brake),
    }
    for name, expr in signals.items():
        report.add_calculated_channel(
            CalculatedChannel(name=name, expr=expr, identity={"channel_name": name})
        )

    trip = ContainerEvent(name="trip", desc="Whole trip")
    driving = BasicEvent(
        name="driving_cycle",
        expr=(speed_kmh > 1).merge_intervals(30 * US).filter(60 * US),
        desc="Moving > 1 km/h; stops < 30 s merged, cycles >= 60 s",
    )
    braking = BasicEvent(
        name="braking",
        expr=udf(udfs.hysteresis)(brake),
        desc="Brake pedal above 0.2 until below 0.05 (UDF detector)",
    )
    for event in [trip, driving, braking]:
        report.add_event(event)

    page = Page(page_number=1)
    page.add_aggregation(
        StatsAggregator(
            name="udf_channel_stats",
            input_expressions=list(signals.values()),
            channel_names=list(signals),
            statistics=["min", "max", "mean", "end"],
            event=driving,
        )
    )
    custom_signals = {
        "speed_kmh": speed_kmh,
        "accel_smooth": signals["accel_smooth"],
        "ax_lowpass": signals["ax_lowpass"],
        **{
            name: ch(name)
            for name in ["Car.ay", "VC.Gas", "VC.Brake", "Vhcl.Steer.Ang", "PT.Engine.rotv"]
        },
    }
    page.add_aggregation(
        StatsAggregator(
            name="driving_percentiles",
            input_expressions=list(custom_signals.values()),
            channel_names=list(custom_signals),
            statistics=["mean"],
            event=driving,
            per_channel_custom_statistics=[
                PerChannelStatistic(
                    func=udfs.percentiles_and_integral,
                    aggregation_labels=["p5", "p50", "p95", "integral"],
                )
            ],
        )
    )
    page.add_aggregation(
        StatsAggregator(
            name="braking_stats",
            input_expressions=[brake, voltage, current, speed_kmh],
            channel_names=["VC.Brake", "voltage_v", "current_a", "speed_kmh"],
            statistics=["max"],
            event=braking,
            cross_channel_custom_statistics=[
                CrossChannelStatistic(
                    func=udfs.energy_kwh,
                    aggregation_labels=["energy_kwh"],
                    inputs=["voltage_v", "current_a"],
                    channel_name="hv_energy",
                ),
                CrossChannelStatistic(
                    func=udfs.time_share_above,
                    aggregation_labels=["brake_share"],
                    inputs=["VC.Brake"],
                    channel_name="brake_share",
                    params={"threshold": 0.1},
                ),
            ],
        )
    )
    page.add_aggregation(
        HistogramDuration(
            name="accel_smooth_hist",
            base_expr=signals["accel_smooth"],
            bins=_bins(-10, 10, 0.5),
            event=driving,
            channel_name="accel_smooth",
            bins_unit="m/s^2",
            values_unit="us",
        )
    )
    page.add_aggregation(
        HistogramCustomWeights(
            name="speed_by_energy",
            base_expr=speed_kmh,
            weights_expr=signals["energy_kwh"],
            bins=_bins(0, 260, 10),
            event=trip,
            channel_name="speed_kmh",
            weights_channel_name="energy_kwh",
            math_fct_for_weights="diff",
        )
    )
    report.add_page(page)
    return report
