"""Python extensions of the `python_udfs` scenario.

Impulse's extension points: UDF calculated channels (smoothing, filtering, integrals, edge
counters), a UDF event detector that returns `Intervals`, and custom statistics. They are
vectorised numpy, so the scenario measures Impulse's UDF overhead rather than Python loops.

Impulse cloudpickles the functions to the executors, which can't import this workspace file:
`run_benchmark` registers the module to be pickled by value.
"""

import numpy as np
from scipy.signal import butter, filtfilt

from impulse_query_engine.model.series.intervals import Intervals
from impulse_query_engine.model.series.sample_series import SampleSeries

US = 1e6  # Timestamps are epoch microseconds.
# kWh per W·µs.
KWH_PER_W_US = 1 / (3.6e6 * US)


def _uniform(series, rate_hz):
    """The series resampled to `rate_hz`, so that it is evenly spaced."""
    return series.resample(US / rate_hz)


def smoothed_gradient(series, rate_hz=10.0, window_s=3.0):
    """Gradient per second of the moving average over `window_s` (e.g. acceleration from speed)."""
    if len(series) < 2:
        return series
    s = _uniform(series, rate_hz)
    n = max(1, round(window_s * rate_hz))
    # Padded with the edge values, so the average doesn't drop towards zero at the ends.
    padded = np.pad(s.values, (n // 2, n - 1 - n // 2), mode="edge")
    smooth = np.convolve(padded, np.ones(n) / n, mode="valid")
    gradient = np.gradient(smooth, s.tstarts / US) if len(s) > 1 else np.zeros(len(s))
    return SampleSeries(s.tstarts, s.tends, gradient)


def lowpass(series, cutoff_hz=1.0, rate_hz=20.0, order=2):
    """Zero-phase Butterworth low-pass of the series resampled to `rate_hz`."""
    if len(series) < 2:
        return series
    s = _uniform(series, rate_hz)
    b, a = butter(order, cutoff_hz / (rate_hz / 2))
    # filtfilt needs more samples than its padding.
    if len(s) <= 3 * max(len(a), len(b)):
        return s
    return SampleSeries(s.tstarts, s.tends, filtfilt(b, a, s.values))


def cumulative_energy_kwh(voltage, current):
    """Cumulative HV energy in kWh, the trapezoidal integral of voltage times current."""
    power = voltage * current
    if len(power) < 2:
        return power
    return power.cumtrapz() * KWH_PER_W_US


def rising_edge_count(series, threshold=0.1):
    """Number of rising edges through `threshold` up to each sample (e.g. brake applications)."""
    if len(series) == 0:
        return series
    above = series.values > threshold
    edges = np.concatenate([[False], above[1:] & ~above[:-1]])
    return SampleSeries(series.tstarts, series.tends, np.cumsum(edges).astype(np.float64))


def hysteresis(series, on=0.2, off=0.05):
    """Intervals that start where the series rises above `on` and end where it falls below
    `off`."""
    if len(series) == 0:
        return Intervals(np.array([]), np.array([]))
    v = series.values
    # 1 above `on`, 0 below `off`, otherwise (-1) the state of the last sample that set one.
    marks = np.where(v > on, 1, np.where(v < off, 0, -1))
    last_mark = np.maximum.accumulate(np.where(marks >= 0, np.arange(len(v)), 0))
    state = (marks[last_mark] == 1).astype(int)
    change = np.diff(np.concatenate([[0], state, [0]]))
    starts, stops = np.flatnonzero(change == 1), np.flatnonzero(change == -1) - 1
    return Intervals(series.tstarts[starts], series.tends[stops])


def percentiles_and_integral(series, t_start, t_end):
    """p5, p50, p95 of the values and their time integral (per second)."""
    if len(series) == 0:
        return [float("nan")] * 4
    p5, p50, p95 = np.percentile(series.values, [5, 50, 95])
    return [float(p5), float(p50), float(p95), float(series.trapz() / US)]


def energy_kwh(series, t_start, t_end):
    """HV energy in kWh of the event: voltage times current, integrated."""
    voltage, current = series
    power = voltage * current
    if len(power) < 2:
        return [float("nan")]
    return [float(power.trapz() * KWH_PER_W_US)]


def time_share_above(series, t_start, t_end, threshold=0.1):
    """Share of the event's duration the first series is above `threshold`."""
    s = series[0]
    if len(s) == 0 or t_end <= t_start:
        return [float("nan")]
    above = s.values > threshold
    return [float(np.sum((s.tends - s.tstarts)[above]) / (t_end - t_start))]
