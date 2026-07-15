"""CrossChannelStatistic descriptor for cross-channel custom statistics."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class CrossChannelStatistic:
    """
    Descriptor for a single cross-channel custom statistic.

    Parameters
    ----------
    func : Callable
        Function with signature ``func(series: list[SampleSeries], t_start: float,
        t_end: float) -> float``. Called once per event interval with the series
        listed in ``inputs`` (clipped to the interval, in declared order). Any
        series may be empty; return ``float("nan")`` for an undefined result.
        The function is cloudpickled to Spark executors, so a module-level
        importable function is recommended (bind parameters via
        ``functools.partial``); never capture Spark objects.
    inputs : list of str, optional
        Names of the input channels the function requires, resolved against the
        aggregator's ``input_names``. ``None`` (default) passes all input
        channels in input order.
    channel_name : str, optional
        The ``channel_name`` under which the statistic's values appear in the
        gold-layer fact table. Consumed by the reporting layer only; ignored by
        the query engine. ``None`` (default) uses the statistic's name.
    """

    func: Callable
    inputs: list[str] | None = None
    channel_name: str | None = None


def normalize_cross_channel_statistics(
    cross_channel_custom_statistics: dict[str, Callable | CrossChannelStatistic] | None,
) -> dict[str, CrossChannelStatistic]:
    """
    Validate and normalize a cross-channel statistics mapping.

    Plain callables are wrapped into ``CrossChannelStatistic`` descriptors with
    default ``inputs`` (all channels) and ``channel_name`` (the statistic name).

    Parameters
    ----------
    cross_channel_custom_statistics : dict or None
        Mapping of statistic name to callable or ``CrossChannelStatistic``.

    Returns
    -------
    dict of str to CrossChannelStatistic
        Normalized mapping; empty when the input is ``None``.

    Raises
    ------
    TypeError
        If the mapping is not a dict, a key is not a string, or a value is
        neither callable nor a ``CrossChannelStatistic`` with a callable func.
    ValueError
        If a statistic name is empty.
    """
    if cross_channel_custom_statistics is None:
        return {}
    if not isinstance(cross_channel_custom_statistics, dict):
        raise TypeError(
            "cross_channel_custom_statistics must be a dict mapping statistic names to "
            "callables or CrossChannelStatistic descriptors, got "
            f"{type(cross_channel_custom_statistics).__name__}"
        )
    normalized: dict[str, CrossChannelStatistic] = {}
    for name, value in cross_channel_custom_statistics.items():
        if not isinstance(name, str):
            raise TypeError(f"cross_channel_custom_statistics keys must be strings, got {name!r}")
        if not name:
            raise ValueError("cross_channel_custom_statistics names must be non-empty strings")
        if isinstance(value, CrossChannelStatistic):
            statistic = value
        elif callable(value):
            statistic = CrossChannelStatistic(func=value)
        else:
            raise TypeError(
                f"cross_channel_custom_statistics['{name}'] must be a callable or a "
                f"CrossChannelStatistic, got {type(value).__name__}"
            )
        if not callable(statistic.func):
            raise TypeError(
                f"cross_channel_custom_statistics['{name}'].func must be callable, got "
                f"{type(statistic.func).__name__}"
            )
        normalized[name] = statistic
    return normalized
