"""Descriptors for custom statistics (per-channel and cross-channel)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PerChannelStatistic:
    """
    Descriptor for a single per-channel custom statistic.

    Parameters
    ----------
    func : Callable
        Function with signature ``func(series: SampleSeries, t_start: float,
        t_end: float, **params) -> float``. Called once per input channel and
        event interval with the channel's series clipped to the interval. The
        series may be empty; return ``float("nan")`` for an undefined result.
        The function is cloudpickled to Spark executors, so a module-level
        importable function is recommended; never capture Spark objects.
    params : dict, optional
        Keyword arguments passed to ``func`` on every invocation
        (``func(series, t_start, t_end, **params)``). Keys must be valid Python
        identifiers matching parameter names of ``func``. Changing params
        changes the aggregation's definition hash.
    """

    func: Callable
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class CrossChannelStatistic:
    """
    Descriptor for a single cross-channel custom statistic.

    Parameters
    ----------
    func : Callable
        Function with signature ``func(series: list[SampleSeries], t_start: float,
        t_end: float, **params) -> float``. Called once per event interval with the
        series listed in ``inputs`` (clipped to the interval, in declared order).
        Any series may be empty; return ``float("nan")`` for an undefined result.
        The function is cloudpickled to Spark executors, so a module-level
        importable function is recommended; never capture Spark objects.
    inputs : list of str, optional
        Names of the input channels the function requires, resolved against the
        aggregator's ``input_names``. ``None`` (default) passes all input
        channels in input order.
    channel_name : str, optional
        The ``channel_name`` under which the statistic's values appear in the
        gold-layer fact table. Consumed by the reporting layer only; ignored by
        the query engine. ``None`` (default) uses the statistic's name.
    params : dict, optional
        Keyword arguments passed to ``func`` on every invocation
        (``func(series, t_start, t_end, **params)``). Keys must be valid Python
        identifiers matching parameter names of ``func``. Changing params
        changes the aggregation's definition hash.
    """

    func: Callable
    inputs: list[str] | None = None
    channel_name: str | None = None
    params: dict[str, Any] = field(default_factory=dict)


def _validate_params(param_name: str, statistic_name: str, params: dict[str, Any]) -> None:
    """Validate a statistic's params mapping (dict with identifier keys; None allowed)."""
    if params is None:
        return
    if not isinstance(params, dict):
        raise TypeError(
            f"{param_name}['{statistic_name}'].params must be a dict, "
            f"got {type(params).__name__}"
        )
    for key in params:
        if not isinstance(key, str) or not key.isidentifier():
            raise TypeError(
                f"{param_name}['{statistic_name}'].params keys must be valid Python "
                f"identifiers (they are passed as keyword arguments), got {key!r}"
            )


def _validate_statistic_name(param_name: str, name: Any) -> None:
    """Validate a statistic's name (non-empty string)."""
    if not isinstance(name, str):
        raise TypeError(f"{param_name} keys must be strings, got {name!r}")
    if not name:
        raise ValueError(f"{param_name} names must be non-empty strings")


def normalize_per_channel_statistics(
    per_channel_custom_statistics: dict[str, Callable | PerChannelStatistic] | None,
) -> dict[str, PerChannelStatistic]:
    """
    Validate and normalize a per-channel statistics mapping.

    Plain callables are wrapped into ``PerChannelStatistic`` descriptors with
    empty ``params``.

    Parameters
    ----------
    per_channel_custom_statistics : dict or None
        Mapping of statistic name to callable or ``PerChannelStatistic``.

    Returns
    -------
    dict of str to PerChannelStatistic
        Normalized mapping; empty when the input is ``None``.

    Raises
    ------
    TypeError
        If the mapping is not a dict, a key is not a string, a value is neither
        callable nor a ``PerChannelStatistic`` with a callable func, or params
        are invalid.
    ValueError
        If a statistic name is empty.
    """
    param_name = "per_channel_custom_statistics"
    if per_channel_custom_statistics is None:
        return {}
    if not isinstance(per_channel_custom_statistics, dict):
        raise TypeError(
            f"{param_name} must be a dict mapping statistic names to callables or "
            f"PerChannelStatistic descriptors, got "
            f"{type(per_channel_custom_statistics).__name__}"
        )
    normalized: dict[str, PerChannelStatistic] = {}
    for name, value in per_channel_custom_statistics.items():
        _validate_statistic_name(param_name, name)
        if isinstance(value, PerChannelStatistic):
            statistic = value
        elif callable(value):
            statistic = PerChannelStatistic(func=value)
        else:
            raise TypeError(
                f"{param_name}['{name}'] must be a callable or a PerChannelStatistic, "
                f"got {type(value).__name__}"
            )
        if not callable(statistic.func):
            raise TypeError(
                f"{param_name}['{name}'].func must be callable, got "
                f"{type(statistic.func).__name__}"
            )
        _validate_params(param_name, name, statistic.params)
        normalized[name] = statistic
    return normalized


def normalize_cross_channel_statistics(
    cross_channel_custom_statistics: dict[str, Callable | CrossChannelStatistic] | None,
) -> dict[str, CrossChannelStatistic]:
    """
    Validate and normalize a cross-channel statistics mapping.

    Plain callables are wrapped into ``CrossChannelStatistic`` descriptors with
    default ``inputs`` (all channels), ``channel_name`` (the statistic name),
    and empty ``params``.

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
        If the mapping is not a dict, a key is not a string, a value is neither
        callable nor a ``CrossChannelStatistic`` with a callable func, or params
        are invalid.
    ValueError
        If a statistic name is empty.
    """
    param_name = "cross_channel_custom_statistics"
    if cross_channel_custom_statistics is None:
        return {}
    if not isinstance(cross_channel_custom_statistics, dict):
        raise TypeError(
            f"{param_name} must be a dict mapping statistic names to callables or "
            f"CrossChannelStatistic descriptors, got "
            f"{type(cross_channel_custom_statistics).__name__}"
        )
    normalized: dict[str, CrossChannelStatistic] = {}
    for name, value in cross_channel_custom_statistics.items():
        _validate_statistic_name(param_name, name)
        if isinstance(value, CrossChannelStatistic):
            statistic = value
        elif callable(value):
            statistic = CrossChannelStatistic(func=value)
        else:
            raise TypeError(
                f"{param_name}['{name}'] must be a callable or a CrossChannelStatistic, "
                f"got {type(value).__name__}"
            )
        if not callable(statistic.func):
            raise TypeError(
                f"{param_name}['{name}'].func must be callable, got "
                f"{type(statistic.func).__name__}"
            )
        _validate_params(param_name, name, statistic.params)
        normalized[name] = statistic
    return normalized
