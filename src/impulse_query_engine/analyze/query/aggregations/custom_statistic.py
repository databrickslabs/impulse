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
        t_end: float, **params) -> Sequence[float]``. Called once per input
        channel and event interval with the channel's series clipped to the
        interval. It must return a sequence of scalars whose length equals
        ``aggregation_labels`` (a single label still requires a one-element
        sequence, e.g. ``[value]``). The series may be empty; return
        ``float("nan")`` entries for undefined results. The function is
        cloudpickled to Spark executors, so a module-level importable function is
        recommended; never capture Spark objects.
    aggregation_labels : list of str
        Output labels this statistic produces. The values returned by ``func``
        are mapped positionally to these labels, which become the keys of the
        statistic's result maps. Labels must be non-empty, unique strings;
        changing them changes the aggregation's definition hash.
    params : dict, optional
        Keyword arguments passed to ``func`` on every invocation
        (``func(series, t_start, t_end, **params)``). Keys must be valid Python
        identifiers matching parameter names of ``func``. Changing params
        changes the aggregation's definition hash.
    """

    func: Callable
    aggregation_labels: list[str]
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class CrossChannelStatistic:
    """
    Descriptor for a single cross-channel custom statistic.

    Parameters
    ----------
    func : Callable
        Function with signature ``func(series: list[SampleSeries], t_start: float,
        t_end: float, **params) -> Sequence[float]``. Called once per event
        interval with the series listed in ``inputs`` (clipped to the interval,
        in declared order). It must return a sequence of scalars whose length
        equals ``aggregation_labels`` (a single label still requires a
        one-element sequence, e.g. ``[value]``). Any series may be empty; return
        ``float("nan")`` entries for undefined results. The function is
        cloudpickled to Spark executors, so a module-level importable function is
        recommended; never capture Spark objects.
    aggregation_labels : list of str
        Output labels this statistic produces. The values returned by ``func``
        are mapped positionally to these labels, which become the keys of the
        statistic's result maps. Labels must be non-empty, unique strings;
        changing them changes the aggregation's definition hash.
    inputs : list of str, optional
        Names of the input channels the function requires, resolved against the
        aggregator's ``input_names``. ``None`` (default) passes all input
        channels in input order.
    channel_name : str, optional
        A channel name applied to all of the statistic's output rows. Consumed by
        downstream consumers (e.g. the reporting layer) only; ignored by the
        query engine. ``None`` (default) leaves it to the consumer, which
        typically falls back to each output's ``aggregation_label``.
    params : dict, optional
        Keyword arguments passed to ``func`` on every invocation
        (``func(series, t_start, t_end, **params)``). Keys must be valid Python
        identifiers matching parameter names of ``func``. Changing params
        changes the aggregation's definition hash.
    """

    func: Callable
    aggregation_labels: list[str]
    inputs: list[str] | None = None
    channel_name: str | None = None
    params: dict[str, Any] = field(default_factory=dict)


def _validate_params(param_name: str, labels: list[str], params: dict[str, Any]) -> None:
    """Validate a statistic's params mapping (dict with identifier keys; None allowed)."""
    if params is None:
        return
    if not isinstance(params, dict):
        raise TypeError(
            f"{param_name} statistic {labels!r}: params must be a dict, "
            f"got {type(params).__name__}"
        )
    for key in params:
        if not isinstance(key, str) or not key.isidentifier():
            raise TypeError(
                f"{param_name} statistic {labels!r}: params keys must be valid Python "
                f"identifiers (they are passed as keyword arguments), got {key!r}"
            )


def _validate_aggregation_labels(param_name: str, labels: Any) -> None:
    """Validate a statistic's aggregation_labels (non-empty list of unique, non-empty strings)."""
    if not isinstance(labels, list) or not labels:
        raise TypeError(
            f"{param_name}: aggregation_labels is required and must be a non-empty "
            f"list of strings, got {labels!r}"
        )
    for label in labels:
        if not isinstance(label, str) or not label:
            raise TypeError(
                f"{param_name}: aggregation_labels must contain non-empty strings, "
                f"got {label!r}"
            )
    if len(set(labels)) != len(labels):
        raise ValueError(f"{param_name}: aggregation_labels must be unique, got {labels!r}")


def normalize_per_channel_statistics(
    per_channel_custom_statistics: list[PerChannelStatistic] | None,
) -> list[PerChannelStatistic]:
    """
    Validate a per-channel statistics list.

    Parameters
    ----------
    per_channel_custom_statistics : list or None
        List of ``PerChannelStatistic`` descriptors.

    Returns
    -------
    list of PerChannelStatistic
        The validated list; empty when the input is ``None``.

    Raises
    ------
    TypeError
        If the value is not a list, an item is not a ``PerChannelStatistic``
        with a callable ``func``, or params/labels are invalid.
    ValueError
        If a descriptor's ``aggregation_labels`` are not unique.
    """
    return _normalize_statistics(
        "per_channel_custom_statistics", per_channel_custom_statistics, PerChannelStatistic
    )


def normalize_cross_channel_statistics(
    cross_channel_custom_statistics: list[CrossChannelStatistic] | None,
) -> list[CrossChannelStatistic]:
    """
    Validate a cross-channel statistics list.

    Parameters
    ----------
    cross_channel_custom_statistics : list or None
        List of ``CrossChannelStatistic`` descriptors.

    Returns
    -------
    list of CrossChannelStatistic
        The validated list; empty when the input is ``None``.

    Raises
    ------
    TypeError
        If the value is not a list, an item is not a ``CrossChannelStatistic``
        with a callable ``func``, or params/labels are invalid.
    ValueError
        If a descriptor's ``aggregation_labels`` are not unique.
    """
    return _normalize_statistics(
        "cross_channel_custom_statistics", cross_channel_custom_statistics, CrossChannelStatistic
    )


def _normalize_statistics(param_name: str, statistics: list | None, descriptor_type: type) -> list:
    """Validate a list of custom-statistic descriptors of a single kind."""
    if statistics is None:
        return []
    if not isinstance(statistics, list):
        raise TypeError(
            f"{param_name} must be a list of {descriptor_type.__name__} descriptors, "
            f"got {type(statistics).__name__}"
        )
    for statistic in statistics:
        if not isinstance(statistic, descriptor_type):
            raise TypeError(
                f"{param_name} items must be {descriptor_type.__name__} descriptors, "
                f"got {type(statistic).__name__}"
            )
        _validate_aggregation_labels(param_name, statistic.aggregation_labels)
        if not callable(statistic.func):
            raise TypeError(
                f"{param_name} statistic {statistic.aggregation_labels!r}: func must be "
                f"callable, got {type(statistic.func).__name__}"
            )
        _validate_params(param_name, statistic.aggregation_labels, statistic.params)
    return statistics
