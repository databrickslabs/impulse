"""Registry of calculated-channel metric KPIs.

Each KPI is a named builder that returns a Spark aggregation :class:`Column`
(already ``.alias(name)``-ed) computed over the narrow calculated-channel fact
rows grouped by ``(container_id, channel_id)``.  The registry is the single
extension point for the ``calculated_channel_metrics`` table: **adding a KPI means
adding one entry to** :data:`KPI_BUILDERS` — it then becomes selectable via
``CalculatedChannels.kpis`` config with no other changes.

Semantics match :class:`SampleSeries` (duration-weighted where relevant), with
``dur = tend - tstart``.  NaN values are excluded from ``min``/``max``/``mean``
(matching ``np.nanmin`` / ``nanmax`` / ``nansum``); the ``mean`` denominator keeps
every interval's duration and uses ``try_divide`` so a zero total duration (a group
of only zero-duration point-in-time samples) yields ``null`` instead of failing the
run under ANSI mode (Spark 4.0 default).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import Column


@dataclass(frozen=True)
class KpiColumns:
    """Reusable per-group column expressions shared by the KPI builders.

    Built once per aggregation from the fact columns so each builder does not
    recompute the duration / NaN-masking expressions.
    """

    dur: Column
    value: Column
    non_nan_value: Column
    weighted_value: Column

    @classmethod
    def from_fact(cls) -> "KpiColumns":
        """Build the shared expressions from the fixed fact column names."""
        dur = F.col("tend") - F.col("tstart")
        value = F.col("value")
        non_nan_value = F.when(~F.isnan(value), value)
        weighted_value = F.when(~F.isnan(value), value * dur).otherwise(F.lit(0.0))
        return cls(
            dur=dur, value=value, non_nan_value=non_nan_value, weighted_value=weighted_value
        )


def _duration(cols: KpiColumns) -> Column:
    return (F.max("tend") - F.min("tstart")).alias("duration")


def _min(cols: KpiColumns) -> Column:
    return F.min(cols.non_nan_value).alias("min")


def _max(cols: KpiColumns) -> Column:
    return F.max(cols.non_nan_value).alias("max")


def _mean(cols: KpiColumns) -> Column:
    # Duration-weighted; try_divide → null (not error) when total duration is zero.
    return F.try_divide(F.sum(cols.weighted_value), F.sum(cols.dur)).alias("mean")


# Name → aggregation-Column builder. Adding a KPI = adding one entry here.
KPI_BUILDERS: dict[str, Callable[[KpiColumns], Column]] = {
    "duration": _duration,
    "min": _min,
    "max": _max,
    "mean": _mean,
}

# Default KPIs computed when config does not narrow the selection.
DEFAULT_KPIS: list[str] = ["duration", "min", "max", "mean"]


def build_kpi_columns(names: list[str]) -> list[Column]:
    """Return the aggregation columns for ``names`` in the given order.

    Parameters
    ----------
    names : list of str
        KPI names to compute; each must be a key of :data:`KPI_BUILDERS`.

    Returns
    -------
    list of Column
        Aliased aggregation columns, one per name, ready to splat into ``.agg``.

    Raises
    ------
    ValueError
        If any name is not a registered KPI.
    """
    unknown = [n for n in names if n not in KPI_BUILDERS]
    if unknown:
        valid = ", ".join(sorted(KPI_BUILDERS))
        raise ValueError(f"Unknown calculated-channel KPI(s): {unknown}. Valid KPIs: {valid}.")
    cols = KpiColumns.from_fact()
    return [KPI_BUILDERS[name](cols) for name in names]
