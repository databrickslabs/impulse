"""Transform the wide ``poi`` occurrence log into the two shapes the solver needs.

POI is N-rows-per-container (one row per occurrence in time). It feeds two
features, and this transformer owns **both** shapings so the read/aggregate logic
lives in one place:

1. :meth:`to_container_granularity` — roll POI up to **one row per container**
   for the *container filter*. For each configured ``poi_type`` it emits
   ``poi_<type>_values`` (the distinct value set) and ``poi_<type>_count``
   (``COUNT(*)`` of that type's rows).

2. :meth:`to_channel_rows` — shape POI into **channel-data union rows** for the
   ``poi_channel`` *selection*: ``(container_id, channel_id, tstart, tend, value,
   selector_ids)``, one zero-duration sample (``tstart == tend == ts``) per
   occurrence, under a synthetic negative ``channel_id`` (disjoint from real
   channel ids). These are unioned into the solve frame so a POI channel resolves
   through the same per-container range index as a normal channel.

Both methods are pure ``DataFrame -> DataFrame`` (no config-gating; the caller
decides when to run them), which keeps the transformer independently testable.
"""

from __future__ import annotations

from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from .solver_config import SolverConfig


def poi_synthetic_channel_id(selector_id: int) -> int:
    """Map a POI channel ``selector_id`` to a synthetic ``channel_id``.

    Real channel ids are non-negative; POI channels get a **negative** id from a
    disjoint namespace so they slot into the same ``(container_id, channel_id)``
    frame/index without ever colliding with a real channel. Fits a signed 32-bit
    int. Deterministic: Stage-P row shaping and the cache lookup compute the same
    id from the same ``selector_id``.
    """
    return -(int(selector_id) % 2_000_000_000) - 1


class PoiTransformer:
    """Shape the wide ``poi`` table for the container filter and the POI channel.

    Parameters
    ----------
    config : SolverConfig
        Provides the POI column names, the ``container_id`` / channel-data column
        names, the list of ``poi_types``, and the rollup output-column naming.
    """

    def __init__(self, config: SolverConfig):
        self.config = config

    def to_container_granularity(self, poi_df: DataFrame) -> DataFrame:
        """Roll ``poi_df`` up to one row per container (the container-filter shape).

        For each ``poi_type`` in ``config.poi_types`` produces a
        ``poi_<type>_values`` set column and a ``poi_<type>_count`` (``COUNT(*)``)
        column, pivoted so every configured type is a pair of columns on the
        container row. Containers present for none of the configured types still
        appear, with empty value sets and zero counts.

        Parameters
        ----------
        poi_df : pyspark.sql.DataFrame
            The POI table, already column-mapped to internal names
            (``container_id``, ``poi_type``, ``value``).

        Returns
        -------
        pyspark.sql.DataFrame
            One row per ``container_id``, with a
            ``(poi_<type>_values, poi_<type>_count)`` pair per configured type.
        """
        cid = self.config.container_id_col
        type_col = self.config.poi_type_col
        val_col = self.config.poi_value_col
        poi_types = self.config.poi_types

        # Per (container, type): the distinct value set + occurrence count.
        # The count is COUNT(*) — one per POI row — because occurrences are not
        # pre-aggregated in the source; each row is a single occurrence.
        per_type = (
            poi_df.where(F.col(type_col).isin(poi_types))
            .groupBy(cid, type_col)
            .agg(
                F.array_sort(F.collect_set(F.col(val_col))).alias("_values"),
                F.count(F.lit(1)).cast("long").alias("_count"),
            )
        )

        # Pivot each configured type into its own values/count column pair.
        # MAX over the single matching row per (container, type) lifts the value
        # out of the CASE; missing types collapse to null and are coalesced below.
        agg_exprs = []
        for t in poi_types:
            values_col = self.config.poi_values_col(t)
            count_col = self.config.poi_count_col(t)
            agg_exprs.append(
                F.max(F.when(F.col(type_col) == t, F.col("_values"))).alias(values_col)
            )
            agg_exprs.append(
                F.max(F.when(F.col(type_col) == t, F.col("_count"))).alias(count_col)
            )

        rolled = per_type.groupBy(cid).agg(*agg_exprs)

        # Normalize absent types: empty value set (not null) + count 0, so
        # downstream predicates (.contains / >= N) are well-defined.
        for t in poi_types:
            values_col = self.config.poi_values_col(t)
            count_col = self.config.poi_count_col(t)
            rolled = rolled.withColumn(
                values_col,
                F.when(F.col(values_col).isNull(), F.array().cast("array<string>")).otherwise(
                    F.col(values_col)
                ),
            ).withColumn(count_col, F.coalesce(F.col(count_col), F.lit(0).cast("long")))

        return rolled

    def to_channel_rows(self, poi_df: DataFrame, poi_selectors, containers_df) -> DataFrame:
        """Shape ``poi_df`` into channel-data union rows for POI channel selections.

        Restricts to the surviving containers via a semi-join, then shapes **each
        selector independently**: filters to that selector's ``poi_type`` and its
        ``row_filters`` (the column-equality refinements from
        ``q.poi_channel(...)``'s extra kwargs), and emits rows in the ``channels``
        shape — ``(container_id, channel_id, tstart, tend, value, selector_ids)`` —
        with each occurrence a zero-duration sample under the selector's own
        synthetic negative ``channel_id`` (see :func:`poi_synthetic_channel_id`).

        Per-selector (not per-``poi_type``) shaping is what makes row refinements
        work: two selectors sharing a ``poi_type`` but differing by their
        ``row_filters`` have distinct ``selector_id``s / synthetic ``channel_id``s
        and must carry distinct row subsets. ``load_poi_blob`` for a given channel
        then reads exactly that selector's filtered rows.

        Parameters
        ----------
        poi_df : pyspark.sql.DataFrame
            The POI table, already column-mapped to internal names.
        poi_selectors : list[PoiChannelSelector]
            POI channel selectors to shape; each supplies its ``poi_type``,
            ``row_filters``, synthetic ``channel_id`` and ``selector_id``.
        containers_df : pyspark.sql.DataFrame
            Containers to scope the POI read to (semi-join key ``container_id``).

        Returns
        -------
        pyspark.sql.DataFrame
            Channel-data-shaped union rows for the requested POI channels. Assumes
            *poi_selectors* is non-empty (the caller gates on that).
        """
        container_id_col = self.config.container_id_col
        channel_id_col = self.config.channel_id_col
        tstart_col = self.config.tstart_col
        tend_col = self.config.tend_col
        value_col = self.config.value_col
        poi_value_str_col = self.config.poi_value_str_col
        type_col = self.config.poi_type_col
        ts_col = self.config.poi_ts_col
        val_col = self.config.poi_value_col

        # Prune to surviving containers once (semi-join; keeps POI grain, no
        # fan-out). Done before the per-selector loop so the scoping scan is
        # shared across selectors.
        scoped = poi_df.join(
            F.broadcast(containers_df.select(container_id_col).distinct()),
            on=container_id_col,
            how="left_semi",
        )

        # ts as double epoch-seconds so it shares the channel time axis.
        ts_double = F.col(ts_col).cast("timestamp").cast("double")



        # Shape each selector INDEPENDENTLY, not per poi_type. Two selectors can
        # share a poi_type but differ by their row_filters (and therefore have
        # different selector_ids); keying by poi_type would collapse them. Per
        # selector we filter to its poi_type AND its row_filters, then emit its
        # rows under its own synthetic channel_id — so ``load_poi_blob`` for that
        # channel reads exactly this selector's subset.
        frames = []
        for s in poi_selectors:         # todo this should be also part of the stellantis solver
            rows = scoped.where(F.col(type_col) == s.poi_type)
            # row_filters: equality on POI columns (e.g. network="FD3"), from the
            # extra kwargs to q.poi_channel(...). Applied at read time so the
            # resulting series contains only the matching occurrences.
            for col_name, expected in s.row_filters.items():
                rows = rows.where(F.col(col_name) == expected)
            chid = poi_synthetic_channel_id(s.selector_id)
            # The source POI ``value`` is a single string column. Carry the raw
            # string in ``poi_value_str`` and leave the numeric ``value`` null;
            # ``load_poi_blob`` interprets per the selector's dtype (parse to
            # double, or keep categorical). This keeps one POI value column and
            # defers numeric parsing to load time.
            frames.append(
                rows.select(
                    F.col(container_id_col).alias(container_id_col),
                    F.lit(chid).alias(channel_id_col),
                    ts_double.alias(tstart_col),
                    ts_double.alias(tend_col),  # zero-duration: tend == tstart
                    F.lit(None).cast("double").alias(value_col),
                    F.col(val_col).cast("string").alias(poi_value_str_col),
                    F.array(F.lit(s.selector_id)).alias("selector_ids"),
                )
            )

        return reduce(DataFrame.unionByName, frames)

