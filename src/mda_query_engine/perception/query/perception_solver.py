"""PerceptionSolver — cogroups ``channels`` and ``object_tracks`` per
container so the per-container UDF can build a ``PerceptionCache`` with
both surfaces and run ``s.build(cache)`` unchanged.

Inherits from ``KeyValueStoreSolver`` for the channel-side filter
pipeline (stages 1–5).  Stage 6 (``solve``) is replaced.

If a query has no perception leaves the solver delegates straight to
the parent ``KeyValueStoreSolver.solve`` so non-perception
``BasicEvent`` paths see no behaviour change.

When a selection's expression has any ``PerceptionSelector`` with
``track_scope=True`` the UDF runs a per-object inner loop: for each
``object_id`` present in the container's ``object_tracks_pdf`` it
re-builds a single-object cache, evaluates the expression, and serializes
the resulting intervals as ``[start, end, object_id]`` triples in the same
output column. Non-track-scoped selections continue to emit the standard
``[start, end]`` pairs.
"""

from __future__ import annotations

from collections.abc import Iterable
from functools import partial

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from mda_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
)

from mda_query_engine.perception.tsal.perception_selector import (
    PerceptionCache,
    PerceptionSelector,
    is_track_scoped,
)


class PerceptionSolver(KeyValueStoreSolver):
    """KeyValueStoreSolver extension that also delivers ``object_tracks`` to
    the per-container UDF.
    """

    @staticmethod
    def _container_bounds(
        channels_pdf: pd.DataFrame,
        object_tracks_pdf: pd.DataFrame,
        ts_col: str,
        te_col: str,
    ) -> tuple[float, float]:
        candidates: list[float] = []
        if len(channels_pdf) > 0:
            candidates.append(float(channels_pdf[ts_col].min()))
            candidates.append(float(channels_pdf[te_col].max()))
        if len(object_tracks_pdf) > 0:
            candidates.append(float(object_tracks_pdf["frame_ts"].min()))
            candidates.append(float(object_tracks_pdf["frame_ts"].max()))
        if not candidates:
            return (0.0, 0.0)
        return (min(candidates), max(candidates))

    @staticmethod
    def _serialize_intervals(res) -> list:
        if hasattr(res, "serialize") and callable(res.serialize):
            return res.serialize()
        if hasattr(res, "get_data") and callable(res.get_data):
            return res.get_data()
        return res

    @staticmethod
    def _solve_perception_udf(
        channels_pdf: pd.DataFrame,
        object_tracks_pdf: pd.DataFrame,
        selections: Iterable,
        col_map: dict[str, str],
    ) -> pd.DataFrame:
        cid_col = col_map["cid"]
        ts_col = col_map["ts"]
        te_col = col_map["te"]
        if len(channels_pdf) > 0:
            container_id = channels_pdf[cid_col].iloc[0]
        elif len(object_tracks_pdf) > 0:
            container_id = object_tracks_pdf["container_id"].iloc[0]
        else:
            return pd.DataFrame()

        bounds = PerceptionSolver._container_bounds(
            channels_pdf, object_tracks_pdf, ts_col, te_col
        )
        # ``KVSTimeSeriesCache`` requires non-empty channels_pdf to drop value/ts/te
        # columns. When channels_pdf is empty (perception-only run), synthesize a
        # zero-row pdf with the right columns so the cache builds without error.
        if len(channels_pdf) == 0:
            channels_pdf = pd.DataFrame(
                {
                    cid_col: pd.Series(dtype=np.int64),
                    col_map["ch"]: pd.Series(dtype=np.int64),
                    ts_col: pd.Series(dtype=np.float64),
                    te_col: pd.Series(dtype=np.float64),
                    col_map["val"]: pd.Series(dtype=np.float64),
                }
            )
        # Cache for non-track-scoped selections — sees every object's frames.
        full_cache = PerceptionCache(
            channels_pdf=channels_pdf,
            col_map=col_map,
            object_tracks_pdf=object_tracks_pdf,
            container_bounds=bounds,
        )

        # Build the per-object cache list up-front. Only used by track-scoped
        # selections, but cheap to skip if none are present.
        per_object_caches: list[tuple[int, PerceptionCache]] = []
        if len(object_tracks_pdf) > 0:
            for object_id, group in object_tracks_pdf.groupby("object_id"):
                per_object_caches.append(
                    (
                        int(object_id),
                        PerceptionCache(
                            channels_pdf=channels_pdf,
                            col_map=col_map,
                            object_tracks_pdf=group.reset_index(drop=True),
                            container_bounds=bounds,
                        ),
                    )
                )

        result: dict[str, list] = {cid_col: [container_id]}
        for s in selections:
            if is_track_scoped(s):
                triples: list[list[float]] = []
                for object_id, cache_obj in per_object_caches:
                    res = PerceptionSolver._serialize_intervals(s.build(cache_obj))
                    for pair in res:
                        # pair is [start, end]; widen to [start, end, object_id].
                        triples.append([float(pair[0]), float(pair[1]), float(object_id)])
                result[s._alias] = [triples]
            else:
                result[s._alias] = [PerceptionSolver._serialize_intervals(s.build(full_cache))]
        return pd.DataFrame(result)

    def solve(self, query, channels_df, selections, dtypes) -> DataFrame:
        if not query.has_perception():
            return super().solve(query, channels_df, selections, dtypes)

        col_map = self.config.col_map
        cid_col = self.config.container_id_col
        ch_col = self.config.channel_id_col

        q = query.db.channels(self.spark)
        q = self._apply_column_mapping(q, self.config.channels.column_name_mapping)
        if self.is_raw_data:
            q = self.interval_encoder.prepare_channels_df(q)

        channels_for_udf = q.join(
            F.broadcast(channels_df), on=[cid_col, ch_col]
        )

        object_tracks_df = query.db.object_tracks(self.spark)

        schema_entries = [T.StructField(cid_col, T.LongType())]
        for s, dtype in zip(selections, dtypes, strict=False):
            schema_entries.append(T.StructField(s._alias, dtype))
        schema = T.StructType(schema_entries)

        udf = partial(
            PerceptionSolver._solve_perception_udf,
            selections=selections,
            col_map=col_map,
        )

        # Container set is the union of containers from both surfaces; if a
        # perception-only container has no matching channels row it still
        # participates so a pure-perception selection can return its windows.
        return (
            channels_for_udf.groupBy(cid_col)
            .cogroup(object_tracks_df.groupBy(cid_col))
            .applyInPandas(udf, schema=schema)
        )
