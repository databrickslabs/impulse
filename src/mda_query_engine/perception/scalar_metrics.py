"""Derivations over the canonical `channels` Delta table.

The `channel_metrics` table is required by the TSAL solver — it does an inner
join against `(container_id, channel_id)` to scope the channel scan, so any
channel that has rows in `channels` but no matching row in `channel_metrics`
is silently dropped from event detection. Every dataset adapter writes the
same `channels` schema, so the derivation lives here rather than in each
adapter or each notebook.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def derive_channel_metrics_from_channels(spark: SparkSession, channels_table: str) -> DataFrame:
    """One row per distinct (container_id, channel_id) in `channels`, with
    summary statistics (sample count, NaN ratio, time range, percentiles).

    The TSAL solver only uses `container_id` and `channel_id` from this table
    to scope its scan; the remaining columns make the table useful for
    ad-hoc inspection without requiring a separate aggregation.
    """
    return spark.sql(f"""
SELECT
  container_id,
  channel_id,
  CAST('double' AS STRING)                                       AS value_type,
  CAST(COUNT(*) AS INT)                                          AS sample_count,
  CAST(SUM(CASE WHEN value IS NULL OR isnan(value) THEN 1 ELSE 0 END)
       / COUNT(*) AS FLOAT)                                      AS nan_ratio,
  CAST(MIN(tstart) / 1e6 AS FLOAT)                               AS begin_s,
  CAST(MAX(tend)   / 1e6 AS FLOAT)                               AS end_s,
  CAST((MAX(tend) - MIN(tstart)) / 1000 AS INT)                  AS duration_ms,
  CAST(COUNT(*) AS INT)                                          AS original_sample_count,
  CAST(NULL AS FLOAT)                                            AS original_sr,
  CAST(MIN(value)  AS FLOAT)                                     AS min,
  CAST(MAX(value)  AS FLOAT)                                     AS max,
  CAST(AVG(value)  AS FLOAT)                                     AS mean,
  CAST(STDDEV(value) AS FLOAT)                                   AS std,
  CAST(percentile_approx(value, 0.01) AS FLOAT)                  AS pz1,
  CAST(percentile_approx(value, 0.10) AS FLOAT)                  AS pz10,
  CAST(percentile_approx(value, 0.90) AS FLOAT)                  AS pz90,
  CAST(percentile_approx(value, 0.99) AS FLOAT)                  AS pz99
FROM {channels_table}
GROUP BY container_id, channel_id
""")
