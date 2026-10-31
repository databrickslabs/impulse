"""Spark execution of a pushdown plan: one shared pass for all rules.

:func:`apply_pushdowns` turns the channel data of a solve into the rows the
solver ships: raw rows where still needed, plus every rule's derived rows, each
tagged with its variant.  Rules contribute columns only (see
:class:`~.pushdown.PushdownRule`), so the number of reads of the channel data
does not grow with the number of rules:

* **Ordered rules** (:class:`SeriesPipeline`) read the channel data once in full
  and once narrowly (channel boundaries, aggregated before their shuffle).  Each
  sample is shuffled once, by channel and chunk, however many variants its
  channel ships, and processed per chunk in parallel.
* **Raw rows and per-sample rules** share one more read, only when present.

The ordered pass, per channel (its samples ordered by ``(tstart, tend, value)``):

1. Channel boundaries give each sample its chunk: equal time slices holding
   about :data:`CHUNK_ROWS` / k samples on average for a channel with k variants.
2. A window per chunk derives each sample's neighbours and run breaks.
3. One summary row per chunk, scanned in chunk order, carries what crosses chunk
   boundaries: neighbours at chunk edges, run numbers and starts, the channel's
   sample count and last sample, and block ends.
4. The samples fan out into one row per variant: a series, one channel's samples
   of one variant.  Stages 1 to 3 are the same for every variant of a channel,
   so they run once; a chunk again holds about :data:`CHUNK_ROWS` rows.
5. Rules add per-sample columns (``row_columns``) and select samples (``keep``).
6. Kept samples get their block end; rules add ``kept_columns``.
7. Rules ``emit`` the derived rows.

Windows never span more than a chunk of samples (or the chunk summaries of a
channel), so the memory per task is bounded by the chunk size.

Keeping one full read for the chunk pass and its summaries relies on both
seeing the same plan below the shuffle:

* The same projected rows: the raw sample columns and a chunk id that cannot be
  null.  Windows over the samples read raw columns only; a derived window input
  is computed before the shuffle, and each consumer would keep only the inputs
  it uses.
* The same runtime filters: the full read is joined with *variants* like the
  narrow read, so both copies are pruned alike, and the summaries are joined on
  keys the planner cannot trace to the scan, so no filter on the chunk pass is
  built from its own summaries.
* The value as the last sort key, and summaries that depend on the chunk window.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import reduce

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame, Window

from .context import (
    BLOCK,
    BLOCK_END,
    CHUNK,
    COUNT,
    LAST,
    NEXT_TE,
    NEXT_TS,
    NEXT_VALUE,
    PREV_TE,
    PREV_TS,
    PREV_VALUE,
    TE,
    TS,
    RunBreak,
    Sample,
    SeriesContext,
    block_break,
    run_col,
)
from .plan import PushdownPlan
from .pushdown import (
    RAW_VARIANT,
    VARIANT_COL,
    VARIANTS_COL,
    Pushdown,
    PushdownRule,
    check_name,
)

#: Average number of rows per chunk of a channel, counting a sample once per variant.
CHUNK_ROWS = 100_000

_ELEMENT = "_pd_element"
_KEPT = "_pd_kept"


def apply_pushdowns(
    rows: DataFrame, variants: DataFrame, plan: PushdownPlan, config, include_raw: bool = True
) -> DataFrame:
    """The rows a solve ships: raw rows (when *include_raw*) and all derived rows.

    Parameters
    ----------
    rows : pyspark.sql.DataFrame
        Channel data ``(container_id, channel_id, tstart, tend, value)``, one row
        per sample.
    variants : pyspark.sql.DataFrame
        ``(container_id, channel_id, VARIANTS_COL)``: the variant ids each matched
        channel ships, one row per channel (see :meth:`PushdownPlan.channel_variants`).
        Channels without a row are dropped.
    plan : PushdownPlan
        The solve's pushdown plan.
    config : SolverConfig
        Provides the internal column names.
    include_raw : bool
        Whether to keep the raw rows (variant 0).

    Returns
    -------
    pyspark.sql.DataFrame
        Rows with the columns and types of *rows* plus :data:`VARIANT_COL`.
    """
    groups = plan.variants_by_rule()
    ordered = [(rule, variants) for rule, variants in groups if rule.ordered]
    per_sample = [(rule, variants) for rule, variants in groups if not rule.ordered]
    key = [config.container_id_col, config.channel_id_col]
    schema = _output_schema(rows)
    parts = []
    row_variants = ([RAW_VARIANT] if include_raw else []) + _variant_ids(per_sample)
    if row_variants:
        # Only the channels shipping a row-branch variant, so the join drops the others.
        shipping = variants.select(*key, _shipped(row_variants).alias(VARIANTS_COL))
        tagged = rows.join(F.broadcast(shipping.where(F.size(VARIANTS_COL) > 0)), on=key).select(
            *rows.columns, F.explode(VARIANTS_COL).alias(VARIANT_COL)
        )
        parts.append(_row_branch(tagged, per_sample, config, schema))
    if ordered:
        parts.append(SeriesPipeline(ordered, config).run(rows, variants))
    if not parts:
        return rows.withColumn(VARIANT_COL, F.lit(None).cast(T.IntegerType())).limit(0)
    return reduce(lambda a, b: a.unionByName(b), parts)


def _variant_ids(groups: Sequence[tuple[PushdownRule, Mapping[int, Pushdown]]]) -> list[int]:
    return sorted(vid for _, variants in groups for vid in variants)


def _shipped(ids: Sequence[int]) -> Column:
    """The variant ids among *ids* that a channel ships, from its :data:`VARIANTS_COL`."""
    return F.filter(F.col(VARIANTS_COL), lambda vid: vid.isin(sorted(ids)))


def _fan_out(df: DataFrame, variants: DataFrame, key: Sequence[str], ids: Sequence[int]):
    """*df* with one row per variant among *ids* its channel ships, in :data:`VARIANT_COL`.

    Channels shipping none of *ids* are dropped.  Unlike ``explode``, ``explode_outer``
    adds no inferred filter to the broadcast side of the join, so the pipeline's joins
    with *variants* share one broadcast.
    """
    return (
        df.join(F.broadcast(variants), on=key)
        .withColumn(VARIANT_COL, F.explode_outer(_shipped(ids)))
        .where(F.col(VARIANT_COL).isNotNull())
        .drop(VARIANTS_COL)
    )


def _opaque_join(df: DataFrame, small: DataFrame, on: Sequence[str]) -> DataFrame:
    """*df* joined with the broadcast *small* on keys the planner cannot trace to a scan.

    *df*'s keys are copied behind an always-true non-deterministic guard, so no
    runtime filter built from *small* reaches *df*'s scans.  When *small* is
    computed from *df*, such a filter would make the two copies of *df*'s plan
    differ and keep them from sharing their shuffle.  The keys must not be null.
    """
    guarded = {c: f"_pd_join_{c}" for c in on}
    # spark_partition_id() is never negative; Spark folds comparisons on rand() away.
    guard = F.spark_partition_id() >= 0
    left = df.select("*", *[F.when(guard, F.col(c)).alias(g) for c, g in guarded.items()])
    right = small.select(*[F.col(c).alias(guarded.get(c, c)) for c in small.columns])
    return left.join(F.broadcast(right), on=list(guarded.values())).drop(*guarded.values())


def _output_schema(rows: DataFrame) -> T.StructType:
    """The columns and types of the shipped rows: those of *rows* plus the variant."""
    return T.StructType([*rows.schema.fields, T.StructField(VARIANT_COL, T.IntegerType())])


@dataclass(frozen=True)
class _Run:
    """A run definition of the pipeline."""

    name: str
    breaks: RunBreak


class SeriesPipeline:
    """The ordered pass shared by all rules that need their series in order.

    Chunking and everything that crosses chunk edges runs once per channel; its
    samples fan out into one series per variant only for the rules' hooks.  The
    shuffle therefore carries each sample once, while a task after it processes
    about k rows per shuffled sample of a channel with k variants (chunks are k
    times smaller to keep the rows per chunk at :data:`CHUNK_ROWS`).

    Rule runs are computed for every channel of the pass; a rule only reads its
    own runs, on its own series.

    Parameters
    ----------
    rules : Sequence of (PushdownRule, Mapping[int, Pushdown])
        Ordered rules with their variants in this solve.
    config : SolverConfig
        Provides the internal column names.
    """

    def __init__(self, rules: Sequence[tuple[PushdownRule, Mapping[int, Pushdown]]], config):
        self.rules = list(rules)
        self.config = config
        self.channel_key = [config.container_id_col, config.channel_id_col]
        self.key = [*self.channel_key, VARIANT_COL]
        self.order = [config.tstart_col, config.tend_col, config.value_col]
        self.variant_ids = _variant_ids(self.rules)
        self.runs = [_Run(BLOCK, block_break)]
        self.rule_runs: dict[str, dict[str, str]] = {}
        for rule, _ in self.rules:
            names = {}
            for name, breaks in rule.runs().items():
                names[name] = f"{rule.name}__{check_name('run', name)}"
                self.runs.append(_Run(names[name], breaks))
            self.rule_runs[rule.name] = names

    def _context(self, rule: PushdownRule, variants: Mapping[int, Pushdown]) -> SeriesContext:
        return SeriesContext(rule, variants, self.key, self.order, True, self.rule_runs[rule.name])

    def run(self, rows: DataFrame, variants: DataFrame) -> DataFrame:
        """Replace the samples of the rules' variants with their derived rows.

        *rows* holds the channel data, one row per sample; *variants* the variant
        ids of each channel (see :func:`apply_pushdowns`).  Only channels shipping a
        variant of the rules are processed.
        """
        df = self._chunked(rows, variants)
        df = self._chunk_scan(df)
        df = _opaque_join(df, self._chunk_summaries(df), [*self.channel_key, CHUNK])
        df = self._resolve_neighbours_and_runs(df)
        # The rules' columns read the variant (SeriesContext.active, .param).
        df = _fan_out(df, variants, self.channel_key, self.variant_ids)
        contexts = [(rule, self._context(rule, variants)) for rule, variants in self.rules]
        for rule, ctx in contexts:
            df = _with_rule_columns(df, ctx, rule.row_columns(ctx))
        df = _keep(df, contexts, extra=_flag(run_col(BLOCK, "ends")))
        df = self._with_block_end(df)
        kept = [(rule, ctx.kept_stage()) for rule, ctx in contexts]
        for rule, ctx in kept:
            df = _with_rule_columns(df, ctx, rule.kept_columns(ctx))
        return _emit(df, kept, _output_schema(rows), self.order, raw=None)

    # --- stages ----------------------------------------------------------------------

    def _chunked(self, rows: DataFrame, variants: DataFrame) -> DataFrame:
        """Channel boundaries and each sample's chunk, with the raw sample columns.

        A channel with k variants gets k times as many chunks: its samples fan out
        per chunk before the rules' hooks (see :meth:`run`).
        """
        ts, te, value = self.order
        ts_d, te_d = F.col(ts).cast("double"), F.col(te).cast("double")
        matched = rows.join(F.broadcast(variants), on=self.channel_key)
        # The time range spans the finite times; a channel without a finite tend is
        # still cut by its tstart range.
        series = (
            matched.groupBy(*self.channel_key).agg(
                F.min(_nan_free(ts_d)).alias("_pd_first"),
                F.greatest(F.max(_nan_free(te_d)), F.max(_nan_free(ts_d))).alias("_pd_end"),
                F.count(F.lit(1)).alias("_pd_count"),
                F.max(F.size(_shipped(self.variant_ids))).alias("_pd_k"),
            )
            # Only the channels shipping a variant of the rules; filtering the aggregate
            # keeps the broadcast of *variants* the same as for the other joins.
            .where(F.col("_pd_k") > 0)
        )
        first, end = F.col("_pd_first"), F.col("_pd_end")
        n_chunks = F.ceil(F.col("_pd_count") * F.col("_pd_k") / F.lit(CHUNK_ROWS))
        position = F.least(F.floor((ts_d - first) / (end - first) * n_chunks), n_chunks - 1)
        # Null and NaN tstarts go where the series order puts them: first and last.
        chunk = F.coalesce(
            F.when(ts_d.isNull(), F.lit(0).cast("long"))
            .when(F.isnan(ts_d), n_chunks - 1)
            .when(end > first, position),
            F.lit(0).cast("long"),
        )
        return matched.join(F.broadcast(series), on=self.channel_key).select(
            *self.channel_key, ts, te, value, chunk.alias(CHUNK)
        )

    def _chunk_scan(self, df: DataFrame) -> DataFrame:
        """Neighbours within the chunk, and each run's breaks and segments."""
        ts, te, value = self.order
        in_chunk = Window.partitionBy(*self.channel_key, CHUNK).orderBy(*self.order)
        running = in_chunk.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        # The neighbour windows read the raw columns and cast their results.
        df = (
            df.withColumn("_pd_first_in_chunk", F.row_number().over(in_chunk) == 1)
            .withColumn("_pd_last_in_chunk", F.lead(F.lit(True)).over(in_chunk).isNull())
            .withColumn("_pd_lag_ts", F.lag(ts).over(in_chunk).cast("double"))
            .withColumn("_pd_lag_te", F.lag(te).over(in_chunk).cast("double"))
            .withColumn("_pd_lag_value", F.lag(value).over(in_chunk))
            .withColumn("_pd_lead_ts", F.lead(ts).over(in_chunk).cast("double"))
            .withColumn("_pd_lead_te", F.lead(te).over(in_chunk).cast("double"))
            .withColumn("_pd_lead_value", F.lead(value).over(in_chunk))
            .withColumn(TS, F.col(ts).cast("double"))
            .withColumn(TE, F.col(te).cast("double"))
        )
        prev = Sample(
            F.col("_pd_lag_ts"), F.col("_pd_lag_te"), F.col("_pd_lag_value").cast("double")
        )
        cur = Sample(F.col(TS), F.col(TE), F.col(value).cast("double"))
        for run in self.runs:
            brk = F.col(run_col(run.name, "brk"))
            df = (
                df.withColumn(
                    run_col(run.name, "brk"),
                    F.when(F.col("_pd_first_in_chunk"), F.lit(0).cast("long")).otherwise(
                        _breaks(run, prev, cur)
                    ),
                )
                .withColumn(run_col(run.name, "seg"), F.sum(brk).over(running))
                .withColumn(
                    run_col(run.name, "seg_start"),
                    F.last(F.when(brk == 1, F.col(TS)), ignorenulls=True).over(running),
                )
                .withColumn(run_col(run.name, "lead_brk"), F.lead(brk).over(in_chunk))
            )
        return df

    def _chunk_summaries(self, df: DataFrame) -> DataFrame:
        """One row per chunk with everything that crosses chunk boundaries."""
        order = F.struct(*self.order)
        sample = F.struct(
            F.col(TS).alias("tstart"),
            F.col(TE).alias("tend"),
            F.col(self.order[2]).cast("double").alias("value"),
        )
        block_brk, block_seg = F.col(run_col(BLOCK, "brk")), F.col(run_col(BLOCK, "seg"))
        # Head and tail are taken via the chunk window's flags, so the summaries always
        # run on the chunk-partitioned samples and share their read.
        head = F.when(F.col("_pd_first_in_chunk"), F.lit(1))
        tail = F.when(F.col("_pd_last_in_chunk"), F.lit(1))
        aggregates = [
            F.max_by(sample, head).alias("_pd_head"),
            F.max_by(sample, tail).alias("_pd_tail"),
            F.max_by(block_brk, tail).alias("_pd_tail_brk"),
            F.count(F.lit(1)).alias("_pd_chunk_count"),
            # The block of the chunk's first sample: its last sample within the chunk.
            F.max_by(sample, F.when(block_seg == 0, order)).alias("_pd_seg0_last"),
            F.sum(F.when(block_seg == 0, 1).otherwise(0)).alias("_pd_seg0_count"),
        ]
        for run in self.runs:
            brk = F.col(run_col(run.name, "brk"))
            aggregates += [
                F.sum(brk).alias(run_col(run.name, "inner")),
                F.max(F.when(brk == 1, F.col(TS))).alias(run_col(run.name, "last_start")),
            ]
        chunks = df.groupBy(*self.channel_key, CHUNK).agg(*aggregates)

        by_chunk = Window.partitionBy(*self.channel_key).orderBy(CHUNK)
        before = by_chunk.rowsBetween(Window.unboundedPreceding, -1)
        so_far = by_chunk.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        everything = by_chunk.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        later = (
            Window.partitionBy(*self.channel_key)
            .orderBy(F.col(CHUNK).desc())
            .rowsBetween(Window.unboundedPreceding, -1)
        )
        head, tail = F.col("_pd_head"), F.col("_pd_tail")
        for field in ("tstart", "tend", "value"):
            chunks = chunks.withColumn(
                f"_pd_prev_tail_{field}", F.lag(tail.getField(field)).over(by_chunk)
            ).withColumn(f"_pd_next_head_{field}", F.lead(head.getField(field)).over(by_chunk))
        prev_tail = Sample(*(F.col(f"_pd_prev_tail_{f}") for f in ("tstart", "tend", "value")))
        head_sample = Sample(*(head.getField(f) for f in ("tstart", "tend", "value")))
        keep = [*self.channel_key, CHUNK, "_pd_head", "_pd_tail"]
        keep += [f"_pd_prev_tail_{f}" for f in ("tstart", "tend", "value")]
        keep += [f"_pd_next_head_{f}" for f in ("tstart", "tend", "value")]
        for run in self.runs:
            head_brk = F.col(run_col(run.name, "head_brk"))
            inner = F.col(run_col(run.name, "inner"))
            starts_here = F.coalesce(
                F.col(run_col(run.name, "last_start")),
                F.when(head_brk == 1, head.getField("tstart")),
            )
            chunks = (
                chunks.withColumn(
                    run_col(run.name, "head_brk"),
                    _breaks(run, prev_tail, head_sample),
                )
                .withColumn(run_col(run.name, "breaks"), head_brk + inner)
                .withColumn(
                    run_col(run.name, "base"),
                    F.sum(run_col(run.name, "breaks")).over(so_far) - inner,
                )
                .withColumn(
                    run_col(run.name, "head_start"),
                    F.when(head_brk == 1, head.getField("tstart")).otherwise(
                        F.last(starts_here, ignorenulls=True).over(before)
                    ),
                )
                .withColumn(run_col(run.name, "next_head_brk"), F.lead(head_brk).over(by_chunk))
                .withColumn(
                    run_col(run.name, "total"), F.sum(run_col(run.name, "breaks")).over(everything)
                )
            )
            keep += [
                run_col(run.name, c)
                for c in ("head_brk", "base", "head_start", "next_head_brk", "total")
            ]

        # Block ends, from the block's last sample (see _block_end_at), wrapped in a
        # struct: a block's end may be null, and must not be mistaken for none.
        head_brk = F.col(run_col(BLOCK, "head_brk")) == 1
        count = F.col("_pd_chunk_count")
        tail_single = F.when(count == 1, head_brk).otherwise(F.col("_pd_tail_brk") == 1)
        seg0_single = (F.col("_pd_seg0_count") == 1) & head_brk
        tail_ends = F.coalesce(F.col(run_col(BLOCK, "next_head_brk")) == 1, F.lit(True))
        tail_end = _found(_block_end_at(tail, tail_single))
        chunks = (
            chunks.withColumn(COUNT, F.sum(count).over(everything))
            .withColumn(LAST, F.last(tail).over(everything))
            # The end of the block the chunk starts in, if that block ends here.
            .withColumn(
                "_pd_first_end",
                F.when(
                    F.col(run_col(BLOCK, "inner")) > 0,
                    _found(_block_end_at(F.col("_pd_seg0_last"), seg0_single)),
                ).when(tail_ends, tail_end),
            )
            .withColumn(
                "_pd_tail_block_end",
                F.when(tail_ends, tail_end).otherwise(
                    F.last("_pd_first_end", ignorenulls=True).over(later)
                ),
            )
        )
        return chunks.select(*keep, COUNT, LAST, "_pd_tail_block_end")

    def _resolve_neighbours_and_runs(self, df: DataFrame) -> DataFrame:
        """Neighbours and run facts of every sample, across chunk boundaries."""
        first, last = F.col("_pd_first_in_chunk"), F.col("_pd_last_in_chunk")
        df = (
            df.withColumn(
                PREV_TS,
                F.when(first, F.col("_pd_prev_tail_tstart")).otherwise(F.col("_pd_lag_ts")),
            )
            .withColumn(
                PREV_TE, F.when(first, F.col("_pd_prev_tail_tend")).otherwise(F.col("_pd_lag_te"))
            )
            .withColumn(
                PREV_VALUE,
                F.when(first, F.col("_pd_prev_tail_value")).otherwise(
                    F.col("_pd_lag_value").cast("double")
                ),
            )
            .withColumn(
                NEXT_TS,
                F.when(last, F.col("_pd_next_head_tstart")).otherwise(F.col("_pd_lead_ts")),
            )
            .withColumn(
                NEXT_TE, F.when(last, F.col("_pd_next_head_tend")).otherwise(F.col("_pd_lead_te"))
            )
            .withColumn(
                NEXT_VALUE,
                F.when(last, F.col("_pd_next_head_value")).otherwise(
                    F.col("_pd_lead_value").cast("double")
                ),
            )
        )
        for run in self.runs:

            def c(name: str, run: _Run = run) -> Column:
                return F.col(run_col(run.name, name))

            df = (
                df.withColumn(
                    run_col(run.name, "starts"),
                    F.when(first, c("head_brk") == 1).otherwise(c("brk") == 1),
                )
                .withColumn(run_col(run.name, "id"), c("base") + c("seg"))
                .withColumn(
                    run_col(run.name, "start"), F.coalesce(c("seg_start"), c("head_start"))
                )
                .withColumn(
                    run_col(run.name, "next_starts"),
                    F.when(last, c("next_head_brk") == 1).otherwise(c("lead_brk") == 1),
                )
                .withColumn(run_col(run.name, "is_last"), c("id") == c("total"))
            )
        # The last sample of a block (also the series' last sample).
        return df.withColumn(
            run_col(BLOCK, "ends"),
            F.coalesce(F.col(run_col(BLOCK, "next_starts")), F.lit(True)),
        )

    def _with_block_end(self, df: DataFrame) -> DataFrame:
        """Each kept sample's block end: the nearest block end at or after it.

        Every block's last sample is kept, so within a chunk a reverse scan over
        the kept samples finds it; a block that continues past the chunk ends
        where the chunk summary says.
        """
        reverse = (
            Window.partitionBy(*self.key, CHUNK)
            .orderBy(*[F.col(c).desc() for c in self.order])
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        here = F.when(
            F.col(run_col(BLOCK, "ends")),
            _found(
                _block_end_at(
                    F.struct(F.col(TS).alias("tstart"), F.col(TE).alias("tend")),
                    F.col(run_col(BLOCK, "starts")),
                )
            ),
        )
        found = F.coalesce(
            F.last(here, ignorenulls=True).over(reverse), F.col("_pd_tail_block_end")
        )
        return df.withColumn(BLOCK_END, found.getField("end"))


def _row_branch(
    rows: DataFrame,
    rules: Sequence[tuple[PushdownRule, Mapping[int, Pushdown]]],
    config,
    schema: T.StructType,
) -> DataFrame:
    """Raw rows unchanged, and the derived rows of per-sample rules.

    *rows* are tagged with :data:`VARIANT_COL`, one copy per variant; *schema* gives
    the output columns.
    """
    if not rules:
        return rows
    key = [config.container_id_col, config.channel_id_col, VARIANT_COL]
    order = [config.tstart_col, config.tend_col, config.value_col]
    df = rows.withColumn(TS, F.col(order[0]).cast("double")).withColumn(
        TE, F.col(order[1]).cast("double")
    )
    contexts = [
        (rule, SeriesContext(rule, variants, key, order, False, {})) for rule, variants in rules
    ]
    for rule, ctx in contexts:
        df = _with_rule_columns(df, ctx, rule.row_columns(ctx))
    df = _keep(df, contexts, extra=F.col(VARIANT_COL) == RAW_VARIANT)
    kept = [(rule, ctx.kept_stage()) for rule, ctx in contexts]
    for rule, ctx in kept:
        df = _with_rule_columns(df, ctx, rule.kept_columns(ctx))
    return _emit(df, kept, schema, order, raw=F.col(VARIANT_COL) == RAW_VARIANT)


# --- helpers shared by both branches ------------------------------------------------


def _with_rule_columns(df: DataFrame, ctx: SeriesContext, columns: Mapping[str, Column]):
    """Add a rule's columns, defined for the rule's samples only."""
    for name, column in columns.items():
        df = df.withColumn(ctx.name(check_name("column", name)), F.when(ctx.active, column))
    return df


def _keep_col(rule: PushdownRule) -> str:
    return f"_pd_keep__{rule.name}"


def _keep(df: DataFrame, contexts, extra: Column) -> DataFrame:
    """Drop the samples no rule keeps (*extra* marks samples kept regardless).

    The filter reads a flag column behind an always-true non-deterministic guard.
    Spark pushes filters through deterministic projections by substituting their
    column definitions, without a cost check; rule columns that refer to each other
    several times would then multiply into one huge condition.  The guard stops the
    pushdown, so the rule columns are computed once, in their own projections.
    """
    keep = extra
    for rule, ctx in contexts:
        df = df.withColumn(
            _keep_col(rule), F.coalesce(F.when(ctx.active, rule.keep(ctx)), F.lit(False))
        )
        keep = keep | F.col(_keep_col(rule))
    # spark_partition_id() is never negative; Spark folds comparisons on rand() away.
    guarded = F.when(F.spark_partition_id() >= 0, keep)
    return df.withColumn(_KEPT, guarded).where(F.col(_KEPT)).drop(_KEPT)


def _emit(
    df: DataFrame, contexts, schema: T.StructType, order: Sequence[str], raw: Column | None
) -> DataFrame:
    """Explode the rules' emitted rows into rows with the columns and types of *schema*.

    *raw* marks rows passed through unchanged.
    """
    elements = None
    for rule, ctx in contexts:
        emitted = F.when(F.col(_keep_col(rule)), rule.emit(ctx))
        elements = (
            F.when(ctx.active, emitted) if elements is None else elements.when(ctx.active, emitted)
        )
    original = F.lit(False) if raw is None else raw
    if raw is not None:
        # One placeholder element keeps each raw row.
        elements = F.when(raw, F.array(SeriesContext.element())).otherwise(elements)
    element = F.col(_ELEMENT)
    fields = dict(zip(order, ("tstart", "tend", "value"), strict=True))

    def out(col: str) -> Column:
        if col not in df.columns:
            # Columns beyond the channel-data shape (e.g. POI string values) are not
            # carried through the series scan; derived rows have none.
            return F.lit(None).cast(schema[col].dataType).alias(col)
        if col not in fields:
            return F.col(col)
        derived = element.getField(fields[col]).cast(schema[col].dataType)
        # Times left out of an element keep the sample's own.
        keep = (
            original
            if fields[col] == "value"
            else original | element.getField(fields[col]).isNull()
        )
        return F.when(keep, F.col(col)).otherwise(derived).alias(col)

    return df.withColumn(_ELEMENT, F.explode(elements)).select(
        *[out(c) for c in schema.fieldNames()]
    )


def _breaks(run: _Run, prev: Sample, cur: Sample) -> Column:
    """1 where *cur* starts a new run after *prev* (a null break counts as one)."""
    return F.coalesce(run.breaks(prev, cur), F.lit(True)).cast("long")


def _flag(name: str) -> Column:
    return F.coalesce(F.col(name), F.lit(False))


def _nan_free(col: Column) -> Column:
    """Map NaN to null so ``min`` / ``max`` behave like ``numpy.nanmin`` / ``nanmax``."""
    return F.when(~F.isnan(col), col)


def _found(end: Column) -> Column:
    """*end* in a struct that is not null even when *end* is."""
    return F.struct(end.alias("end"))


def _block_end_at(last_sample: Column, single: Column) -> Column:
    """``nanmax`` of a block's ``tend`` values, from its last sample.

    Within a block every ``tend`` equals the next sample's ``tstart``, so the
    maximum is the last ``tend`` or, for blocks of several samples, the last
    ``tstart`` if larger (NaN ``tend`` ignored).
    """
    tend = _nan_free(last_sample.getField("tend"))
    return F.when(single, tend).otherwise(F.greatest(last_sample.getField("tstart"), tend))
