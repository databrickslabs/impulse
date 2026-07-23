"""Helper functions for Report orchestration.

Extracted from Report to keep the class focused on orchestration
and reduce its size.
"""

from __future__ import annotations

import uuid
from functools import reduce
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql.types import StructType

    from impulse_query_engine.analyze.metadata.time_series_expression import (
        TimeSeriesExpression,
    )
    from impulse_query_engine.analyze.query.query_builder import QueryBuilder
    from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
    from impulse_reporting.incremental.definition_hash_comparator import (
        DefinitionHashComparator,
    )
    from impulse_reporting.persist.report_storage import Sink, WriterFactory


def build_batches(
    expressions: list[TimeSeriesExpression],
    batch_size: int,
) -> list[list[TimeSeriesExpression]]:
    """Selector-aware best-fit-decreasing bin packing.

    Groups expressions that share ``TimeSeriesSelector`` instances to
    maximise data locality and minimise cross-batch selector duplication.

    Parameters
    ----------
    expressions : list[TimeSeriesExpression]
        Expressions to partition into batches.
    batch_size : int
        Maximum number of unique ``TimeSeriesSelector`` instances per batch.

    Returns
    -------
    list[list[TimeSeriesExpression]]
        Partitioned batches of expressions.
    """
    n = len(expressions)
    if n == 0:
        return []

    # --- Phase 1: Map each expression index to the set of selector ids
    # it references.  Using selector_id() gives a stable, content-based key.
    selector_ids: dict[int, set[int]] = {}
    for i, expr in enumerate(expressions):
        sels = expr.get_selectors()
        selector_ids[i] = {s.selector_id for s in sels}

    # --- Phase 2: Fast-path – if the total number of distinct selectors across
    # ALL expressions already fits in a single batch, skip packing entirely.
    all_selector_ids: set[int] = set()
    for s in selector_ids.values():
        all_selector_ids |= s
    if len(all_selector_ids) <= batch_size:
        return [list(expressions)]

    # --- Phase 3: Best-Fit Decreasing (BFD) bin-packing.
    #
    # Sort expressions by the number of selectors they use (heaviest first).
    # Then for each expression find the existing batch where it causes the
    # smallest growth of the selector set (= highest overlap) without
    # exceeding batch_size.  If no batch can accommodate it, open a new one.
    #
    # Why BFD?  Expressions that share the same TimeSeriesSelector objects
    # (e.g. same channel/signal) are naturally packed together, maximising
    # data locality during the Spark solve step and minimising the number
    # of redundant selector reads across batches.
    order = sorted(range(n), key=lambda i: len(selector_ids[i]), reverse=True)

    final_batches: list[list[int]] = []  # expression indices per batch
    batch_selector_ids: list[set[int]] = []  # accumulated selector ids per batch

    for i in order:
        expr_sels = selector_ids[i]
        best_idx = -1
        best_overlap = -1

        # Find the batch with the most selector overlap that still fits
        for bi in range(len(final_batches)):
            combined = batch_selector_ids[bi] | expr_sels
            if len(combined) <= batch_size:
                overlap = len(batch_selector_ids[bi] & expr_sels)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_idx = bi

        if best_idx >= 0:
            # Place expression into the best-fitting existing batch
            final_batches[best_idx].append(i)
            batch_selector_ids[best_idx] = batch_selector_ids[best_idx] | expr_sels
        else:
            # No existing batch can fit this expression – start a new batch
            final_batches.append([i])
            batch_selector_ids.append(set(expr_sels))

    return [[expressions[i] for i in batch] for batch in final_batches]


def split_by_hash_change(
    items_by_type: dict[str, list],
    type_enum,
    sink: Sink | None,
    spark: SparkSession,
    hash_comparator: DefinitionHashComparator,
    kind: str = "event",
) -> tuple[dict[str, list], dict[str, list], dict[str, list[int]]]:
    """Split items into changed/unchanged using definition-hash comparison.

    Parameters
    ----------
    items_by_type : dict[str, list]
        ``{type_name: [items]}`` as returned by ``_group_*_by_type()``.
    type_enum : type
        ``EventType``, ``AggregationType``, or ``ChannelType`` enum class.
    sink : Sink | None
        Report sink (``None`` in sinkless mode).
    spark : SparkSession
        Active Spark session.
    hash_comparator : DefinitionHashComparator
        Comparator instance.
    kind : str
        One of ``"event"``, ``"aggregation"``, or ``"channel"`` — selects which
        comparator method to use.

    Returns
    -------
    tuple[dict, dict, dict]
        ``(changed_by_type, unchanged_by_type, changed_ids)``
    """
    comparators = {
        "event": hash_comparator.group_events_by_hash_change,
        "aggregation": hash_comparator.group_aggregations_by_hash_change,
        "channel": hash_comparator.group_calculated_channels_by_hash_change,
    }
    if kind not in comparators:
        raise ValueError(f"Unsupported kind '{kind}'; expected one of {sorted(comparators)}.")
    compare = comparators[kind]

    changed_by_type: dict[str, list] = {}
    unchanged_by_type: dict[str, list] = {}
    changed_ids: dict[str, list[int]] = {}

    for type_name, items in items_by_type.items():
        if not items:
            continue

        if sink is None:
            # Sinkless mode: everything is "changed"
            changed_by_type[type_name] = items
            changed_ids[type_name] = [item.get_id() for item in items]
            continue

        dim_table = sink.config.get_output_uri_dimension_table(type_enum[type_name])
        changed, unchanged = compare(items, dim_table)

        if changed:
            changed_by_type[type_name] = changed
            changed_ids[type_name] = [item.get_id() for item in changed]
        if unchanged:
            unchanged_by_type[type_name] = unchanged

    return changed_by_type, unchanged_by_type, changed_ids


def collect_solvable_expressions(
    items_by_type: dict[str, list],
    type_enum,
    exclude_cls: type | None = None,
) -> list[TimeSeriesExpression]:
    """Collect all non-None expressions from typed items.

    Parameters
    ----------
    items_by_type : dict[str, list]
        ``{type_name: [items]}``.
    type_enum : type
        ``EventType`` or ``AggregationType`` enum class.
    exclude_cls : type | None
        Skip any type whose class ``issubclass(cls, exclude_cls)``.

    Returns
    -------
    list[TimeSeriesExpression]
        Flat list of expressions.
    """
    expressions: list[TimeSeriesExpression] = []
    for type_name, items in items_by_type.items():
        cls = type_enum[type_name].value
        if exclude_cls is not None and issubclass(cls, exclude_cls):
            continue
        for item in items:
            expr = item.get_expression()
            if expr is not None:
                expressions.append(expr)
    return expressions


def dispatch_events(
    spark: SparkSession,
    events_by_type: dict[str, list],
    type_enum,
    solved_df: DataFrame | None,
    query: QueryBuilder,
    solver: QuerySolver,
    pre_filtered_containers_df: DataFrame | None,
    container_event_cls: type,
) -> dict:
    """Dispatch ``determine_events`` calls per type.

    Solvable event types receive ``solved_df``; ``ContainerEvent`` receives
    ``query``/``solver``.

    Parameters
    ----------
    spark : SparkSession
    events_by_type : dict[str, list]
    type_enum : EventType enum
    solved_df : DataFrame | None
    query : QueryBuilder
    solver : QuerySolver
    pre_filtered_containers_df : DataFrame | None
    container_event_cls : type
        The ``ContainerEvent`` class.

    Returns
    -------
    dict
        ``event_dfs``
    """
    event_dfs: dict = {}

    for type_name, events in events_by_type.items():
        if not events:
            continue
        cls = type_enum[type_name].value

        if issubclass(cls, container_event_cls):
            # ContainerEvent uses filter pipeline, not solved_df
            event_dfs[type_name] = cls.determine_events(
                spark,
                events,
                query=query,
                solver=solver,
                pre_filtered_containers_df=pre_filtered_containers_df,
            )
        else:
            event_dfs[type_name] = cls.determine_events(
                spark,
                events,
                solved_df=solved_df,
            )

    return event_dfs


def dispatch_aggregations(
    spark: SparkSession,
    aggs_by_type: dict[str, list],
    type_enum,
    solved_df: DataFrame | None,
) -> dict:
    """Dispatch ``determine_aggregations`` calls per type.

    Parameters
    ----------
    spark : SparkSession
    aggs_by_type : dict[str, list]
    type_enum : AggregationType enum
    solved_df : DataFrame | None

    Returns
    -------
    dict
        ``aggregation_dfs``
    """
    aggregation_dfs: dict = {}

    for type_name, aggregations in aggs_by_type.items():
        if not aggregations:
            continue
        cls = type_enum[type_name].value

        aggregation_dfs[type_name] = cls.determine_aggregations(
            spark,
            aggregations,
            solved_df=solved_df,
        )

    return aggregation_dfs


def dispatch_calculated_channels(
    spark: SparkSession,
    channels_by_type: dict[str, list],
    type_enum,
    query: QueryBuilder,
    solver: QuerySolver,
    pre_filtered_containers_df: DataFrame | None,
) -> dict:
    """Dispatch ``determine_calculated_channels`` calls per type.

    Unlike events/aggregations, calculated channels never ride the wide
    ``solved_df`` — each type drives its own narrow solve via
    ``query.solve_calculated_channels`` (the ``ContainerEvent`` pattern), so this
    always passes ``spark``/``query``/``solver``/``pre_filtered_containers_df``.

    Parameters
    ----------
    spark : SparkSession
    channels_by_type : dict[str, list]
    type_enum : ChannelType enum
    query : QueryBuilder
    solver : QuerySolver
    pre_filtered_containers_df : DataFrame | None

    Returns
    -------
    dict
        ``channel_dfs``
    """
    channel_dfs: dict = {}

    for type_name, channels in channels_by_type.items():
        if not channels:
            continue
        cls = type_enum[type_name].value
        channel_dfs[type_name] = cls.determine_calculated_channels(
            spark,
            channels,
            query=query,
            solver=solver,
            pre_filtered_containers_df=pre_filtered_containers_df,
        )

    return channel_dfs


def solve_expressions_batched(
    spark: SparkSession,
    expressions: list[TimeSeriesExpression],
    query: QueryBuilder,
    solver: QuerySolver,
    batch_size: int,
    *,
    has_sink: bool = False,
    catalog: str = None,
    schema: str = None,
    pre_filtered_containers_df: DataFrame = None,
) -> DataFrame | None:
    """Solve all expressions in configurable batches and return a joined wide DataFrame.

    Each batch is solved independently via ``query.select(*batch_exprs).solve(...)``.
    When a sink is configured the intermediate result is persisted as a temporary
    Delta table (``__impulse_temp_<run_id>_<batch_idx>``); otherwise a Spark temp view
    is used.  After all batches are solved the per-batch DataFrames are joined on
    ``container_id`` with a full outer join.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    expressions : list[TimeSeriesExpression]
        Expressions to solve.
    query : QueryBuilder
        Query builder instance.
    solver : QuerySolver
        Query solver instance.
    batch_size : int
        Maximum number of unique selectors per batch (passed to ``build_batches``).
    has_sink : bool
        Whether a Unity Catalog sink is configured.
    catalog : str, optional
        Unity Catalog catalog name (required when *has_sink* is ``True``).
    schema : str, optional
        Unity Catalog schema name (required when *has_sink* is ``True``).
    pre_filtered_containers_df : DataFrame, optional
        Pre-filtered containers for incremental processing.

    Returns
    -------
    DataFrame | None
        Wide DataFrame with one column per expression plus ``container_id``,
        or ``None`` if *expressions* is empty.
    """
    if not expressions:
        return None

    run_id = uuid.uuid4().hex[:8]
    batches = build_batches(expressions, batch_size)

    batch_names: list[str] = []
    for batch_idx, batch_exprs in enumerate(batches):
        batch_query = query.select(*batch_exprs)
        batch_df = batch_query.solve(
            spark=spark,
            solver=solver,
            pre_filtered_containers_df=pre_filtered_containers_df,
        )

        if has_sink:
            table_name = f"__impulse_temp_{run_id}_{batch_idx}"
            fq_name = f"`{catalog}`.`{schema}`.`{table_name}`"
            batch_df.write.format("delta").mode("overwrite").saveAsTable(fq_name)
            batch_names.append(fq_name)
        else:
            view_name = f"__impulse_temp_{run_id}_{batch_idx}"
            batch_df.createOrReplaceTempView(view_name)
            batch_names.append(view_name)

    cid_col = solver.config.container_id_col
    dfs = [spark.table(name) for name in batch_names]

    result = dfs[0]
    for i in range(1, len(dfs)):
        result = result.join(dfs[i], on=cid_col, how="full_outer")

    return result


def cleanup_temp_tables(spark: SparkSession, catalog: str, schema: str) -> None:
    """Drop leftover ``__impulse_temp_*`` Delta tables from previous runs.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    catalog : str
        Unity Catalog catalog name.
    schema : str
        Unity Catalog schema name.
    """
    tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}` LIKE '__impulse_temp_*'")
    for row in tables.collect():
        table_name = row["tableName"]
        spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{table_name}`")


# ---------------------------------------------------------------------------
# Entity orchestration + persistence helpers
#
# Generic over the entity type-enum (``EventType`` / ``AggregationType`` /
# ``ChannelType``); shared by events, aggregations, and calculated channels.
# ``persist_facts_incremental`` groups by output table and unions shared-table
# types into a single ``merge_incremental``; ``merge_keys`` accepts a per-type
# callable.
# ---------------------------------------------------------------------------


def group_selectables_by_type(items: list, type_enum) -> dict[str, list]:
    """Bucket *items* into ``{type_enum member name: [items]}`` by ``isinstance``.

    Each item is assigned to the first type whose ``.value`` class it is an
    instance of.  Every enum member gets a (possibly empty) bucket.

    Parameters
    ----------
    items : list
        Entities to group (e.g. a report's calculated channels).
    type_enum : Enum
        The entity type-enum (e.g. ``ChannelType``); each member's ``.value`` is
        the entity class.

    Returns
    -------
    dict[str, list]
        ``{type_name: [items]}``.
    """
    by_type: dict[str, list] = {member.name: [] for member in type_enum}
    for item in items:
        for type_name in by_type:
            if isinstance(item, type_enum[type_name].value):
                by_type[type_name].append(item)
                break
    return by_type


def merge_changed_unchanged(
    changed_by_type: dict[str, DataFrame | None],
    unchanged_by_type: dict[str, DataFrame | None],
) -> dict[str, dict[str, DataFrame | None]]:
    """Combine changed/unchanged dispatch results into per-type structured dicts.

    Parameters
    ----------
    changed_by_type : dict[str, DataFrame | None]
        Solved facts for changed definitions, keyed by type name.
    unchanged_by_type : dict[str, DataFrame | None]
        Solved facts for unchanged definitions, keyed by type name.

    Returns
    -------
    dict[str, dict[str, DataFrame | None]]
        ``{type_name: {"changed": df, "unchanged": df}}`` for every type present
        in either input.
    """
    result: dict[str, dict[str, DataFrame | None]] = {}
    for type_name in set(changed_by_type) | set(unchanged_by_type):
        result[type_name] = {
            "changed": changed_by_type.get(type_name),
            "unchanged": unchanged_by_type.get(type_name),
        }
    return result


def build_metadata_dfs(
    items_by_type: dict[str, list],
    type_enum,
    spark: SparkSession,
) -> dict[str, DataFrame]:
    """Build the dimension (metadata) DataFrame for each non-empty type.

    Parameters
    ----------
    items_by_type : dict[str, list]
        ``{type_name: [items]}`` as returned by :func:`group_selectables_by_type`.
    type_enum : Enum
        The entity type-enum; ``type_enum[name].value`` is the entity class whose
        ``determine_metadata_df(spark, items)`` classmethod builds the dimension.
    spark : SparkSession
        Active Spark session.

    Returns
    -------
    dict[str, DataFrame]
        ``{type_name: metadata_df}`` for each type with at least one item.
    """
    metadata_dfs: dict[str, DataFrame] = {}
    for type_name, items in items_by_type.items():
        if not items:
            continue
        cls = type_enum[type_name].value
        metadata_dfs[type_name] = cls.determine_metadata_df(spark, items)
    return metadata_dfs


def _fact_dfs_for_table(dfs) -> list[DataFrame]:
    """Flatten a per-type fact value into a list of DataFrames to write.

    Accepts the structured ``{"changed": df, "unchanged": df}`` dict (either may
    be ``None``) or a bare DataFrame; ``None`` values are dropped.
    """
    if isinstance(dfs, dict):
        return [dfs[key] for key in ("changed", "unchanged") if dfs.get(key) is not None]
    return [dfs] if dfs is not None else []


def persist_facts_full(dfs_by_type: dict, type_enum, writer_factory: WriterFactory) -> None:
    """Full-overwrite persist of fact DataFrames, grouped by output table.

    Groups per-type facts by their fact-table name (so types sharing a table are
    written together), then writes each table via the entity writer.

    Parameters
    ----------
    dfs_by_type : dict
        ``{type_name: value}`` where value is a structured
        ``{"changed", "unchanged"}`` dict or a bare DataFrame.
    type_enum : Enum
        The entity type-enum (resolves fact-table names + writer).
    writer_factory : WriterFactory
        Factory producing the entity writer.
    """
    dfs_by_table: dict[str, list[DataFrame]] = {}
    for type_name, dfs in dfs_by_type.items():
        table_name = type_enum[type_name].get_fact_table_name()
        dfs_by_table.setdefault(table_name, []).extend(_fact_dfs_for_table(dfs))

    for table_name, dfs_list in dfs_by_table.items():
        if not dfs_list:
            continue
        entity_type = type_enum.get_any_for_fact_table(table_name)
        writer = writer_factory.create_writer(entity_type)
        schema, uri = writer.extract_fact_schema_and_output_uri(entity_type)
        writer.write(dfs_list, schema=schema, uri=uri)


def persist_dimensions_full(
    metadata_dfs_by_type: dict[str, DataFrame],
    type_enum,
    writer_factory: WriterFactory,
) -> None:
    """Full-overwrite persist of dimension DataFrames, grouped by output table.

    Parameters
    ----------
    metadata_dfs_by_type : dict[str, DataFrame]
        ``{type_name: metadata_df}``.
    type_enum : Enum
        The entity type-enum (resolves dimension-table names + writer).
    writer_factory : WriterFactory
        Factory producing the entity writer.
    """
    dfs_by_table: dict[str, list[DataFrame]] = {}
    for type_name, metadata_df in metadata_dfs_by_type.items():
        table_name = type_enum[type_name].get_dimension_table_name()
        dfs_by_table.setdefault(table_name, []).append(metadata_df)

    for table_name, dfs_list in dfs_by_table.items():
        if not dfs_list:
            continue
        entity_type = type_enum.get_any_for_dimension_table(table_name)
        writer = writer_factory.create_writer(entity_type)
        schema, uri = writer.extract_metadata_schema_and_output_uri(entity_type)
        writer.write(dfs_list, schema=schema, uri=uri)


def _resolve_merge_keys(merge_keys, entity_type) -> list[str]:
    """Return merge keys, resolving a per-type callable if one was supplied."""
    return merge_keys(entity_type) if callable(merge_keys) else merge_keys


def persist_facts_incremental(
    dfs_by_type: dict,
    type_enum,
    sink: Sink,
    transform_fn: Callable[[DataFrame, StructType], DataFrame],
    *,
    id_column: str,
    merge_keys: list[str] | Callable[[object], list[str]],
    changed_ids: dict[str, list[int]],
    processed_container_ids: list | None = None,
    updated_container_ids: list | None = None,
    container_id_col: str = "container_id",
) -> None:
    """Incremental persist of fact DataFrames in a single MERGE per output table.

    Per-type facts are grouped by fact-table name, so types sharing a table
    (e.g. ``StatsAggregator`` + ``PointValueAggregator``, or mixed event types)
    persist together with no clobber. Per table, changed rows (all containers)
    and unchanged rows (reprocessed containers) are ``unionByName``-combined into
    one ``sink.merge_incremental`` source; the delete scope prunes stale rows a
    shrunk container leaves behind. The union is collision-free because an entity
    is in exactly one bucket and ``merge_keys`` always includes its id.

    Parameters
    ----------
    dfs_by_type : dict
        ``{type_name: value}`` where value is a structured
        ``{"changed", "unchanged"}`` dict or a bare DataFrame (treated as
        unchanged).
    type_enum : Enum
        The entity type-enum (resolves fact schema/uri + writer).
    sink : Sink
        Target sink exposing ``merge_incremental``.
    transform_fn : Callable[[DataFrame, StructType], DataFrame]
        Prepares a DataFrame for persistence (column projection + metadata).
    id_column : str
        Entity id column scoping changed-definition deletes (e.g.
        ``"channel_id"``, ``"visual_id"``, ``"event_id"``).
    merge_keys : list[str] or Callable
        MERGE keys; a callable is resolved per entity type (mirrors
        ``Report._get_aggregation_merge_keys``).
    changed_ids : dict[str, list[int]]
        ``{type_name: [ids]}`` with changed definitions recomputed over all
        containers.
    processed_container_ids : list, optional
        Ids of containers recomputed this run (new + updated). Gates whether a
        table is written — new containers carry no delete scope but must still be
        inserted. Empty/None + no changed ids → nothing to do.
    updated_container_ids : list, optional
        Ids of UPDATED containers only (present in gold, refreshed in silver).
        Scopes the delete-by-source; new containers are excluded because they have
        no gold rows to prune. Empty/None → no container-scoped delete.
    container_id_col : str, optional
        Gold fact-table container column, by default ``"container_id"``.
    """
    from impulse_reporting.persist.report_storage import WriterFactory

    processed_container_ids = processed_container_ids or []
    updated_container_ids = updated_container_ids or []

    # Group per-type facts by output table so shared-table types persist together.
    changed_by_table: dict[str, list[DataFrame]] = {}
    unchanged_by_table: dict[str, list[DataFrame]] = {}
    changed_ids_by_table: dict[str, list[int]] = {}
    for type_name, data in dfs_by_type.items():
        table_name = type_enum[type_name].get_fact_table_name()
        if isinstance(data, dict):
            changed_df = data.get("changed")
            unchanged_df = data.get("unchanged")
            if changed_df is not None and type_name in changed_ids:
                changed_by_table.setdefault(table_name, []).append(changed_df)
                changed_ids_by_table.setdefault(table_name, []).extend(changed_ids[type_name])
            if unchanged_df is not None:
                unchanged_by_table.setdefault(table_name, []).append(unchanged_df)
        elif data is not None:
            unchanged_by_table.setdefault(table_name, []).append(data)

    factory = WriterFactory(sink)
    for table_name in set(changed_by_table) | set(unchanged_by_table):
        entity_type = type_enum.get_any_for_fact_table(table_name)
        writer = factory.create_writer(entity_type)
        schema, uri = writer.extract_fact_schema_and_output_uri(entity_type)
        keys = _resolve_merge_keys(merge_keys, entity_type)

        # Skip when there is nothing to write: no containers processed (new or
        # updated) and no changed definitions. Keeps idempotent runs byte-identical
        # (no no-op MERGE commit).
        table_changed_ids = changed_ids_by_table.get(table_name, [])
        if not processed_container_ids and not table_changed_ids:
            continue

        # Scope the delete-by-source: updated containers (only they can hold stale
        # rows — new ones have none) and changed-definition entities.
        delete_conditions = []
        if updated_container_ids:
            delete_conditions.append(
                F.col(f"target.{container_id_col}").isin(updated_container_ids)
            )
        if table_changed_ids:
            delete_conditions.append(F.col(f"target.{id_column}").isin(table_changed_ids))

        # Single source: changed rows (all containers) + unchanged rows (processed
        # containers), unioned by name into one MERGE.
        source_dfs = [
            transform_fn(df, schema)
            for df in changed_by_table.get(table_name, []) + unchanged_by_table.get(table_name, [])
        ]
        source = reduce(lambda a, b: a.unionByName(b), source_dfs)
        sink.merge_incremental(source, uri, keys, delete_conditions=delete_conditions)


def persist_dimensions_incremental(
    metadata_dfs_by_type: dict[str, DataFrame],
    type_enum,
    sink: Sink,
    transform_fn: Callable[[DataFrame, StructType], DataFrame],
    *,
    merge_keys: list[str],
) -> None:
    """Incremental persist of dimension DataFrames (always ``upsert``), per type.

    Parameters
    ----------
    metadata_dfs_by_type : dict[str, DataFrame]
        ``{type_name: metadata_df}``.
    type_enum : Enum
        The entity type-enum (resolves dimension schema/uri + writer).
    sink : Sink
        Target sink exposing ``upsert``.
    transform_fn : Callable[[DataFrame, StructType], DataFrame]
        Prepares a DataFrame for persistence (column projection + metadata).
    merge_keys : list[str]
        MERGE keys for the dimension upsert (e.g. ``["channel_id"]``).
    """
    from impulse_reporting.persist.report_storage import WriterFactory

    factory = WriterFactory(sink)
    for type_name, metadata_df in metadata_dfs_by_type.items():
        entity_type = type_enum[type_name]
        writer = factory.create_writer(entity_type)
        schema, uri = writer.extract_metadata_schema_and_output_uri(entity_type)
        sink.upsert(transform_fn(metadata_df, schema), uri, merge_keys)
