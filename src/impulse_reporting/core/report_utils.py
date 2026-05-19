"""Helper functions for Report orchestration.

Extracted from Report to keep the class focused on orchestration
and reduce its size.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession

if TYPE_CHECKING:
    from impulse_query_engine.analyze.metadata.time_series_expression import (
        TimeSeriesExpression,
    )
    from impulse_query_engine.analyze.query.query_builder import QueryBuilder
    from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
    from impulse_reporting.incremental.definition_hash_comparator import (
        DefinitionHashComparator,
    )
    from impulse_reporting.persist.report_storage import Sink


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
    is_event: bool = True,
) -> tuple[dict[str, list], dict[str, list], dict[str, list[int]]]:
    """Split items into changed/unchanged using definition-hash comparison.

    Parameters
    ----------
    items_by_type : dict[str, list]
        ``{type_name: [items]}`` as returned by ``_group_*_by_type()``.
    type_enum : type
        ``EventType`` or ``AggregationType`` enum class.
    sink : Sink | None
        Report sink (``None`` in sinkless mode).
    spark : SparkSession
        Active Spark session.
    hash_comparator : DefinitionHashComparator
        Comparator instance.
    is_event : bool
        ``True`` for events, ``False`` for aggregations.

    Returns
    -------
    tuple[dict, dict, dict]
        ``(changed_by_type, unchanged_by_type, changed_ids)``
    """
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

        if is_event:
            changed, unchanged = hash_comparator.group_events_by_hash_change(items, dim_table)
        else:
            changed, unchanged = hash_comparator.group_aggregations_by_hash_change(
                items, dim_table
            )

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
