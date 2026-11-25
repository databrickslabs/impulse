"""``solve()`` results produced in Spark, without the pandas UDF.

When every selection of a solve is a pushdown as a whole whose rule builds its
result in Spark (:meth:`PushdownPlan.native_selections`), each container's result
row is aggregated from the shipped rows.  It must equal what
:meth:`DefaultSolver._solve_udf` emits, including which containers get a row: every
container with shipped rows (presence rows included), like the containers that
reach the UDF.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from impulse_query_engine.analyze.query.optimizer import (
    VARIANT_COL,
    Pushdown,
    PushdownPlan,
    series_order,
)

from .solver_config import SolverConfig

_KEY = "_ns_key"


def native_solve_rows(
    rows: DataFrame,
    schema: T.StructType,
    native: Sequence[tuple[Pushdown, int]],
    selections: Sequence,
    config: SolverConfig,
) -> DataFrame:
    """One result row per container, aggregated from the shipped rows in Spark.

    Parameters
    ----------
    rows : pyspark.sql.DataFrame
        Finished derived rows joined with the channel matches (``selector_ids``),
        partitioned by container; no raw rows.
    schema : pyspark.sql.types.StructType
        The solve's output schema (:meth:`QuerySolver._build_solve_output_schema`).
    native : Sequence of (Pushdown, int)
        The pushdown and variant computing each selection, in order.
    selections : Sequence
        The solve's selections.
    config : SolverConfig
        Provides the internal column names.

    Returns
    -------
    pyspark.sql.DataFrame
        Rows with *schema*.
    """
    cid = config.container_id_col
    selector_ids = PushdownPlan.channel_selector_ids("selector_ids")
    # Rows in the order of the UDF's cache: the sort key first, then the element.
    keyed = F.struct(
        F.struct(*series_order(config)).alias(_KEY),
        F.col(config.tstart_col).cast("double").alias("tstart"),
        F.col(config.tend_col).cast("double").alias("tend"),
        F.col(config.value_col).cast("double").alias("value"),
    )
    values = []
    for (pushdown, variant), selection, field in zip(
        native, selections, schema.fields[1:], strict=True
    ):
        sid = F.lit(pushdown.selector.selector_id).cast("long")
        feeds = (F.col(VARIANT_COL) == F.lit(variant)) & F.array_contains(selector_ids, sid)
        ordered = F.sort_array(F.collect_list(F.when(feeds, keyed)))
        elements = F.transform(
            ordered,
            lambda r: F.struct(
                r["tstart"].alias("tstart"), r["tend"].alias("tend"), r["value"].alias("value")
            ),
        )
        value = pushdown.rule.native_value(pushdown, elements)
        values.append(value.cast(field.dataType).alias(selection._alias))
    return rows.groupBy(cid).agg(*values).select(*schema.fieldNames())
