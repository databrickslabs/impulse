"""Delta Lake helpers for status updates and table operations."""

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from .constants import (
    DELTA_TARGET_FILE_SIZE_BYTES,
    OPTIMIZE_TABLES,
    OWNED_TABLES,
    ROLLBACK_TABLES,
    STATUS_SUCCEEDED,
)


def table_exists(spark, catalog: str, schema: str, table_name: str) -> bool:
    """Check whether a Unity Catalog table exists."""
    return spark.catalog.tableExists(f"{catalog}.{schema}.{table_name}")


def create_table_if_missing(
    spark,
    table_name: str,
    schema: StructType,
    cluster_by: list[str],
) -> bool:
    """Create an empty, liquid-clustered Delta table. Returns True when it was created."""
    if spark.catalog.tableExists(table_name):
        return False
    (
        spark.createDataFrame([], schema)
        .write.clusterBy(*cluster_by)
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )
    spark.sql(
        f"ALTER TABLE {table_name} "
        f"SET TBLPROPERTIES (delta.targetFileSize = '{DELTA_TARGET_FILE_SIZE_BYTES}')"
    )
    return True


def drop_owned_tables(spark, catalog: str, schema: str) -> None:
    """Drop the status and silver tables owned by this accelerator (never the schema)."""
    for table_name in OWNED_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}")


def update_status(spark, status_table_name: str, run_id: str, new_status: str) -> None:
    """Set the status of every container in a run."""
    updates = {"status": F.lit(new_status)}
    if new_status == STATUS_SUCCEEDED:
        updates["_processing_done_ts"] = F.current_timestamp()
    DeltaTable.forName(spark, status_table_name).update(
        condition=F.col("run_id") == F.lit(run_id),
        set=updates,
    )


def assign_run(
    spark, status_table_name: str, container_ids: list[int], run_id: str, new_status: str
) -> None:
    """Attach containers to a run_id and set their status in one update."""
    DeltaTable.forName(spark, status_table_name).update(
        condition=F.col("container_id").isin(container_ids),
        set={"run_id": F.lit(run_id), "status": F.lit(new_status)},
    )


def get_files_by_status(status_table_df: DataFrame, status: str) -> DataFrame:
    """Filter a status table DataFrame by a specific status value."""
    return status_table_df.where(F.col("status") == F.lit(status))


def merge_or_append_table(
    spark,
    df: DataFrame,
    table_name: str,
    merge_condition: str,
    cluster_by: list[str],
    merge_schema: bool = True,
) -> None:
    """Create a Delta table on first write; merge on subsequent writes."""
    if not spark.catalog.tableExists(table_name):
        writer = df.write.clusterBy(*cluster_by).format("delta").mode("append")
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        writer.saveAsTable(table_name)
        return

    (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def replace_containers(df: DataFrame, table_name: str, container_ids: list[int]) -> None:
    """Atomically replace all rows of the given containers (idempotent batch re-runs)."""
    ids_sql = ", ".join(str(int(cid)) for cid in container_ids)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"container_id IN ({ids_sql})")
        .saveAsTable(table_name)
    )


def rollback_batch_tables(spark, catalog: str, schema: str, container_ids: list) -> None:
    """Delete batch rows from all silver output tables."""
    if not container_ids:
        return
    for table_name in ROLLBACK_TABLES:
        full_name = f"{catalog}.{schema}.{table_name}"
        if not spark.catalog.tableExists(full_name):
            continue
        DeltaTable.forName(spark, full_name).delete(F.col("container_id").isin(container_ids))


def optimize_tables(
    spark,
    catalog: str,
    schema: str,
    table_names: tuple[str, ...] = OPTIMIZE_TABLES,
) -> None:
    """Run OPTIMIZE (incremental liquid clustering) on tables that exist."""
    for table_name in table_names:
        full_name = f"{catalog}.{schema}.{table_name}"
        if spark.catalog.tableExists(full_name):
            spark.sql(f"OPTIMIZE {full_name}")
