"""Notebook helpers for Databricks ingest jobs."""

import os
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from .constants import (
    CHANNELS_FORMATS,
    DEFAULT_CATALOG,
    DEFAULT_SCHEMA,
    DEFAULT_SOURCE_PATH,
    STATUS_TABLE,
)
from .delta_ops import table_exists
from .source_data import dim_path


def notebook_dir(dbutils) -> str:
    """Return the workspace directory of the running notebook."""
    return os.path.dirname(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    )


def read_catalog_schema_widgets(
    dbutils, catalog_default: str = DEFAULT_CATALOG, schema_default: str = DEFAULT_SCHEMA
) -> tuple[str, str]:
    """Read catalog and schema job widgets."""
    dbutils.widgets.text("catalog", catalog_default)
    dbutils.widgets.text("schema", schema_default)
    return dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")


def read_source_path_widget(dbutils) -> str:
    """Read the source dataset root (directory holding ``fact_signal_data`` and ``dim_*``)."""
    dbutils.widgets.text("source_path", DEFAULT_SOURCE_PATH)
    return normalize_volume_path(dbutils.widgets.get("source_path"))


def read_channels_format_widget(dbutils) -> str:
    """Read and validate the ``channels`` storage format (RLE or RAW)."""
    dbutils.widgets.dropdown("channels_format", "RLE", list(CHANNELS_FORMATS))
    return parse_channels_format(dbutils.widgets.get("channels_format"))


def parse_channels_format(value: str) -> str:
    fmt = value.strip().upper()
    if fmt not in CHANNELS_FORMATS:
        raise ValueError(f"channels_format must be one of {CHANNELS_FORMATS}, got {value!r}")
    return fmt


def parse_positive_int(value: str, name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as e:
        raise ValueError(f"{name} must be a positive integer, got {value!r}") from e
    if parsed < 1:
        raise ValueError(f"{name} must be a positive integer, got {value!r}")
    return parsed


def normalize_volume_path(path: str) -> str:
    """Return ``/Volumes/...`` for ``/Volumes``, ``dbfs:/Volumes`` or ``/dbfs/Volumes`` input."""
    path = path.strip()
    if path.startswith("dbfs:/Volumes/"):
        path = path[len("dbfs:") :]
    elif path.startswith("/dbfs/Volumes/"):
        path = path[len("/dbfs") :]
    if not path.startswith("/Volumes/"):
        raise ValueError(f"expected a Unity Catalog Volume path (/Volumes/...), got {path!r}")
    return path.rstrip("/")


def require_status_table(spark, dbutils, catalog: str, schema: str) -> DataFrame:
    """Load the status table or exit the notebook when it is missing."""
    if not table_exists(spark, catalog, schema, STATUS_TABLE):
        dbutils.notebook.exit("status table not found")
    return spark.read.table(f"{catalog}.{schema}.{STATUS_TABLE}")


def get_batch_context(status_table_df: DataFrame, run_id: str) -> dict[str, Any]:
    """Collect the containers (trips) of the current run_id."""
    batch_df = status_table_df.where(F.col("run_id") == F.lit(run_id))
    rows = batch_df.select("container_id", "trip_hash", "source_path").collect()
    return {
        "batch_df": batch_df,
        "container_ids": [int(r["container_id"]) for r in rows],
        "trip_hashes": [r["trip_hash"] for r in rows],
        "source_dirs": [r["source_path"] for r in rows],
    }


def read_dim(spark, source_path: str, dim: str) -> DataFrame:
    """Read one of the (small) source dimension tables."""
    return spark.read.parquet(dim_path(source_path, dim))
