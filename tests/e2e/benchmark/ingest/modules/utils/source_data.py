"""Helpers for the Hugging Face `renumics/synthetic-vehicle-fleet-telemetry` source layout.

The fact table is stored as ``fact_signal_data/trip_hash=<hash>/partition=<n>/*.parquet``:
one directory per trip (= one Impulse container), split into signal partitions. Every trip
directory holds exactly one ``session_id`` / ``vehicle_id`` / ``vehicle_state_id``, which the
Parquet footer statistics expose without scanning the data.
"""

from .constants import (
    DIM_SIGNAL_INFO_DIR,
    DIM_TRIP_INFO_DIR,
    DIM_VEHICLE_INFO_DIR,
    DIM_VEHICLE_STATE_DIR,
    FACT_DIR,
    TRIP_HASH_DIR_PREFIX,
)

TRIP_KEY_COLUMNS = ("session_id", "vehicle_id", "vehicle_state_id")

# Captures the trip hash from a fact file path (used with Spark regexp_extract).
TRIP_HASH_PATTERN = r"/trip_hash=([^/]+)/"


def fact_root(source_path: str) -> str:
    return f"{source_path.rstrip('/')}/{FACT_DIR}"


def trip_dir(source_path: str, trip_hash: str) -> str:
    return f"{fact_root(source_path)}/{TRIP_HASH_DIR_PREFIX}{trip_hash}"


def dim_path(source_path: str, dim: str) -> str:
    if dim not in (
        DIM_SIGNAL_INFO_DIR,
        DIM_TRIP_INFO_DIR,
        DIM_VEHICLE_INFO_DIR,
        DIM_VEHICLE_STATE_DIR,
    ):
        raise ValueError(f"unknown dimension table: {dim}")
    return f"{source_path.rstrip('/')}/{dim}"


def to_local_path(path: str) -> str:
    """Strip the ``dbfs:`` scheme so a UC Volume path can be opened with local file APIs."""
    return path[len("dbfs:") :] if path.startswith("dbfs:") else path


def read_trip_keys(parquet_path: str) -> dict[str, int]:
    """Read session/vehicle/state ids of a trip from one fact file's footer statistics.

    Raises ValueError when statistics are missing or a key is not constant in the file,
    because the container mapping relies on exactly one session per trip directory.
    """
    import pyarrow.parquet as pq

    metadata = pq.ParquetFile(to_local_path(parquet_path)).metadata
    names = [metadata.schema.column(i).name for i in range(metadata.num_columns)]
    keys: dict[str, int] = {}
    for col in TRIP_KEY_COLUMNS:
        if col not in names:
            raise ValueError(f"{parquet_path}: column {col} missing")
        idx = names.index(col)
        mins, maxs = set(), set()
        for rg in range(metadata.num_row_groups):
            stats = metadata.row_group(rg).column(idx).statistics
            if stats is None or not stats.has_min_max:
                raise ValueError(f"{parquet_path}: no min/max statistics for {col}")
            mins.add(stats.min)
            maxs.add(stats.max)
        values = mins | maxs
        if len(values) != 1:
            raise ValueError(f"{parquet_path}: {col} is not constant ({sorted(values)})")
        keys[col] = int(values.pop())
    return keys
