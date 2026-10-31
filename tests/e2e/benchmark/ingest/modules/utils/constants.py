"""Shared constants of the Renumics fleet telemetry → Impulse silver ingest."""

DEFAULT_CATALOG = "impulse_testing_catalog"
DEFAULT_SCHEMA = "impulse_renumics"
DEFAULT_SOURCE_PATH = (
    "/Volumes/impulse_testing_catalog/impulse_renumics/raw_data/"
    "synthetic-vehicle-fleet-telemetry/assets"
)
DEFAULT_MAX_CONTAINERS_PER_RUN = 20

# Source dataset layout (relative to source_path), as published on Hugging Face
# (renumics/synthetic-vehicle-fleet-telemetry).
FACT_DIR = "fact_signal_data"
DIM_SIGNAL_INFO_DIR = "dim_signal_info"
DIM_TRIP_INFO_DIR = "dim_trip_info"
DIM_VEHICLE_INFO_DIR = "dim_vehicle_info"
DIM_VEHICLE_STATE_DIR = "dim_vehicle_state"
TRIP_HASH_DIR_PREFIX = "trip_hash="

STATUS_UNPROCESSED = "unprocessed"
STATUS_IN_PROGRESS = "in_progress"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"

CHANNELS_FORMAT_RLE = "RLE"
CHANNELS_FORMAT_RAW = "RAW"
CHANNELS_FORMATS = (CHANNELS_FORMAT_RLE, CHANNELS_FORMAT_RAW)

DELTA_TARGET_FILE_SIZE_BYTES = 134_217_728  # 128 MiB

STATUS_TABLE = "status"
CHANNELS_TABLE = "channels"
CHANNEL_METRICS_TABLE = "channel_metrics"
CHANNEL_TAGS_TABLE = "channel_tags"
CONTAINER_METRICS_TABLE = "container_metrics"
CONTAINER_TAGS_TABLE = "container_tags"

SILVER_TABLES = (
    CHANNELS_TABLE,
    CHANNEL_METRICS_TABLE,
    CHANNEL_TAGS_TABLE,
    CONTAINER_METRICS_TABLE,
    CONTAINER_TAGS_TABLE,
)

# Tables owned by this bundle; only these are dropped on reset (never the schema,
# which also holds the raw-data volume).
OWNED_TABLES = (STATUS_TABLE, *SILVER_TABLES)

ROLLBACK_TABLES = SILVER_TABLES

OPTIMIZE_TABLES = SILVER_TABLES
