"""
PySpark custom data sources for reading MDF4 files.

Provides three data sources:
  - "mdf_signals": Reads signal time-series data (file_uri, channel_id, time, value)
  - "mdf_metadata": Reads channel metadata (file_uri, channel_id, group_idx, channel_idx, channel_name, unit, header_datetime, md_comment)
  - "mdf_masters": Reads each group's master time base, one row per original
        sample (file_uri, group_idx, timestamp) — used to reverse RLE signals.

Usage:
    from databricks.sdk import WorkspaceClient
    from impulse_data_sources.mdf import register_mdf_datasources

    register_mdf_datasources(spark, WorkspaceClient())

    signals_df = (
        spark.read.format("mdf_signals")
        .option("path", "/mnt/data/mdf_files")
        .option("files", "batch_a/run_1/file1.mf4,/other/volume/file2.mf4")  # optional
        .option("target_partition_mb", "64")
        .load()
    )

    metadata_df = (
        spark.read.format("mdf_metadata")
        .option("path", "/mnt/data/mdf_files")
        .load()  # discovers all *.mf4 under path, including subdirectories
    )

Options (shared by ``mdf_signals``, ``mdf_metadata``, and ``mdf_masters``):
    path (required): Base directory for file discovery and relative ``files`` entries.
    files (optional): Comma-separated list of MDF4 files to read. Each entry may be
                      an absolute path or a path relative to ``path``. When set, only
                      these files are read (no directory scan). When omitted, every
                      ``*.mf4`` file under ``path`` is discovered recursively.
    target_partition_mb (optional, signals/masters): Target partition size in MB.
                      Default 64.
    max_groups_per_partition (optional, signals/masters): Max small channel groups
        coalesced into one task. Default 64.
"""

import os
from typing import TYPE_CHECKING

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from .schemas import METADATA_SCHEMA

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

_ws: "WorkspaceClient | None" = None


def register_mdf_datasources(spark, ws: "WorkspaceClient"):
    """Register all MDF data sources and enable read telemetry.

    Verifies the workspace client, stores it module-wide for partition-planning
    telemetry, and registers ``mdf_signals``, ``mdf_metadata``, and
    ``mdf_masters`` with Spark.
    """
    from impulse_query_engine import __version__
    from impulse_query_engine.telemetry import verify_workspace_client

    global _ws
    _ws = verify_workspace_client(ws, "databricks-impulse", __version__)
    spark.dataSource.register(MdfSignalsDataSource)
    spark.dataSource.register(MdfMetadataDataSource)
    spark.dataSource.register(MdfMastersDataSource)
    return _ws


def _emit_read_telemetry(source: str) -> None:
    if _ws is not None:
        from impulse_query_engine.telemetry import log_telemetry

        log_telemetry(_ws, "mdf", source)


def _discover_mf4_files(base_path: str) -> list[str]:
    """Return sorted paths to every ``.mf4`` file under ``base_path`` (recursive)."""
    found: list[str] = []
    for root, _dirs, files in os.walk(base_path):
        for name in files:
            if name.lower().endswith(".mf4"):
                found.append(os.path.join(root, name))
    return sorted(found)


def _resolve_file_entry(base_path: str, entry: str) -> str:
    """Resolve one ``files`` option entry to a normalized filesystem path."""
    if os.path.isabs(entry):
        return os.path.normpath(entry)
    return os.path.normpath(os.path.join(base_path, entry))


def _resolve_file_list(options):
    """Resolve the list of MDF4 file paths from data source options."""
    base_path = options.get("path")
    if not base_path:
        raise ValueError("Option 'path' is required: base directory containing MDF4 files")

    files_option = options.get("files", "")
    if files_option.strip():
        filenames = [f.strip() for f in files_option.split(",") if f.strip()]
        file_paths = [_resolve_file_entry(base_path, f) for f in filenames]
    else:
        file_paths = _discover_mf4_files(base_path)

    if not file_paths:
        raise ValueError(f"No MDF4 files found in '{base_path}'")

    return file_paths


class MdfSignalsDataSource(DataSource):
    """
    Custom PySpark data source that reads MDF4 signal data.

    Produces rows of (file_uri, channel_id, time, value).

    Options (in addition to path/files/target_partition_mb):
      time_dtype, value_dtype: 'float64' (default) or 'float32'. float32 halves
        the on-disk size of that column — useful when the source precision allows.
      run_length_encoding: 'false' (default) or 'true'. When true, consecutive
        equal samples of a channel are collapsed into a single row covering the
        half-open interval [tstart, tend) over which the value stays constant
        (zero-order hold), and the schema becomes
        (file_uri, channel_id, tstart, tend, value). Each channel ends with a
        zero-width point row (tstart == tend == last timestamp) so the final
        sample is recoverable.
      max_groups_per_partition: int (default 64). Upper bound on how many small
        channel groups are coalesced into one Spark task (caps the scattered
        block reads per task). Raise it to further cut task count for files with
        very many tiny groups; lower it for more parallelism per group.
      absolute_time: 'false' (default) or 'true'. When true, the MDF measurement
        start time (UTC, sub-second precision) is ADDED to every timestamp so the
        time columns are absolute Unix epoch seconds. Because epoch seconds need
        ~31 bits of integer range, the time columns are forced to float64
        regardless of time_dtype (float32 cannot represent epoch seconds usefully).
    """

    @classmethod
    def name(cls):
        """Format string for spark.read.format(...): "mdf_signals"."""
        return "mdf_signals"

    def _absolute_time(self):
        return str(self.options.get("absolute_time", "false")).lower() == "true"

    def schema(self):
        """Output schema: (file_uri, channel_id, time, value) — or the RLE variant
        (file_uri, channel_id, tstart, tend, value) when run_length_encoding=true.
        time/value follow time_dtype/value_dtype; absolute_time forces the time
        columns to double."""
        # Built inline (no module-level helper reference): schema() runs when the
        # data source instance is created server-side, and referencing a
        # module-global function there forces a module import that can fail in
        # that context. Inline imports of pyspark types are always safe.
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            IntegerType,
            DoubleType,
            FloatType,
        )

        absolute = self._absolute_time()

        def _ftype(opt):
            # absolute_time forces only the TIME columns to float64 (epoch seconds
            # need the range); the value column always follows value_dtype, matching
            # what the decoder emits — otherwise schema() and the Arrow batches
            # disagree on `value` and the writer fails (getDouble on a float vector).
            if absolute and opt == "time_dtype":
                return DoubleType()
            return (
                FloatType() if str(self.options.get(opt, "float64")) == "float32" else DoubleType()
            )

        if str(self.options.get("run_length_encoding", "false")).lower() == "true":
            return StructType(
                [
                    StructField("file_uri", StringType(), False),
                    StructField("channel_id", IntegerType(), False),
                    StructField("tstart", _ftype("time_dtype"), False),
                    StructField("tend", _ftype("time_dtype"), False),
                    StructField("value", _ftype("value_dtype"), True),
                ]
            )

        return StructType(
            [
                StructField("file_uri", StringType(), False),
                StructField("channel_id", IntegerType(), False),
                StructField("time", _ftype("time_dtype"), False),
                StructField("value", _ftype("value_dtype"), True),
            ]
        )

    def reader(self, schema):
        return MdfSignalsReader(self.options)


class MdfSignalsReader(DataSourceReader):
    """Batch reader for MDF4 signal data."""

    def __init__(self, options):
        self.options = options

    def partitions(self):
        """
        Plan partitions: scan each file's metadata, then use plan_partitions to
        produce record-range / channel-subset partitions (decoupling parallelism
        from channel count). Returns one InputPartition per spec, with the spec
        JSON carried in InputPartition.value.
        """
        _emit_read_telemetry("mdf_signals")
        import json
        from .mdf4_reader import MDF4Reader
        from .bin_packer import plan_partitions, plan_stripes_for_file

        file_paths = _resolve_file_list(self.options)
        target_partition_mb = float(self.options.get("target_partition_mb", "64"))
        run_length_encoding = (
            str(self.options.get("run_length_encoding", "false")).lower() == "true"
        )
        max_groups_per_partition = int(self.options.get("max_groups_per_partition", "64"))
        absolute_time = str(self.options.get("absolute_time", "false")).lower() == "true"
        # partitioning: "group" (default, per-group/record-range) or "stripe"
        # (Design B byte-offset stripes — reads each file once to build the map).
        partitioning = str(self.options.get("partitioning", "group")).lower()
        stripe_target_mb = float(self.options.get("stripe_target_mb", "128"))
        # Absolute time needs float64 for the time columns.
        time_dtype = "float64" if absolute_time else str(self.options.get("time_dtype", "float64"))
        value_dtype = str(self.options.get("value_dtype", "float64"))

        all_partition_specs = []
        for file_path in file_paths:
            time_offset = 0.0
            if absolute_time:
                start = MDF4Reader(file_path).read_header_start_epoch_seconds()
                if start is None:
                    raise ValueError(
                        f"absolute_time=true but {file_path} has no measurement "
                        f"start time in its HD block"
                    )
                time_offset = start

            if partitioning == "stripe":
                specs = plan_stripes_for_file(
                    file_path,
                    target_partition_mb=target_partition_mb,
                    time_dtype=time_dtype,
                    value_dtype=value_dtype,
                    run_length_encoding=run_length_encoding,
                    time_offset=time_offset,
                    stripe_target_mb=stripe_target_mb,
                )
                all_partition_specs.extend(json.dumps(s) for s in specs)
                continue

            reader = MDF4Reader(file_path)
            organized = reader.scan_channels_organized()
            if not organized["signal_channels"]:
                continue
            specs = plan_partitions(
                file_path,
                organized["master_channels"],
                organized["signal_channels"],
                organized["channel_id_map"],
                target_partition_mb=target_partition_mb,
                time_dtype=time_dtype,
                value_dtype=value_dtype,
                run_length_encoding=run_length_encoding,
                max_groups_per_partition=max_groups_per_partition,
                time_offset=time_offset,
                unsorted_dg_ctx=organized.get("unsorted_dg_ctx"),
            )
            all_partition_specs.extend(json.dumps(s) for s in specs)

        # PySpark's Python DataSource API requires partitions() to return
        # InputPartition instances (not plain dicts); the payload is carried in
        # InputPartition.value and is read back in read().
        if not all_partition_specs:
            return [InputPartition("[]")]

        return [InputPartition(spec_json) for spec_json in all_partition_specs]

    def read(self, partition):
        """Read signal data for one partition (one bin of channels).

        Yields pyarrow.RecordBatch via the SAME shared Arrow conversion core as
        the mapInArrow converter (udf_helpers.convert_spec_to_arrow_batches), so
        the data source inherits the streaming / chunking / constant-array
        optimizations instead of the slower row-by-row path.
        """
        import json
        import logging
        from .udf_helpers import (
            convert_spec_to_arrow_batches,
            convert_stripe_spec_to_arrow_batches,
        )

        spec_json = partition.value
        if spec_json == "[]":
            return

        spec = json.loads(spec_json)
        # Prefer the dtype/flags baked into the spec by plan_partitions (these
        # already reflect the absolute_time float64 override); fall back to
        # options for older specs.
        time_dtype = str(spec.get("time_dtype", self.options.get("time_dtype", "float64")))
        value_dtype = str(spec.get("value_dtype", self.options.get("value_dtype", "float64")))
        run_length_encoding = bool(
            spec.get(
                "run_length_encoding",
                str(self.options.get("run_length_encoding", "false")).lower() == "true",
            )
        )
        prof = {"read": 0, "decode": 0, "arrow": 0, "rows": 0}
        # Stripe spec (Design B) carries "subblocks"; group spec carries "channels".
        decode = (
            convert_stripe_spec_to_arrow_batches
            if "subblocks" in spec
            else convert_spec_to_arrow_batches
        )
        yield from decode(
            spec,
            prof=prof,
            time_dtype=time_dtype,
            value_dtype=value_dtype,
            run_length_encoding=run_length_encoding,
        )
        logging.getLogger("impulse_data_sources.mdf.convert").warning(
            "MDF_PROFILE rows=%d read_ms=%.0f decode_ms=%.0f arrow_ms=%.0f",
            prof["rows"],
            prof["read"] / 1e6,
            prof["decode"] / 1e6,
            prof["arrow"] / 1e6,
        )


class MdfMetadataDataSource(DataSource):
    """
    Custom PySpark data source that reads MDF4 channel metadata.

    Produces rows of (file_uri, channel_id, group_idx, channel_idx, channel_name, unit, header_datetime, md_comment).
    md_comment is the channel's cn_md_comment block (##MD XML header, or ##TX text), or null.
    """

    @classmethod
    def name(cls):
        """Format string for spark.read.format(...): "mdf_metadata"."""
        return "mdf_metadata"

    def schema(self):
        """Output schema: (file_uri, channel_id, group_idx, channel_idx,
        channel_name, unit, header_datetime, md_comment)."""
        return METADATA_SCHEMA

    def reader(self, schema):
        return MdfMetadataReader(self.options)


class MdfMetadataReader(DataSourceReader):
    """Batch reader for MDF4 metadata."""

    def __init__(self, options):
        self.options = options

    def partitions(self):
        """One partition per file for metadata scanning."""
        _emit_read_telemetry("mdf_metadata")
        file_paths = _resolve_file_list(self.options)
        partitions = [InputPartition({"file_path": fp}) for fp in file_paths]
        return partitions if partitions else [InputPartition({"file_path": ""})]

    def read(self, partition):
        """Read channel metadata for one file."""
        from .mdf4_reader import MDF4Reader

        p = partition.value
        file_path = p["file_path"]

        if not file_path:
            return iter([])

        reader = MDF4Reader(file_path)
        organized = reader.scan_channels_organized()
        header_datetime = reader.read_header_datetime()

        rows = []
        for ch_id, ch in enumerate(organized["signal_channels"]):
            rows.append(
                (
                    file_path,
                    ch_id,
                    ch.group_idx,
                    ch.channel_idx,
                    ch.channel_name,
                    ch.unit or "",
                    header_datetime,
                    ch.md_comment or None,
                )
            )

        return iter(rows)


class MdfMastersDataSource(DataSource):
    """
    Custom PySpark data source that reads MDF4 MASTER channel data — the time
    base of each acquisition group, one row per ORIGINAL sample.

    Produces rows of (file_uri, group_idx, timestamp). This is the companion
    to run-length-encoded signals (format 'mdf_signals' with
    run_length_encoding=true): RLE keeps only [tstart, tend] intervals, so the
    original per-sample grid is recovered by joining a group's timestamps here
    against those intervals (assign each interval's value to every group
    timestamp t with tstart <= t < tend; <= tend for the final interval).

    Options:
      path (required), files, target_partition_mb,
      max_groups_per_partition: as for 'mdf_signals'.
      time_dtype: 'float64' (default) or 'float32'. Use the SAME value as the
        signals table so the [tstart, tend] join is exact.
      absolute_time: 'false' (default) or 'true'. Adds the measurement start time
        so timestamps are absolute Unix epoch seconds (UTC). Set the SAME value
        as the signals table so the reverse-RLE join lines up; forces float64.
    """

    @classmethod
    def name(cls):
        """Format string for spark.read.format(...): "mdf_masters"."""
        return "mdf_masters"

    def schema(self):
        """Output schema: (file_uri, group_idx, timestamp) — one row per original
        master sample. timestamp is float (double unless time_dtype=float32, or
        absolute_time which forces double)."""
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            IntegerType,
            DoubleType,
            FloatType,
        )

        absolute = str(self.options.get("absolute_time", "false")).lower() == "true"
        ts_type = (
            DoubleType()
            if absolute or str(self.options.get("time_dtype", "float64")) != "float32"
            else FloatType()
        )
        return StructType(
            [
                StructField("file_uri", StringType(), False),
                StructField("group_idx", IntegerType(), False),
                StructField("timestamp", ts_type, False),
            ]
        )

    def reader(self, schema):
        return MdfMastersReader(self.options)


class MdfMastersReader(DataSourceReader):
    """Batch reader for MDF4 master (time-base) data."""

    def __init__(self, options):
        self.options = options

    def partitions(self):
        """Plan per-group master partitions (record-range split for big groups,
        coalescing for small ones), one InputPartition per spec."""
        _emit_read_telemetry("mdf_masters")
        import json
        from .mdf4_reader import MDF4Reader
        from .bin_packer import plan_master_partitions

        file_paths = _resolve_file_list(self.options)
        target_partition_mb = float(self.options.get("target_partition_mb", "64"))
        max_groups_per_partition = int(self.options.get("max_groups_per_partition", "64"))
        absolute_time = str(self.options.get("absolute_time", "false")).lower() == "true"
        time_dtype = "float64" if absolute_time else str(self.options.get("time_dtype", "float64"))

        all_specs = []
        for file_path in file_paths:
            reader = MDF4Reader(file_path)
            organized = reader.scan_channels_organized()
            if not organized["master_channels"]:
                continue
            time_offset = 0.0
            if absolute_time:
                start = reader.read_header_start_epoch_seconds()
                if start is None:
                    raise ValueError(
                        f"absolute_time=true but {file_path} has no measurement "
                        f"start time in its HD block"
                    )
                time_offset = start
            specs = plan_master_partitions(
                file_path,
                organized["master_channels"],
                target_partition_mb=target_partition_mb,
                time_dtype=time_dtype,
                max_groups_per_partition=max_groups_per_partition,
                time_offset=time_offset,
                unsorted_dg_ctx=organized.get("unsorted_dg_ctx"),
            )
            all_specs.extend(json.dumps(s) for s in specs)

        if not all_specs:
            return [InputPartition("[]")]
        return [InputPartition(spec_json) for spec_json in all_specs]

    def read(self, partition):
        """Yield pyarrow.RecordBatch of (file_uri, group_idx, timestamp) for
        one master spec, via the shared master decode core."""
        import json
        import logging
        from .udf_helpers import convert_master_spec_to_arrow_batches

        spec_json = partition.value
        if spec_json == "[]":
            return

        spec = json.loads(spec_json)
        # Use the dtype baked into the spec (reflects the absolute_time override).
        time_dtype = str(spec.get("time_dtype", self.options.get("time_dtype", "float64")))
        prof = {"read": 0, "decode": 0, "arrow": 0, "rows": 0}
        yield from convert_master_spec_to_arrow_batches(spec, prof=prof, time_dtype=time_dtype)
        logging.getLogger("impulse_data_sources.mdf.convert").warning(
            "MDF_PROFILE(masters) rows=%d read_ms=%.0f decode_ms=%.0f arrow_ms=%.0f",
            prof["rows"],
            prof["read"] / 1e6,
            prof["decode"] / 1e6,
            prof["arrow"] / 1e6,
        )
