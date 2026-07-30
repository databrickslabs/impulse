"""
Main converter module: orchestrates MDF4 -> Delta Lake conversion using PySpark.

Architecture:
1. Driver reads MDF4 metadata (fast, sequential scan of block headers)
2. Identifies master channels for each channel group
3. Bin-packs signal channels into balanced partitions
4. Each Spark task reads its assigned channels directly from the MDF binary
5. Uses mapInArrow for zero-copy vectorized processing
6. Writes to two Delta tables: signals (time series) and metadata (channel info)
"""

import time
from datetime import datetime
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from .mdf4_reader import (
    MDF4Reader,
    ChannelInfo,
    CN_TYPE_MASTER,
    CN_TYPE_VIRTUAL_MASTER,
)
from .bin_packer import plan_partitions
from .schemas import METADATA_SCHEMA

_artifacts_shipped = False


def _ensure_artifacts_shipped(spark: SparkSession):
    """Ship the impulse_ds.mdf package to Spark workers once per session.

    NOTE: this `addArtifact(pyfile=True)` reaches mapInArrow UDF workers (which
    import the package at runtime, by-value serialized) but does NOT reach the
    server's `create_data_source` worker, which deserializes the registered
    custom data-source class BY REFERENCE and therefore must
    `import impulse_ds.mdf` at that point. So the mapInArrow conversion path
    works with only this shipped artifact, while the `mdf_signals`/`mdf_metadata`
    data sources additionally require the package to be importable cluster-side
    (e.g. installed as a cluster library). A warm-up mapInArrow was tried and did
    NOT help — the create_data_source worker is a separate/fresh process.
    """
    global _artifacts_shipped
    if _artifacts_shipped:
        return
    import pathlib
    import zipfile
    import tempfile

    package_dir = pathlib.Path(__file__).parent
    impulse_ds_dir = package_dir.parent
    # Ship as a zip to preserve package structure (import impulse_ds.mdf.*)
    zip_path = pathlib.Path(tempfile.gettempdir()) / "impulse_ds_mdf.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(impulse_ds_dir / "__init__.py", "impulse_ds/__init__.py")
        for py_file in package_dir.glob("*.py"):
            zf.write(py_file, f"impulse_ds/mdf/{py_file.name}")
    spark.addArtifact(str(zip_path), pyfile=True)
    _artifacts_shipped = True


@dataclass
class ConversionResult:
    """Result of an MDF to Delta conversion."""

    file_uri: str
    file_path: str
    num_channels: int
    total_samples: int
    num_partitions: int
    duration_seconds: float
    signals_table: str
    metadata_table: str


class MDFToDeltaConverter:
    """
    Converts MDF4 files to Delta Lake tables using distributed Spark processing.

    Usage:
        converter = MDFToDeltaConverter(spark, signals_table="catalog.schema.signals",
                                        metadata_table="catalog.schema.metadata")
        result = converter.convert("/path/to/file.mf4")
    """

    def __init__(
        self,
        spark: SparkSession,
        signals_table: str,
        metadata_table: str,
        target_partition_mb: float = 64.0,
        time_dtype: str = "float64",
        value_dtype: str = "float64",
        run_length_encoding: bool = False,
        max_groups_per_partition: int = 64,
    ):
        """
        Args:
            spark: Active SparkSession (local cluster or Databricks Connect).
            signals_table: Fully-qualified Delta table for the signal rows
                (file_uri, channel_id, time, value); created with liquid
                clustering on (file_uri, channel_id).
            metadata_table: Fully-qualified Delta table for channel metadata
                (file_uri, channel_id, group_idx, channel_idx, channel_name,
                unit, header_datetime, md_comment); clustered on file_uri.
            target_partition_mb: Target output size per Spark task (drives how
                large channel groups are split into record ranges and how many
                small groups are coalesced). Lower => more, smaller tasks.
            time_dtype / value_dtype: 'float64' (default) or 'float32'. float32
                halves that column's on-disk size when source precision allows.
            run_length_encoding: When True, collapse consecutive equal samples of
                a channel into half-open [tstart, tend) interval rows (plus a
                terminal point row per channel); the signals schema becomes
                (file_uri, channel_id, tstart, tend, value).
            max_groups_per_partition: Upper bound on how many small channel
                groups are coalesced into one task (caps scattered reads/task).
        """
        self.spark = spark
        self.signals_table = signals_table
        self.metadata_table = metadata_table
        self.target_partition_mb = target_partition_mb
        self.time_dtype = time_dtype
        self.value_dtype = value_dtype
        self.run_length_encoding = run_length_encoding
        self.max_groups_per_partition = max_groups_per_partition

    def convert(
        self,
        file_path: str,
        mode: str = "append",
    ) -> ConversionResult:
        """
        Convert a single MDF4 file to Delta Lake.

        Args:
            file_path: Path to the MDF4 file (must be accessible from Spark workers).
                Also used verbatim as the row identifier (file_uri).
            mode: Write mode for Delta table ("append" or "overwrite").

        Returns:
            ConversionResult with statistics about the conversion.
        """
        start_time = time.time()

        # Step 1: Scan metadata on the driver
        reader = MDF4Reader(file_path)
        all_channels = reader.scan_metadata()
        header_datetime = reader.read_header_datetime()

        if not all_channels:
            raise ValueError(f"No channels found in {file_path}")

        # Step 2: Identify master channels per group and build channel map
        master_channels, signal_channels, channel_id_map = self._organize_channels(all_channels)

        # Step 3: Write metadata table
        self._write_metadata(signal_channels, channel_id_map, file_path, header_datetime)

        # Step 4: Plan record-range / channel-subset partitions
        partition_specs = self._plan(
            file_path,
            master_channels,
            signal_channels,
            channel_id_map,
            unsorted_dg_ctx=reader._unsorted_dg_ctx,
        )

        # Step 5: Run distributed conversion
        total_samples = sum(ch.sample_count for ch in signal_channels)
        self._run_spark_conversion(partition_specs, mode)

        duration = time.time() - start_time

        return ConversionResult(
            file_uri=file_path,
            file_path=file_path,
            num_channels=len(signal_channels),
            total_samples=total_samples,
            num_partitions=len(partition_specs),
            duration_seconds=duration,
            signals_table=self.signals_table,
            metadata_table=self.metadata_table,
        )

    def _organize_channels(
        self, channels: list[ChannelInfo]
    ) -> tuple[dict[int, ChannelInfo], list[ChannelInfo], dict[tuple[int, int], int]]:
        """
        Separate master and signal channels, assign channel IDs.

        Returns:
            - master_channels: {group_idx: ChannelInfo} for time channels
            - signal_channels: list of non-master channels
            - channel_id_map: {(group_idx, channel_idx): channel_id}
        """
        master_channels: dict[int, ChannelInfo] = {}
        signal_channels: list[ChannelInfo] = []
        channel_id_map: dict[tuple[int, int], int] = {}

        channel_id = 0
        for ch in channels:
            if ch.channel_type in (CN_TYPE_MASTER, CN_TYPE_VIRTUAL_MASTER):
                master_channels[ch.group_idx] = ch
            else:
                channel_id_map[(ch.group_idx, ch.channel_idx)] = channel_id
                signal_channels.append(ch)
                channel_id += 1

        return master_channels, signal_channels, channel_id_map

    def _metadata_rows(self, signal_channels, channel_id_map, file_uri, header_datetime):
        """Build the metadata-table rows (one dict per signal channel) for one
        file. Shared by convert() and convert_batch_parallel()."""
        return [
            {
                "file_uri": file_uri,
                "channel_id": channel_id_map[(ch.group_idx, ch.channel_idx)],
                "group_idx": ch.group_idx,
                "channel_idx": ch.channel_idx,
                "channel_name": ch.channel_name,
                "unit": ch.unit or "",
                "header_datetime": header_datetime,
                "md_comment": ch.md_comment or None,
            }
            for ch in signal_channels
        ]

    def _plan(
        self,
        file_path,
        master_channels,
        signal_channels,
        channel_id_map,
        unsorted_dg_ctx=None,
    ):
        """Plan partition specs for one file using this converter's settings.
        Shared by convert() and convert_batch_parallel()."""
        return plan_partitions(
            file_path,
            master_channels,
            signal_channels,
            channel_id_map,
            target_partition_mb=self.target_partition_mb,
            time_dtype=self.time_dtype,
            value_dtype=self.value_dtype,
            run_length_encoding=self.run_length_encoding,
            max_groups_per_partition=self.max_groups_per_partition,
            unsorted_dg_ctx=unsorted_dg_ctx,
        )

    def _write_metadata(
        self,
        signal_channels: list[ChannelInfo],
        channel_id_map: dict[tuple[int, int], int],
        file_uri: str,
        header_datetime: datetime | None,
    ):
        """Write channel metadata to the metadata Delta table."""
        metadata_rows = self._metadata_rows(
            signal_channels, channel_id_map, file_uri, header_datetime
        )
        if metadata_rows:
            meta_df = self.spark.createDataFrame(metadata_rows, schema=METADATA_SCHEMA)
            _write_metadata_df(meta_df, self.metadata_table, "append")

    def _run_spark_conversion(self, partition_specs: list[dict], mode: str):
        """
        Execute the distributed conversion using mapInArrow for vectorized processing.

        Creates one partition per bin, each partition reads its assigned channels
        from the MDF file and produces Arrow record batches.
        """
        import json

        # Serialize partition specs as JSON strings - one per partition
        spec_strings = [json.dumps(spec) for spec in partition_specs]

        # Create a seed DataFrame with one row per partition
        seed_df = self.spark.createDataFrame(
            [(i, spec_strings[i]) for i in range(len(spec_strings))],
            schema=StructType(
                [
                    StructField("partition_id", IntegerType(), False),
                    StructField("spec_json", StringType(), False),
                ]
            ),
        ).repartition(len(spec_strings), "partition_id")

        # Use mapInArrow for zero-copy vectorized conversion. The output schema
        # follows the dtype carried in the specs (time/value may be float32).
        # _make_arrow_udf returns a local function that cloudpickle can serialize
        # without requiring the impulse_ds.mdf module on workers
        udf_func = _make_arrow_udf()
        td = partition_specs[0].get("time_dtype", "float64") if partition_specs else "float64"
        vd = partition_specs[0].get("value_dtype", "float64") if partition_specs else "float64"
        rle = (
            bool(partition_specs[0].get("run_length_encoding", False))
            if partition_specs
            else False
        )
        result_df = seed_df.mapInArrow(udf_func, schema=_signals_spark_schema(td, vd, rle))

        # Write to Delta (plain write; see _write_signals_df for storage findings)
        _write_signals_df(result_df, self.signals_table, mode)

    def convert_batch(
        self,
        file_paths: list[str],
        mode: str = "append",
    ) -> list[ConversionResult]:
        """
        Convert multiple MDF4 files in sequence; each row is tagged with its
        source file path (file_uri).

        Args:
            file_paths: List of paths to MDF4 files.
            mode: Write mode ("append" or "overwrite"). First file uses given mode,
                  subsequent files always append.

        Returns:
            List of ConversionResult for each file.
        """
        results = []
        for i, path in enumerate(file_paths):
            current_mode = mode if i == 0 else "append"
            result = self.convert(path, current_mode)
            results.append(result)
        return results

    def convert_batch_parallel(
        self,
        file_paths: list[str],
        mode: str = "overwrite",
    ) -> ConversionResult:
        """
        Convert multiple MDF4 files in a single Spark job.

        Scans all files on the driver, bin-packs channels across all files,
        and submits one large mapInArrow job. More efficient than sequential
        convert() calls when files are numerous but individually small.

        Args:
            file_paths: List of paths to MDF4 files.
            mode: Write mode for the Delta table.

        Returns:
            Aggregate ConversionResult.
        """
        from concurrent.futures import ThreadPoolExecutor

        start_time = time.time()
        all_partition_specs = []
        all_metadata_rows = []
        total_channels = 0
        total_samples = 0

        # Scan all files' metadata in parallel (item 5a). Scanning walks block
        # headers and is I/O-bound; CPython releases the GIL during file reads,
        # so threads overlap the per-file latency. Spec building stays sequential
        # afterward to keep output ordering deterministic by input order.
        def _scan(idx_path):
            idx, fp = idx_path
            reader = MDF4Reader(fp)
            channels = reader.scan_metadata()
            return (
                idx,
                fp,
                channels,
                reader.read_header_datetime(),
                reader._unsorted_dg_ctx,
            )

        max_workers = min(16, max(1, len(file_paths)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            scanned = sorted(
                ex.map(_scan, enumerate(file_paths)),
                key=lambda r: r[0],
            )

        for _i, file_path, all_channels_in_file, header_datetime, unsorted_ctx in scanned:
            if not all_channels_in_file:
                continue

            master_channels, signal_channels, channel_id_map = self._organize_channels(
                all_channels_in_file
            )

            all_metadata_rows.extend(
                self._metadata_rows(signal_channels, channel_id_map, file_path, header_datetime)
            )
            all_partition_specs.extend(
                self._plan(
                    file_path,
                    master_channels,
                    signal_channels,
                    channel_id_map,
                    unsorted_dg_ctx=unsorted_ctx,
                )
            )

            total_channels += len(signal_channels)
            total_samples += sum(ch.sample_count for ch in signal_channels)

        # Write all metadata at once
        if all_metadata_rows:
            meta_df = self.spark.createDataFrame(all_metadata_rows, schema=METADATA_SCHEMA)
            _write_metadata_df(meta_df, self.metadata_table, mode)

        # Run all partitions in a single Spark job
        if all_partition_specs:
            self._run_spark_conversion(all_partition_specs, mode)

        duration = time.time() - start_time

        return ConversionResult(
            file_uri=f"{len(file_paths)} files",
            file_path=f"{len(file_paths)} files",
            num_channels=total_channels,
            total_samples=total_samples,
            num_partitions=len(all_partition_specs),
            duration_seconds=duration,
            signals_table=self.signals_table,
            metadata_table=self.metadata_table,
        )


def _make_arrow_udf():
    """Return the Arrow UDF with __module__ set so cloudpickle serializes it inline."""
    func = _convert_partition_arrow
    # Prevent cloudpickle from trying to import impulse_ds.mdf on workers
    func.__module__ = "__main__"
    func.__qualname__ = "_convert_partition_arrow"
    return func


def _schema_to_ddl(schema) -> str:
    """Render a Spark StructType as a CREATE TABLE column list (e.g.
    'file_uri string, channel_id int, ...'). simpleString() yields the SQL
    type name for each field."""
    return ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in schema.fields)


def _ensure_clustered_table(spark, table: str, schema, cluster_cols: str):
    """
    Ensure `table` exists as a Delta table with liquid clustering on
    `cluster_cols` BEFORE data is written, so the write itself is
    clustering-aware (clustering-on-write).

    - CREATE TABLE IF NOT EXISTS ... CLUSTER BY: creates a fresh table already
      configured for liquid clustering.
    - ALTER TABLE ... CLUSTER BY: idempotently enforces the clustering columns
      on a pre-existing (e.g. legacy, non-clustered) unpartitioned table.
    """
    ddl = _schema_to_ddl(schema)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) " f"USING DELTA CLUSTER BY ({cluster_cols})"
    )
    try:
        spark.sql(f"ALTER TABLE {table} CLUSTER BY ({cluster_cols})")
    except Exception:
        # No-op when already clustered on these columns; best-effort enforcement.
        pass


def _write_metadata_df(df, table: str, mode: str):
    """Write the channel-metadata DataFrame to its Delta table with liquid
    clustering on file_uri (item: clustering). Schema is unchanged."""
    _ensure_clustered_table(df.sparkSession, table, METADATA_SCHEMA, "file_uri")
    df.write.format("delta").mode(mode).saveAsTable(table)


def _write_signals_df(df, table: str, mode: str):
    """
    Write the signals DataFrame to the Delta signals table. Preserves the fixed
    (file_uri, channel_id, time, value) schema exactly.

    History / measured findings (kept here so the dead-ends aren't re-explored):
      - Item 1a (sortWithinPartitions + ZSTD) gave NO storage win and added write
        CPU: the heavy columns are float64 and Parquet has no delta/RLE encoding
        for DOUBLE, so sorting `time` enables no better encoding. Reverted.
      - BYTE_STREAM_SPLIT prototype (on real extracted columns): it helps `time`
        (~-11% with zstd, monotonic high-cardinality doubles) but is catastrophic
        for `value` (+73%) because `value` is low-cardinality and default
        DICTIONARY encoding already crushes it. The win only exists *per-column*
        (BSS on `time`, dictionary on `value` → ~-23% vs the snappy default), but
        Spark/Delta expose no per-column Parquet encoding control, so it is not
        wireable through the standard Delta write. Not applied.
      - ZSTD looked promising locally (~-14% vs pyarrow's snappy default), but on
        the Databricks Delta writer the codec is NOT controllable: verified that
        compression.codec = uncompressed / snappy / zstd all produce BYTE-IDENTICAL
        output (the runtime forces its own codec, and the data is already ~10:1
        compressed). Neither the DataFrameWriter ".option('compression', ...)" nor
        the session conf "spark.sql.parquet.compression.codec" has any effect.

    Conclusion: under the fixed schema + Databricks Delta write there is no
    achievable codec lever, so this is a plain write. The table is created with
    liquid clustering on (file_uri, channel_id) so data is organized for
    pruning on those keys. Keeps a best-effort delta.targetFileSize hint (item
    1c) for downstream OPTIMIZE.

    The clustered table is created from df.schema (not the fixed SIGNALS_SCHEMA)
    so a float32 time/value DataFrame produces a matching float table.
    """
    _ensure_clustered_table(df.sparkSession, table, df.schema, "file_uri, channel_id")
    df.write.format("delta").mode(mode).saveAsTable(table)
    try:
        df.sparkSession.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES " f"('delta.targetFileSize' = '128mb')"
        )
    except Exception:
        # Property hint is best-effort; never fail the conversion over it.
        pass


def _signals_spark_schema(
    time_dtype: str = "float64", value_dtype: str = "float64", run_length_encoding: bool = False
):
    """Spark StructType for the signals output; time/value follow the configured
    dtype (float32 halves their on-disk bytes). With run_length_encoding the
    per-sample `time` column is replaced by the [`tstart`, `tend`] interval."""
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        DoubleType,
        FloatType,
    )

    def _ft(dt):
        return FloatType() if str(dt) == "float32" else DoubleType()

    if run_length_encoding:
        return StructType(
            [
                StructField("file_uri", StringType(), False),
                StructField("channel_id", IntegerType(), False),
                StructField("tstart", _ft(time_dtype), False),
                StructField("tend", _ft(time_dtype), False),
                StructField("value", _ft(value_dtype), True),
            ]
        )

    return StructType(
        [
            StructField("file_uri", StringType(), False),
            StructField("channel_id", IntegerType(), False),
            StructField("time", _ft(time_dtype), False),
            StructField("value", _ft(value_dtype), True),
        ]
    )


def _convert_partition_arrow(batch_iter):
    """mapInArrow UDF: one seed row per partition carries a spec JSON; delegate
    each spec to the shared Arrow conversion core so the mapInArrow path and the
    'mdf_signals' data source share identical streaming/decoding logic. The
    time/value dtype is read from the spec (float32 or float64)."""
    import json
    import logging
    import pyarrow as pa
    from .udf_helpers import (
        convert_spec_to_arrow_batches,
        convert_stripe_spec_to_arrow_batches,
        signals_arrow_schema,
    )

    log = logging.getLogger("impulse_ds.mdf.convert")
    prof = {"read": 0, "decode": 0, "arrow": 0, "rows": 0}

    for batch in batch_iter:
        spec_jsons = batch.column("spec_json").to_pylist()
        # dtype is uniform per job; read it from the first spec for the empty
        # fallback batch so it matches the mapInArrow-declared schema.
        td, vd, rle = "float64", "float64", False
        if spec_jsons:
            _first = json.loads(spec_jsons[0])
            td = _first.get("time_dtype", "float64")
            vd = _first.get("value_dtype", "float64")
            rle = bool(_first.get("run_length_encoding", False))
        output_schema = signals_arrow_schema(td, vd, rle)
        yielded = False
        for spec_json in spec_jsons:
            spec = json.loads(spec_json)
            # A stripe spec (Design B, byte-offset) carries "subblocks"; a group
            # spec (default) carries "channels". Dispatch to the matching decoder.
            decode = (
                convert_stripe_spec_to_arrow_batches
                if "subblocks" in spec
                else convert_spec_to_arrow_batches
            )
            for rb in decode(
                spec,
                prof=prof,
                time_dtype=spec.get("time_dtype", "float64"),
                value_dtype=spec.get("value_dtype", "float64"),
                run_length_encoding=bool(spec.get("run_length_encoding", False)),
            ):
                yielded = True
                yield rb

        if not yielded:
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array([], type=output_schema.field(i).type)
                    for i in range(len(output_schema))
                ],
                schema=output_schema,
            )

    # Per-task phase breakdown (item 0). WARNING so it surfaces in executor logs
    # without raising the default level; it is a measurement line, not an error.
    log.warning(
        "MDF_PROFILE rows=%d read_ms=%.0f decode_ms=%.0f arrow_ms=%.0f",
        prof["rows"],
        prof["read"] / 1e6,
        prof["decode"] / 1e6,
        prof["arrow"] / 1e6,
    )
