"""
Stable import surface for the executor-side MDF4 decode helpers.

The implementation is split across three focused modules:
  - mdf_blocks  : low-level ``##DT``/``##DZ``/``##DL`` block I/O
  - mdf_decode  : raw bytes -> value / timestamp arrays (+ CC, invalidation)
  - arrow_emit  : build Arrow batches (per-group, stripe, master) + RLE

This module re-exports their public names so existing imports
(``from .udf_helpers import convert_spec_to_arrow_batches``)
and the by-value mapInArrow UDFs keep working unchanged.
"""

from .mdf_blocks import (  # noqa: F401
    decompress_dz,
    resolve_dl_addr,
    read_data_list_raw,
    read_data_list_range,
    read_raw_data,
    dt_data_extent,
    parse_subblocks,
    _collect_dl_block_addrs,
    _decompress_subblock_blob,
    _read_block_chunks,
)
from .mdf_decode import (  # noqa: F401
    convert_values,
    extract_column_strided,
    apply_invalidation,
    apply_cc_conversion,
    extract_signal,
    extract_timestamps,
    read_record_id,
    storage_record_id,
    filter_unsorted_records,
    prepare_cg_records,
    unsorted_fields_from_ctx,
    FLOAT_LE,
    FLOAT_BE,
    UINT_LE,
    UINT_BE,
    SINT_LE,
    SINT_BE,
    CC_IDENTITY,
    CC_LINEAR,
    CC_RATIONAL,
    CC_ALGEBRAIC,
    CC_TAB_INTERP,
    CC_TAB_NOINTERP,
    CC_RANGE_TO_VALUE,
    CN_FLAG_ALL_INVALID,
    CN_FLAG_INVALIDATION_PRESENT,
)
from .arrow_emit import (  # noqa: F401
    signals_arrow_schema,
    master_arrow_schema,
    convert_spec_to_arrow_batches,
    convert_master_spec_to_arrow_batches,
    convert_stripe_spec_to_arrow_batches,
    _rle_run_starts,
    _rle_compress_chunk,
    _rle_flush,
)
