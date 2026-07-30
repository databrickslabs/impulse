"""
Decode raw MDF4 record bytes into float64 value / timestamp arrays: data-type
interpretation, CC (channel conversion) scaling, and invalidation-bit handling.
Vectorised with numpy; no file I/O.
"""

import struct

import numpy as np

# MDF4 data type constants
FLOAT_LE = 4
FLOAT_BE = 5
UINT_LE = 0
UINT_BE = 1
SINT_LE = 2
SINT_BE = 3

# CC conversion types
CC_IDENTITY = 0
CC_LINEAR = 1
CC_RATIONAL = 2
CC_ALGEBRAIC = 3
CC_TAB_INTERP = 4
CC_TAB_NOINTERP = 5
CC_RANGE_TO_VALUE = 6

# Invalidation flags
CN_FLAG_ALL_INVALID = 1
CN_FLAG_INVALIDATION_PRESENT = 1 << 1


def convert_values(raw_bytes, data_type, bit_count, bit_offset, sample_count):
    """Convert raw channel bytes to float64 array based on MDF4 data type."""
    value_bytes = (bit_count + 7) // 8
    if data_type == FLOAT_LE:
        if bit_count == 64:
            return raw_bytes.view(np.float64).reshape(sample_count).copy()
        elif bit_count == 32:
            return raw_bytes.view(np.float32).reshape(sample_count).astype(np.float64)
        return np.zeros(sample_count, dtype=np.float64)
    elif data_type == FLOAT_BE:
        if bit_count == 64:
            return raw_bytes[:, ::-1].copy().view(np.float64).reshape(sample_count).copy()
        elif bit_count == 32:
            return (
                raw_bytes[:, ::-1].copy().view(np.float32).reshape(sample_count).astype(np.float64)
            )
        return np.zeros(sample_count, dtype=np.float64)
    elif data_type in (UINT_LE, UINT_BE):
        is_be = data_type == UINT_BE
        if bit_count <= 8:
            vals = raw_bytes[:, 0].astype(np.uint64)
        elif bit_count <= 16:
            if value_bytes == 2 and not is_be:
                vals = raw_bytes.view(np.uint16).reshape(sample_count).astype(np.uint64)
            else:
                padded = np.zeros((sample_count, 2), dtype=np.uint8)
                padded[:, :value_bytes] = raw_bytes[:, :value_bytes]
                if is_be:
                    padded = padded[:, ::-1].copy()
                vals = padded.view(np.uint16).reshape(sample_count).astype(np.uint64)
        elif bit_count <= 32:
            if value_bytes == 4 and not is_be:
                vals = raw_bytes.view(np.uint32).reshape(sample_count).astype(np.uint64)
            else:
                padded = np.zeros((sample_count, 4), dtype=np.uint8)
                padded[:, :value_bytes] = raw_bytes[:, :value_bytes]
                if is_be:
                    padded = padded[:, ::-1].copy()
                vals = padded.view(np.uint32).reshape(sample_count).astype(np.uint64)
        else:
            if value_bytes == 8 and not is_be:
                vals = raw_bytes.view(np.uint64).reshape(sample_count)
            else:
                padded = np.zeros((sample_count, 8), dtype=np.uint8)
                padded[:, :value_bytes] = raw_bytes[:, :value_bytes]
                if is_be:
                    padded = padded[:, ::-1].copy()
                vals = padded.view(np.uint64).reshape(sample_count)
        if bit_offset > 0:
            vals = vals >> bit_offset
        if bit_count < 64:
            vals = vals & ((1 << bit_count) - 1)
        return vals.astype(np.float64)
    elif data_type in (SINT_LE, SINT_BE):
        is_be = data_type == SINT_BE
        if bit_count <= 8:
            vals = raw_bytes[:, 0].astype(np.uint64)
        elif bit_count <= 16:
            if value_bytes == 2 and not is_be:
                vals = raw_bytes.view(np.uint16).reshape(sample_count).astype(np.uint64)
            else:
                padded = np.zeros((sample_count, 2), dtype=np.uint8)
                padded[:, :value_bytes] = raw_bytes[:, :value_bytes]
                if is_be:
                    padded = padded[:, ::-1].copy()
                vals = padded.view(np.uint16).reshape(sample_count).astype(np.uint64)
        elif bit_count <= 32:
            if value_bytes == 4 and not is_be:
                vals = raw_bytes.view(np.uint32).reshape(sample_count).astype(np.uint64)
            else:
                padded = np.zeros((sample_count, 4), dtype=np.uint8)
                padded[:, :value_bytes] = raw_bytes[:, :value_bytes]
                if is_be:
                    padded = padded[:, ::-1].copy()
                vals = padded.view(np.uint32).reshape(sample_count).astype(np.uint64)
        else:
            if value_bytes == 8 and not is_be:
                vals = raw_bytes.view(np.uint64).reshape(sample_count)
            else:
                padded = np.zeros((sample_count, 8), dtype=np.uint8)
                padded[:, :value_bytes] = raw_bytes[:, :value_bytes]
                if is_be:
                    padded = padded[:, ::-1].copy()
                vals = padded.view(np.uint64).reshape(sample_count)
        if bit_offset > 0:
            vals = vals >> bit_offset
        vals = vals & ((1 << bit_count) - 1)
        sign_bit = np.uint64(1 << (bit_count - 1))
        vals = np.where(
            vals & sign_bit, vals.astype(np.int64) - (1 << bit_count), vals.astype(np.int64)
        )
        return vals.astype(np.float64)
    else:
        return np.zeros(sample_count, dtype=np.float64)


def extract_column_strided(raw_data, record_size, byte_offset, value_bytes, sample_count):
    """Extract column bytes using strided access (avoids full 2D reshape)."""
    buf = np.frombuffer(raw_data, dtype=np.uint8)
    return np.lib.stride_tricks.as_strided(
        buf[byte_offset:],
        shape=(sample_count, value_bytes),
        strides=(record_size, 1),
    ).copy()


def apply_invalidation(buf, record_size, actual_samples, values, signal_info):
    """Apply invalidation bits to values array, setting invalid samples to NaN.

    buf: np.ndarray (uint8) view of the raw data, or raw bytes.
    """
    invalidation_bytes_nr = signal_info.get("invalidation_bytes", 0)
    cn_flags = signal_info.get("cn_flags", 0)
    if invalidation_bytes_nr <= 0:
        return values
    if not (cn_flags & (CN_FLAG_ALL_INVALID | CN_FLAG_INVALIDATION_PRESENT)):
        return values
    if cn_flags & CN_FLAG_ALL_INVALID:
        values[:] = np.nan
        return values
    data_bytes_nr = signal_info.get("data_bytes", 0)
    inv_bit_pos = signal_info.get("invalidation_bit_pos", 0)
    byte_pos = inv_bit_pos // 8
    bit_pos = inv_bit_pos % 8
    inval_col = data_bytes_nr + byte_pos
    if not isinstance(buf, np.ndarray):
        buf = np.frombuffer(buf, dtype=np.uint8)
    inval_bytes = np.lib.stride_tricks.as_strided(
        buf[inval_col:],
        shape=(actual_samples,),
        strides=(record_size,),
    ).copy()
    invalid_mask = (inval_bytes & (1 << bit_pos)).astype(bool)
    values[invalid_mask] = np.nan
    return values


def apply_cc_conversion(values, cc_type, cc_params):
    """
    Apply MDF4 CC (Channel Conversion) block scaling to raw values.
    Modifies values in-place where possible to reduce allocations.
    """
    if cc_type < 0 or cc_type == CC_IDENTITY or not cc_params:
        return values
    if cc_type == CC_LINEAR:
        b, a = cc_params[0], cc_params[1]
        if a != 1.0:
            values *= a
        if b != 0.0:
            values += b
        return values
    if cc_type == CC_RATIONAL:
        P1, P2, P3, P4, P5, P6 = cc_params[:6]
        X = values
        num = P1 * X * X + P2 * X + P3
        den = P4 * X * X + P5 * X + P6
        with np.errstate(divide="ignore", invalid="ignore"):
            np.divide(num, den, out=values)
        return values
    if cc_type == CC_TAB_INTERP:
        raw_tab = np.array(cc_params[0::2])
        phys_tab = np.array(cc_params[1::2])
        return np.interp(values, raw_tab, phys_tab)
    if cc_type == CC_TAB_NOINTERP:
        n = len(cc_params) // 2
        raw_tab = np.array(cc_params[0::2])
        phys_tab = np.array(cc_params[1::2])
        inds = np.searchsorted(raw_tab, values)
        inds = np.clip(inds, 0, n - 1)
        inds2 = np.clip(inds - 1, 0, n - 1)
        cond = np.abs(values - raw_tab[inds]) >= np.abs(values - raw_tab[inds2])
        return np.where(cond, phys_tab[inds2], phys_tab[inds])
    if cc_type == CC_RANGE_TO_VALUE:
        n = (len(cc_params) - 1) // 3
        default = cc_params[3 * n] if len(cc_params) > 3 * n else np.nan
        if n <= 0:
            return np.full_like(values, default)
        # Vectorized: build sorted lower-bound edges and use searchsorted
        lowers = np.array([cc_params[i * 3] for i in range(n)])
        uppers = np.array([cc_params[i * 3 + 1] for i in range(n)])
        phys_vals = np.array([cc_params[i * 3 + 2] for i in range(n)])
        # Find which range each value falls into
        indices = np.searchsorted(lowers, values, side="right") - 1
        indices = np.clip(indices, 0, n - 1)
        result = np.where(
            (values >= lowers[indices]) & (values < uppers[indices]),
            phys_vals[indices],
            default,
        )
        return result
    return values


_RECORD_ID_FMT = {1: "<B", 2: "<H", 4: "<I", 8: "<Q"}


def storage_record_id(record_id: int, rec_id_size: int) -> int:
    """Return the record ID as stored in the leading bytes of each unsorted record."""
    if rec_id_size <= 0:
        return 0
    if rec_id_size >= 8:
        return record_id
    mask = (1 << (8 * rec_id_size)) - 1
    return record_id & mask


def read_record_id(buf: bytes | memoryview, offset: int, rec_id_size: int) -> int:
    """Read a little-endian unsigned record ID from ``buf`` at ``offset``."""
    if rec_id_size <= 0:
        return 0
    fmt = _RECORD_ID_FMT.get(rec_id_size)
    if fmt is None:
        raise ValueError(f"Unsupported rec_id_size: {rec_id_size}")
    return struct.unpack_from(fmt, buf, offset)[0]


def filter_unsorted_records(
    raw_data: bytes,
    rec_id_size: int,
    target_record_id: int,
    cg_record_sizes: dict[int, int],
) -> bytes:
    """Extract records belonging to ``target_record_id`` from an interleaved block."""
    if rec_id_size <= 0:
        return raw_data
    target_record_id = storage_record_id(target_record_id, rec_id_size)
    out = bytearray()
    pos = 0
    n = len(raw_data)
    while pos < n:
        if pos + rec_id_size > n:
            break
        rid = read_record_id(raw_data, pos, rec_id_size)
        rec_size = cg_record_sizes.get(rid)
        if rec_size is None:
            break
        if pos + rec_size > n:
            break
        if rid == target_record_id:
            out.extend(raw_data[pos : pos + rec_size])
        pos += rec_size
    return bytes(out)


def prepare_cg_records(
    raw_data: bytes,
    *,
    record_size: int,
    rec_id_size: int = 0,
    record_id: int = 0,
    cg_record_sizes: dict[int, int] | None = None,
    row_start: int | None = None,
    row_end: int | None = None,
) -> tuple[bytes, int]:
    """Filter unsorted interleaved records and optionally slice by logical row range.

    Returns ``(prepared_bytes, index_offset)`` where ``index_offset`` is the
    logical sample index of the first record (for virtual masters).
    """
    if rec_id_size > 0:
        if not cg_record_sizes:
            return b"", 0
        # JSON partition specs stringify dict keys; normalize back to int.
        cg_sizes = {int(k): v for k, v in cg_record_sizes.items()}
        raw_data = filter_unsorted_records(
            raw_data,
            rec_id_size,
            record_id,
            cg_sizes,
        )

    if record_size <= 0:
        return b"", 0

    actual = len(raw_data) // record_size
    if actual == 0:
        return b"", 0

    lo = 0 if row_start is None else max(0, min(row_start, actual))
    hi = actual if row_end is None else max(lo, min(row_end, actual))
    if hi <= lo:
        return b"", lo

    start = lo * record_size
    end = hi * record_size
    return raw_data[start:end], lo


def unsorted_fields_from_ctx(dg_block_addr: int, record_id: int, unsorted_dg_ctx: dict) -> dict:
    """Build unsorted read kwargs from scan-time DG context and channel record_id."""
    ctx = unsorted_dg_ctx.get(dg_block_addr) if unsorted_dg_ctx else None
    if not ctx or ctx.get("rec_id_size", 0) == 0:
        return {"rec_id_size": 0, "record_id": 0, "cg_record_sizes": None}
    rec_id_size = ctx["rec_id_size"]
    return {
        "rec_id_size": rec_id_size,
        "record_id": storage_record_id(record_id, rec_id_size),
        "cg_record_sizes": ctx["cg_sizes"],
    }


def extract_signal(raw_data, record_size, ch_spec):
    """Extract and convert a signal channel from raw data."""
    actual = len(raw_data) // record_size
    if actual == 0:
        return None
    if ch_spec.get("channel_type") == 1:  # VLSD
        return np.full(actual, np.nan, dtype=np.float64)

    usable = raw_data[: actual * record_size]
    s_data_type = ch_spec["data_type"]
    s_bit_count = ch_spec["bit_count"]
    s_byte_offset = ch_spec["byte_offset"]
    s_bit_offset = ch_spec["bit_offset"]
    s_bytes = (s_bit_count + 7) // 8

    buf = np.frombuffer(usable, dtype=np.uint8)

    # Fast paths: direct strided view for common aligned types (no column copy)
    if s_bit_offset == 0:
        if s_data_type == FLOAT_LE and s_bit_count == 64 and s_byte_offset % 8 == 0:
            values = np.ndarray(
                shape=(actual,),
                dtype=np.float64,
                buffer=buf,
                offset=s_byte_offset,
                strides=(record_size,),
            ).copy()
        elif s_data_type == FLOAT_LE and s_bit_count == 32 and s_byte_offset % 4 == 0:
            values = np.ndarray(
                shape=(actual,),
                dtype=np.float32,
                buffer=buf,
                offset=s_byte_offset,
                strides=(record_size,),
            ).astype(np.float64)
        elif s_data_type == UINT_LE and s_bit_count == 16 and s_byte_offset % 2 == 0:
            values = np.ndarray(
                shape=(actual,),
                dtype=np.uint16,
                buffer=buf,
                offset=s_byte_offset,
                strides=(record_size,),
            ).astype(np.float64)
        elif s_data_type == UINT_LE and s_bit_count == 32 and s_byte_offset % 4 == 0:
            values = np.ndarray(
                shape=(actual,),
                dtype=np.uint32,
                buffer=buf,
                offset=s_byte_offset,
                strides=(record_size,),
            ).astype(np.float64)
        elif s_data_type == UINT_LE and s_bit_count == 8:
            values = np.ndarray(
                shape=(actual,),
                dtype=np.uint8,
                buffer=buf,
                offset=s_byte_offset,
                strides=(record_size,),
            ).astype(np.float64)
        elif s_data_type == SINT_LE and s_bit_count == 16 and s_byte_offset % 2 == 0:
            values = np.ndarray(
                shape=(actual,),
                dtype=np.int16,
                buffer=buf,
                offset=s_byte_offset,
                strides=(record_size,),
            ).astype(np.float64)
        elif s_data_type == SINT_LE and s_bit_count == 32 and s_byte_offset % 4 == 0:
            values = np.ndarray(
                shape=(actual,),
                dtype=np.int32,
                buffer=buf,
                offset=s_byte_offset,
                strides=(record_size,),
            ).astype(np.float64)
        elif s_data_type == SINT_LE and s_bit_count == 8:
            values = np.ndarray(
                shape=(actual,),
                dtype=np.int8,
                buffer=buf,
                offset=s_byte_offset,
                strides=(record_size,),
            ).astype(np.float64)
        elif s_data_type == UINT_LE and s_bit_count == 64 and s_byte_offset % 8 == 0:
            values = np.ndarray(
                shape=(actual,),
                dtype=np.uint64,
                buffer=buf,
                offset=s_byte_offset,
                strides=(record_size,),
            ).astype(np.float64)
        elif s_data_type == SINT_LE and s_bit_count == 64 and s_byte_offset % 8 == 0:
            values = np.ndarray(
                shape=(actual,),
                dtype=np.int64,
                buffer=buf,
                offset=s_byte_offset,
                strides=(record_size,),
            ).astype(np.float64)
        else:
            sig_raw = extract_column_strided(usable, record_size, s_byte_offset, s_bytes, actual)
            values = convert_values(sig_raw, s_data_type, s_bit_count, s_bit_offset, actual)
    else:
        sig_raw = extract_column_strided(usable, record_size, s_byte_offset, s_bytes, actual)
        values = convert_values(sig_raw, s_data_type, s_bit_count, s_bit_offset, actual)

    # Apply invalidation — pass buf to avoid redundant np.frombuffer
    cn_flags = ch_spec.get("cn_flags", 0)
    invalidation_bytes_nr = ch_spec.get("invalidation_bytes", 0)
    if invalidation_bytes_nr > 0 and (
        cn_flags & (CN_FLAG_ALL_INVALID | CN_FLAG_INVALIDATION_PRESENT)
    ):
        signal_info = {
            "cn_flags": cn_flags,
            "invalidation_bit_pos": ch_spec.get("invalidation_bit_pos", 0),
            "invalidation_bytes": invalidation_bytes_nr,
            "data_bytes": ch_spec.get("data_bytes", 0),
        }
        values = apply_invalidation(buf, record_size, actual, values, signal_info)

    # Apply CC conversion if present
    cc_type = ch_spec.get("cc_type", -1)
    cc_params = ch_spec.get("cc_params")
    if cc_type > 0 and cc_params:
        values = apply_cc_conversion(values, cc_type, cc_params)

    return values


def extract_timestamps(raw_data, record_size, master_info, index_offset=0):
    """Extract timestamps from raw data given master channel info.

    index_offset shifts the implicit sample index for virtual masters. It must
    be set to the absolute index of the first record when raw_data is a chunk of
    a larger block, so that virtual-master timestamps stay globally continuous.
    """
    actual = len(raw_data) // record_size
    if actual == 0:
        return np.array([], dtype=np.float64)
    if master_info["channel_type"] == 3:  # virtual master
        timestamps = np.arange(index_offset, index_offset + actual, dtype=np.float64)
    else:
        usable = raw_data[: actual * record_size]
        m_bit_count = master_info["bit_count"]
        m_byte_offset = master_info["byte_offset"]
        m_bit_offset = master_info["bit_offset"]
        m_data_type = master_info["data_type"]
        m_bytes = (m_bit_count + 7) // 8

        buf = np.frombuffer(usable, dtype=np.uint8)

        if (
            m_bit_offset == 0
            and m_data_type == FLOAT_LE
            and m_bit_count == 64
            and m_byte_offset % 8 == 0
        ):
            timestamps = np.ndarray(
                shape=(actual,),
                dtype=np.float64,
                buffer=buf,
                offset=m_byte_offset,
                strides=(record_size,),
            ).copy()
        elif (
            m_bit_offset == 0
            and m_data_type == FLOAT_LE
            and m_bit_count == 32
            and m_byte_offset % 4 == 0
        ):
            timestamps = np.ndarray(
                shape=(actual,),
                dtype=np.float32,
                buffer=buf,
                offset=m_byte_offset,
                strides=(record_size,),
            ).astype(np.float64)
        elif (
            m_bit_offset == 0
            and m_data_type == UINT_LE
            and m_bit_count == 64
            and m_byte_offset % 8 == 0
        ):
            timestamps = np.ndarray(
                shape=(actual,),
                dtype=np.uint64,
                buffer=buf,
                offset=m_byte_offset,
                strides=(record_size,),
            ).astype(np.float64)
        else:
            master_raw = extract_column_strided(
                usable, record_size, m_byte_offset, m_bytes, actual
            )
            timestamps = convert_values(master_raw, m_data_type, m_bit_count, m_bit_offset, actual)

    # Apply CC conversion to master channel if present
    cc_type = master_info.get("cc_type", -1)
    cc_params = master_info.get("cc_params")
    if cc_type > 0 and cc_params:
        timestamps = apply_cc_conversion(timestamps, cc_type, cc_params)

    return timestamps
