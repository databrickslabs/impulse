"""
Low-level MDF4 data-block I/O: read and decompress ``##DT`` (uncompressed),
``##DZ`` (zlib, possibly transposed), and ``##DL``/``##HL`` (data lists) blocks,
and stream record ranges. Pure binary helpers (struct/zlib/numpy) shared by the
Arrow emitters; they operate on an open binary file object (real file or an
in-memory ``BytesIO``).
"""
import struct
import zlib
import numpy as np


def _read_dz_header(f, dz_addr):
    """Read DZ block header fields, return (zip_type, zip_parameter, org_size, data_length)."""
    f.seek(dz_addr + 24)  # skip id(4) + reserved(4) + length(8) + link_count(8)
    f.read(2)  # org_block_type
    dz_zip_type = struct.unpack("<B", f.read(1))[0]
    f.read(1)  # reserved
    dz_zip_parameter = struct.unpack("<I", f.read(4))[0]
    dz_org_size = struct.unpack("<Q", f.read(8))[0]
    dz_data_length = struct.unpack("<Q", f.read(8))[0]
    return dz_zip_type, dz_zip_parameter, dz_org_size, dz_data_length


def decompress_dz(f, dz_addr):
    """Decompress a single DZ block, returning raw bytes."""
    dz_zip_type, dz_zip_parameter, dz_org_size, dz_data_length = _read_dz_header(f, dz_addr)
    compressed = f.read(dz_data_length)
    decompressed = zlib.decompress(compressed)
    if dz_zip_type == 1:
        cols = dz_zip_parameter
        rows = len(decompressed) // cols
        remainder = len(decompressed) % cols
        if remainder == 0:
            arr = np.frombuffer(decompressed, dtype=np.uint8).reshape(cols, rows)
            decompressed = arr.T.tobytes()
        else:
            # Source is stored column-major with unequal column lengths: the
            # first `remainder` columns hold (rows+1) bytes, the rest hold
            # `rows`. Build destination (row-major) indices per column and
            # scatter in one pass instead of looping over every byte.
            src = np.frombuffer(decompressed, dtype=np.uint8)
            dest_idx = np.empty(len(src), dtype=np.int64)
            pos = 0
            for col in range(cols):
                col_size = rows + (1 if col < remainder else 0)
                dest_idx[pos:pos + col_size] = np.arange(col_size) * cols + col
                pos += col_size
            out = np.empty(len(src), dtype=np.uint8)
            out[dest_idx] = src
            decompressed = out.tobytes()
    return decompressed


def _collect_dl_block_addrs(f, dl_addr):
    """
    Walk the DL chain and collect all data block addresses.
    Returns list of (block_addr, block_type) tuples where block_type is the 4-byte ID.
    """
    addrs = []
    addr = dl_addr
    seen = set()
    while addr != 0:
        if addr in seen:
            break  # guard against a malformed/cyclic DL chain
        seen.add(addr)
        f.seek(addr)
        block_id = f.read(4)
        if block_id != b"##DL":
            break
        f.read(4)  # reserved
        block_len = struct.unpack("<Q", f.read(8))[0]
        link_count = struct.unpack("<Q", f.read(8))[0]
        links = struct.unpack(f"<{link_count}Q", f.read(8 * link_count))
        next_dl = links[0]
        f.read(1)  # flags
        f.read(3)  # reserved
        dl_count = struct.unpack("<I", f.read(4))[0]
        for i in range(1, min(link_count, dl_count + 1)):
            data_addr = links[i]
            if data_addr != 0:
                addrs.append(data_addr)
        addr = next_dl
    return addrs


def resolve_dl_addr(f, data_block_addr):
    """Return the ##DL chain address for a DL or HL block, else None.

    A ##HL (header list) block points to the first ##DL of the chain."""
    f.seek(data_block_addr)
    bid = f.read(4)
    if bid == b"##DL":
        return data_block_addr
    if bid == b"##HL":
        f.read(4)   # reserved
        f.read(8)   # length
        f.read(8)   # link_count
        hl_dl_first = struct.unpack("<Q", f.read(8))[0]
        return hl_dl_first or None
    return None


def _read_dl_blob(f, dl_addr):
    """Read a ##DL chain's entire data region in ONE sequential read (plus the
    cheap chain walk), returning (blob, span_start, sub_addrs).

    DL sub-blocks are written contiguously, so a single large read over
    [first_addr, last_addr + last_len] replaces the hundreds of scattered
    per-sub-block reads that dominate cost on FUSE/cloud volumes (latency-bound).
    Sub-blocks are then parsed/decompressed from the in-memory blob with no
    further I/O. The block 'length' field (offset 8) is the on-disk size for both
    ##DT and ##DZ, so only the last sub-block's 16-byte header is needed to bound
    the span.
    """
    addrs = _collect_dl_block_addrs(f, dl_addr)
    if not addrs:
        return b"", 0, []
    lo = min(addrs)
    hi = max(addrs)
    f.seek(hi)
    hdr = f.read(16)
    if len(hdr) < 16:
        return b"", 0, []
    last_len = struct.unpack_from("<Q", hdr, 8)[0]
    f.seek(lo)
    blob = f.read(hi + last_len - lo)
    return blob, lo, addrs


def _subblock_uncompressed_len(blob, off):
    """Uncompressed byte length of the sub-block at blob[off:] (no decompression)."""
    if blob[off:off + 4] == b"##DZ":
        return struct.unpack_from("<Q", blob, off + 32)[0]      # dz_org_size
    return struct.unpack_from("<Q", blob, off + 8)[0] - 24       # ##DT length - header


def _decompress_subblock_blob(blob, off):
    """Decompress (##DZ) or copy (##DT) the sub-block at blob[off:], from memory."""
    if blob[off:off + 4] == b"##DZ":
        zip_type = blob[off + 26]
        zip_param = struct.unpack_from("<I", blob, off + 28)[0]
        data_len = struct.unpack_from("<Q", blob, off + 40)[0]
        dec = zlib.decompress(bytes(blob[off + 48:off + 48 + data_len]))
        if zip_type == 1:  # transposed deflate -> un-transpose
            cols = zip_param
            rows = len(dec) // cols
            remainder = len(dec) % cols
            if remainder == 0:
                dec = np.frombuffer(dec, dtype=np.uint8).reshape(cols, rows).T.tobytes()
            else:
                src = np.frombuffer(dec, dtype=np.uint8)
                dest_idx = np.empty(len(src), dtype=np.int64)
                pos = 0
                for col in range(cols):
                    cs = rows + (1 if col < remainder else 0)
                    dest_idx[pos:pos + cs] = np.arange(cs) * cols + col
                    pos += cs
                out = np.empty(len(src), dtype=np.uint8)
                out[dest_idx] = src
                dec = out.tobytes()
        return dec
    length = struct.unpack_from("<Q", blob, off + 8)[0]
    return bytes(blob[off + 24:off + 24 + (length - 24)])


def _read_subblock_file(f, addr):
    """Fallback: read one sub-block directly from file (used only if the coalesced
    blob doesn't cover it, e.g. a non-contiguous layout)."""
    f.seek(addr)
    if f.read(4) == b"##DZ":
        return decompress_dz(f, addr)
    f.seek(addr + 8)
    length = struct.unpack("<Q", f.read(8))[0]
    f.seek(addr + 24)
    return f.read(length - 24)


def _dl_subblock_meta(f, dl_addr):
    """Coalesced read + parse: returns (blob, span_start, meta) where meta is a
    list of (addr, off, uncompressed_len, covered). 'covered' is False only for
    the rare sub-block the single blob read didn't span (then read from file)."""
    blob, lo, addrs = _read_dl_blob(f, dl_addr)
    blen = len(blob)
    meta = []
    for addr in addrs:
        off = addr - lo
        covered = 0 <= off and off + 24 <= blen
        if covered:
            ondisk = struct.unpack_from("<Q", blob, off + 8)[0]
            covered = off + ondisk <= blen
        ulen = _subblock_uncompressed_len(blob, off) if covered else len(_read_subblock_file(f, addr))
        meta.append((addr, off, ulen, covered))
    return blob, lo, meta


def read_data_list_raw(f, dl_addr):
    """Read all data from a ##DL chain: one coalesced read, in-memory decompress."""
    blob, lo, meta = _dl_subblock_meta(f, dl_addr)
    if not meta:
        return b""
    result = bytearray(sum(m[2] for m in meta))
    pos = 0
    for addr, off, ulen, covered in meta:
        chunk = _decompress_subblock_blob(blob, off) if covered else _read_subblock_file(f, addr)
        result[pos:pos + len(chunk)] = chunk
        pos += len(chunk)
    return bytes(result)


def read_data_list_range(f, dl_addr, record_size, row_start, row_end):
    """Read records [row_start, row_end) from a ##DL chain. One coalesced read of
    the region, then decompress only the sub-blocks overlapping the range.

    Returns (raw_bytes, actual_first_record). Reads more compressed bytes than a
    strict scattered read, but in ONE sequential read instead of many latency-
    bound round-trips (the bottleneck on cloud volumes).
    """
    blob, lo, meta = _dl_subblock_meta(f, dl_addr)
    if not meta:
        return b"", row_start
    bounds = []
    rec = 0
    for addr, off, ulen, covered in meta:
        n = ulen // record_size if record_size else 0
        bounds.append((addr, off, rec, rec + n, covered))
        rec += n
    total = rec
    row_start = max(0, min(row_start, total))
    row_end = max(row_start, min(row_end, total))
    parts = []
    first = None
    for addr, off, rs, re, covered in bounds:
        if re <= row_start or rs >= row_end:
            continue
        if first is None:
            first = rs
        parts.append(_decompress_subblock_blob(blob, off) if covered else _read_subblock_file(f, addr))
    if first is None:
        return b"", row_start
    raw = b"".join(parts)
    start_off = (row_start - first) * record_size
    end_off = (row_end - first) * record_size
    return raw[start_off:end_off], row_start


def read_raw_data(f, data_block_addr, record_size, sample_count):
    """Read raw record data from any block type (DT, DL, DZ, HL) using an open handle."""
    f.seek(data_block_addr)
    block_id = f.read(4)
    if block_id == b"##DT":
        f.read(4)
        dt_block_len = struct.unpack("<Q", f.read(8))[0]
        f.read(8)
        return f.read(dt_block_len - 24)
    elif block_id == b"##DL":
        return read_data_list_raw(f, data_block_addr)
    elif block_id == b"##DZ":
        return decompress_dz(f, data_block_addr)
    elif block_id == b"##HL":
        f.read(4)  # reserved
        f.read(8)  # length
        f.read(8)  # link_count
        hl_dl_first = struct.unpack("<Q", f.read(8))[0]
        if hl_dl_first == 0:
            return b""
        return read_data_list_raw(f, hl_dl_first)
    else:
        f.seek(data_block_addr + 24)
        return f.read(record_size * sample_count)


def dt_data_extent(f, data_block_addr):
    """Return (data_start, data_size) for a plain uncompressed ##DT block.

    Returns None for any other block type (##DL, ##DZ, ##HL, ...), signalling
    that the caller must fall back to read_raw_data() for whole-block handling.
    A plain DT block stores records contiguously, so it can be streamed in
    record-aligned chunks without decompression or link traversal.
    """
    f.seek(data_block_addr)
    block_id = f.read(4)
    if block_id != b"##DT":
        return None
    f.read(4)  # reserved
    dt_block_len = struct.unpack("<Q", f.read(8))[0]
    f.read(8)  # link count (0 for DT)
    return data_block_addr + 24, dt_block_len - 24


def _read_block_chunks(f, data_block_addr, record_size, sample_count,
                       row_start, row_end, prof, log):
    """Yield (raw_bytes, start_record) for records [row_start, row_end) of one
    data block, using the SAME read strategy as the signals core: stream an
    uncompressed ##DT in record-aligned chunks, read only the overlapping
    sub-blocks of a ##DL/##HL range, else read the whole block (##DZ/unknown) and
    slice. row_start/row_end None => the whole block. Shared by the master decoder
    so it inherits the streaming/coalescing behaviour without duplicating it."""
    import time
    _now = time.perf_counter_ns
    _CHUNK_BYTES = 256 * 1024 * 1024

    try:
        extent = dt_data_extent(f, data_block_addr)
    except Exception as e:
        log.warning("DT extent probe failed at %d: %s", data_block_addr, e)
        extent = None

    if extent is not None:
        data_start, data_size = extent
        total = data_size // record_size if record_size else 0
        if total == 0:
            return
        lo = 0 if row_start is None else max(0, min(row_start, total))
        hi = total if row_end is None else max(lo, min(row_end, total))
        chunk_records = max(1, _CHUNK_BYTES // record_size)
        for rec0 in range(lo, hi, chunk_records):
            recN = min(rec0 + chunk_records, hi)
            nrec = recN - rec0
            t0 = _now()
            f.seek(data_start + rec0 * record_size)
            raw = f.read(nrec * record_size)
            prof["read"] += _now() - t0
            if len(raw) // record_size:
                yield raw, rec0
        return

    if row_start is not None:
        dl_addr = resolve_dl_addr(f, data_block_addr)
        if dl_addr is not None:
            re_hi = row_end if row_end is not None else (1 << 62)
            t0 = _now()
            raw, start = read_data_list_range(f, dl_addr, record_size, row_start, re_hi)
            prof["read"] += _now() - t0
            if len(raw) // record_size:
                yield raw, start
            return

    t0 = _now()
    raw = read_raw_data(f, data_block_addr, record_size, sample_count)
    prof["read"] += _now() - t0
    total = len(raw) // record_size if record_size else 0
    if total == 0:
        return
    if row_start is not None:
        lo = max(0, min(row_start, total))
        hi = total if row_end is None else max(lo, min(row_end, total))
        raw = raw[lo * record_size:hi * record_size]
        start = lo
    else:
        start = 0
    if len(raw) // record_size:
        yield raw, start


def parse_subblocks(f, data_block_addr, record_size, sample_count):
    """Return the data sub-blocks of ONE group as a list of
    (abs_offset, on_disk_len, rec_start, rec_count). Used to build the block map
    during planning; `f` is expected to be the in-RAM whole-file buffer so the
    per-sub-block header reads are free (no scattered disk IO). A standalone
    ##DT/##DZ is a single sub-block; a ##DL/##HL chain yields one entry per
    referenced data block.
    """
    f.seek(data_block_addr)
    bid = f.read(4)
    if bid in (b"##DT", b"##DZ"):
        f.seek(data_block_addr + 8)
        length = struct.unpack("<Q", f.read(8))[0]
        return [(data_block_addr, length, 0, sample_count)]
    if bid in (b"##DL", b"##HL"):
        dl = resolve_dl_addr(f, data_block_addr)
        if dl is None:
            return []
        sub_addrs = _collect_dl_block_addrs(f, dl)
        subs = []
        rec_start = 0
        for sa in sub_addrs:
            f.seek(sa)
            sid = f.read(4)
            f.read(4)
            length = struct.unpack("<Q", f.read(8))[0]
            if sid == b"##DZ":
                f.seek(sa + 32)
                ulen = struct.unpack("<Q", f.read(8))[0]   # dz_org_size (uncompressed)
            else:  # ##DT
                ulen = length - 24
            rc = ulen // record_size if record_size else 0
            subs.append((sa, length, rec_start, rc))
            rec_start += rc
        return subs
    # Unknown block: treat as a single opaque sub-block (decoder falls back).
    f.seek(data_block_addr + 8)
    length = struct.unpack("<Q", f.read(8))[0]
    return [(data_block_addr, length, 0, sample_count)]
