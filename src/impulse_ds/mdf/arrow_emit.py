"""
Build PyArrow RecordBatches from MDF4 partition specs — the executor-side decode
core shared by both the mapInArrow converter and the custom data sources.

Three entry points emit the public output schemas:
  - convert_spec_to_arrow_batches        -> signals (per channel group)
  - convert_stripe_spec_to_arrow_batches -> signals (byte-offset stripe)
  - convert_master_spec_to_arrow_batches -> per-group master time base

plus run-length encoding (collapse constant runs into [tstart, tend) intervals).
"""
import numpy as np

from .mdf_blocks import (
    dt_data_extent, resolve_dl_addr, read_data_list_range, read_raw_data,
    _decompress_subblock_blob, _read_block_chunks,
)
from .mdf_decode import extract_signal, extract_timestamps, prepare_cg_records


def _pa_float(dtype):
    import pyarrow as pa
    return pa.float32() if str(dtype) == "float32" else pa.float64()


def _np_float(dtype):
    return np.float32 if str(dtype) == "float32" else np.float64


def _unsorted_kwargs(ch_spec):
    return {
        "rec_id_size": ch_spec.get("rec_id_size", 0),
        "record_id": ch_spec.get("record_id", 0),
        "cg_record_sizes": ch_spec.get("cg_record_sizes"),
    }


def _emit_prepared_signal_group(
    raw_data,
    group_channels,
    record_size,
    master_info,
    time_offset,
    emit_fn,
    prof,
    log,
    data_block_addr,
    _now,
    index_offset=0,
):
    """Decode one prepared (filtered/sliced) raw block for all channels in a CG."""
    actual = len(raw_data) // record_size if record_size else 0
    if actual == 0:
        return
    t0 = _now()
    if master_info is not None:
        timestamps = extract_timestamps(
            raw_data, record_size, master_info, index_offset=index_offset,
        )
    else:
        timestamps = np.arange(index_offset, index_offset + actual, dtype=np.float64)
    if time_offset:
        timestamps = timestamps + time_offset
    prof["decode"] += _now() - t0
    for ch_spec in group_channels:
        try:
            t0 = _now()
            values = extract_signal(raw_data, record_size, ch_spec)
            prof["decode"] += _now() - t0
            if values is None:
                continue
            yield from emit_fn(timestamps, values, ch_spec["channel_id"])
        except Exception as e:
            log.warning(
                "extract failed ch=%s block=%d: %s",
                ch_spec.get("channel_id"), data_block_addr, e,
            )


def _eq_nan(a, b):
    """Value equality for run-length encoding, treating NaN == NaN as equal so
    consecutive invalid samples collapse into a single run."""
    return a == b or (a != a and b != b)


def _rle_run_starts(vs):
    """Indices at which a new run begins in `vs` (value differs from predecessor;
    NaN is considered equal to NaN). Always includes index 0."""
    n = len(vs)
    if n <= 1:
        return np.zeros(n, dtype=np.int64)
    neq = vs[1:] != vs[:-1]
    nan_both = np.isnan(vs[1:]) & np.isnan(vs[:-1])
    change = neq & ~nan_both
    return np.concatenate(([0], np.nonzero(change)[0] + 1)).astype(np.int64)


def _rle_compress_chunk(ts, vs, carry):
    """Run-length-encode one time-ordered (ts, vs) chunk for a SINGLE channel,
    merging with the trailing run carried over from the previous chunk.

    Uses a zero-order hold: a run holding value v from ts[s] ends when the value
    next changes, so its tend is the start time of the following run. The final
    run of the whole channel stays open (its tend is only known once a later
    sample arrives, or, at flush, falls back to the last sample's own time).

    Returns (closed, new_carry):
      closed     = (tstart_arr, tend_arr, value_arr) of fully-determined runs,
                   or None if this chunk produced no closed runs yet.
      new_carry  = [value, tstart, last_ts] describing the still-open trailing
                   run (fed back in on the next chunk, or flushed at the end).
    """
    n = len(vs)
    if n == 0:
        return None, carry
    starts = _rle_run_starts(vs)
    run_vals = vs[starts]
    run_t0 = ts[starts]
    m = len(starts)
    last_ts = ts[n - 1]

    seg_t0, seg_t1, seg_v = [], [], []
    first_t0 = run_t0[0]
    if carry is not None:
        c_val, c_t0, _c_last = carry
        if _eq_nan(c_val, run_vals[0]):
            # The open run continues into this chunk: keep its original start.
            first_t0 = c_t0
        else:
            # Value changed at this chunk's first sample: close the carried run
            # there (zero-order hold to the change point).
            seg_t0.append(np.array([c_t0])); seg_t1.append(np.array([ts[0]]))
            seg_v.append(np.array([c_val]))

    if m >= 2:
        t0 = run_t0[:m - 1].copy()
        t0[0] = first_t0
        seg_t0.append(t0)
        seg_t1.append(run_t0[1:m])      # tend = start of the next run
        seg_v.append(run_vals[:m - 1])
        new_carry = [run_vals[m - 1], run_t0[m - 1], last_ts]
    else:
        # Whole chunk is a single run; it remains open.
        new_carry = [run_vals[0], first_t0, last_ts]

    if seg_v:
        return (np.concatenate(seg_t0), np.concatenate(seg_t1),
                np.concatenate(seg_v)), new_carry
    return None, new_carry


def _rle_flush(carry):
    """Close the final open run. Runs are half-open [tstart, tend); the final
    sample sits exactly on the trailing boundary, so it is emitted as an extra
    zero-width POINT row with tstart == tend == last timestamp. This guarantees
    every original sample (including the last) is recoverable — a consumer
    re-expanding with `t >= tstart AND t < tend` also accepts a point row via
    `tstart == tend AND t == tstart`.

    Returns (tstart_arr, tend_arr, value_arr) or None:
      - multi-sample final run -> [c_t0, c_last) held interval + [c_last, c_last] point
      - single-sample final run -> just the [c_last, c_last] point (already a point)
    """
    if carry is None:
        return None
    c_val, c_t0, c_last = carry
    if c_t0 < c_last:
        return (np.array([c_t0, c_last]), np.array([c_last, c_last]),
                np.array([c_val, c_val]))
    return np.array([c_last]), np.array([c_last]), np.array([c_val])


def signals_arrow_schema(time_dtype="float64", value_dtype="float64",
                         run_length_encoding=False):
    """Arrow schema for emitted signal batches. time/value default to float64 but
    can be float32 (halves their on-disk bytes) per the data-source options.
    value is nullable to accept NaN/None invalid samples.

    With run_length_encoding the per-sample `time` column is replaced by the
    [`tstart`, `tend`) half-open interval over which the (run-length-collapsed)
    value holds. Each channel also ends with a zero-width point row
    (tstart == tend == last timestamp) so the final sample is recoverable.
    """
    import pyarrow as pa
    if run_length_encoding:
        return pa.schema([
            pa.field("file_uri", pa.string(), nullable=False),
            pa.field("channel_id", pa.int32(), nullable=False),
            pa.field("tstart", _pa_float(time_dtype), nullable=False),
            pa.field("tend", _pa_float(time_dtype), nullable=False),
            pa.field("value", _pa_float(value_dtype), nullable=True),
        ])
    return pa.schema([
        pa.field("file_uri", pa.string(), nullable=False),
        pa.field("channel_id", pa.int32(), nullable=False),
        pa.field("time", _pa_float(time_dtype), nullable=False),
        pa.field("value", _pa_float(value_dtype), nullable=True),
    ])


def master_arrow_schema(time_dtype="float64"):
    """Arrow schema for the master time-base output: one row per ORIGINAL sample
    of each group's master channel (file_uri, group_idx, timestamp)."""
    import pyarrow as pa
    return pa.schema([
        pa.field("file_uri", pa.string(), nullable=False),
        pa.field("group_idx", pa.int32(), nullable=False),
        pa.field("timestamp", _pa_float(time_dtype), nullable=False),
    ])


def _make_signal_emitters(file_uri, output_schema, pa_time, pa_value,
                          np_time, np_value, run_length_encoding, prof,
                          max_batch_rows=2_000_000):
    """Build the ``(emit_fn, flush_fn)`` pair shared by the per-group and
    stripe signal converters, so both inherit identical batching/RLE behaviour.

      emit_fn(timestamps, values, channel_id) -> iterator[pyarrow.RecordBatch]
      flush_fn()                              -> iterator[pyarrow.RecordBatch]

    Both close over a cached, full-width ``file_uri`` constant column (the
    source path repeated for every row — the row identifier — built once and
    sliced per batch), the float dtypes, and the ``prof`` accumulator, and cap
    each output batch at ``max_batch_rows`` rows.

    Without RLE, emit_fn yields per-sample (file_uri, channel_id, time, value)
    batches and flush_fn is a no-op. With RLE, emit_fn collapses consecutive
    equal samples into [tstart, tend] interval rows (zero-order hold), carrying
    one open trailing run per channel across the internal chunks of a partition
    /stripe; flush_fn drains those trailing runs (incl. the terminal
    zero-width point row per channel)."""
    import time
    import pyarrow as pa
    _now = time.perf_counter_ns

    uri_cache = []

    def _uri_const():
        if not uri_cache:
            uri_cache.append(
                pa.array(np.full(max_batch_rows, file_uri, dtype=object), type=pa.string()))
        return uri_cache[0]

    def _emit_points(timestamps, values, channel_id):
        n = len(values)
        if n == 0:
            return
        cont_full = _uri_const()
        t0 = _now()
        chan_full = pa.array(np.full(min(n, max_batch_rows), channel_id, dtype=np.int32),
                             type=pa.int32())
        prof["arrow"] += _now() - t0
        for offset in range(0, n, max_batch_rows):
            end = min(offset + max_batch_rows, n)
            clen = end - offset
            t1 = _now()
            ts = timestamps[offset:end]
            vs = values[offset:end]
            if np_time is np.float32:
                ts = ts.astype(np.float32)
            if np_value is np.float32:
                vs = vs.astype(np.float32)
            rb = pa.RecordBatch.from_arrays(
                [cont_full.slice(0, clen), chan_full.slice(0, clen),
                 pa.array(ts, type=pa_time), pa.array(vs, type=pa_value)],
                schema=output_schema)
            prof["arrow"] += _now() - t1
            prof["rows"] += clen
            yield rb

    if not run_length_encoding:
        return _emit_points, (lambda: iter(()))

    rle_state = {}  # channel_id -> open trailing run carried between chunks

    def _emit_rle_batch(t0arr, t1arr, varr, channel_id):
        n = len(varr)
        if n == 0:
            return
        cont_full = _uri_const()
        for offset in range(0, n, max_batch_rows):
            end = min(offset + max_batch_rows, n)
            clen = end - offset
            _ta = _now()
            t0 = t0arr[offset:end]; te = t1arr[offset:end]; vv = varr[offset:end]
            if np_time is np.float32:
                t0 = t0.astype(np.float32); te = te.astype(np.float32)
            if np_value is np.float32:
                vv = vv.astype(np.float32)
            chan_full = pa.array(np.full(clen, channel_id, dtype=np.int32), type=pa.int32())
            rb = pa.RecordBatch.from_arrays(
                [cont_full.slice(0, clen), chan_full,
                 pa.array(t0, type=pa_time), pa.array(te, type=pa_time),
                 pa.array(vv, type=pa_value)],
                schema=output_schema)
            prof["arrow"] += _now() - _ta
            prof["rows"] += clen
            yield rb

    def emit_fn(timestamps, values, channel_id):
        closed, new_carry = _rle_compress_chunk(timestamps, values, rle_state.get(channel_id))
        rle_state[channel_id] = new_carry
        if closed is not None:
            yield from _emit_rle_batch(closed[0], closed[1], closed[2], channel_id)

    def flush_fn():
        for channel_id, carry in rle_state.items():
            out = _rle_flush(carry)
            if out is not None:
                yield from _emit_rle_batch(out[0], out[1], out[2], channel_id)
        rle_state.clear()

    return emit_fn, flush_fn


def convert_spec_to_arrow_batches(spec, prof=None, time_dtype="float64", value_dtype="float64",
                                  run_length_encoding=False):
    """
    Convert ONE partition spec into an iterator of pyarrow.RecordBatch with the
    signals schema (file_uri, channel_id, time, value).

    This is the single shared Arrow conversion core used by BOTH the mapInArrow
    UDF (converter._convert_partition_arrow) and the 'mdf_signals' custom data
    source (datasources.MdfSignalsReader.read), so both inherit the same
    optimizations: stream uncompressed DT blocks in record-aligned chunks to
    bound memory, fall back to whole-block reads for DL/DZ/HL, reuse a cached
    file_uri constant column, and cap output batches at _MAX_BATCH_ROWS rows.

    spec: dict with keys file_path, channels (list of channel specs).
    prof: optional mutable dict accumulating {read, decode, arrow, rows} nanoseconds.

    run_length_encoding: when True, collapse consecutive equal samples of a
    channel into one row spanning [tstart, tend] (the interval over which the
    value stays constant, zero-order hold), emitting the
    (file_uri, channel_id, tstart, tend, value) schema instead. Runs are
    merged across the internal read/output chunks of a partition; note that
    because the planner may split one channel group into several record-range
    partitions, runs that straddle a partition boundary are not merged across
    partitions (at most one extra row per boundary per channel).
    """
    import time
    import logging

    log = logging.getLogger("impulse_ds.mdf.convert")
    _now = time.perf_counter_ns
    if prof is None:
        prof = {"read": 0, "decode": 0, "arrow": 0, "rows": 0}

    output_schema = signals_arrow_schema(time_dtype, value_dtype, run_length_encoding)
    pa_time, pa_value = _pa_float(time_dtype), _pa_float(value_dtype)
    np_time, np_value = _np_float(time_dtype), _np_float(value_dtype)

    # Coarse read chunk for I/O efficiency (bounds memory for huge DT blocks);
    # output batch sizing is independent (see _make_signal_emitters) so a small
    # record_size cannot produce a huge single batch.
    _CHUNK_BYTES = 256 * 1024 * 1024

    file_path = spec["file_path"]
    file_uri = file_path
    channels_spec = spec["channels"]
    # Optional record-range slice (set by the planner for deep DT groups so a
    # single channel group is processed by many parallel tasks). Applies to the
    # DT fast path only; all channels in such a spec belong to one DT block.
    spec_row_start = spec.get("row_start")
    spec_row_end = spec.get("row_end")

    # Absolute-time offset (epoch seconds, UTC) added to every timestamp when the
    # planner baked one in (absolute_time option). 0.0 => relative times unchanged.
    time_offset = float(spec.get("time_offset", 0.0))

    # Emission dispatch (shared with the stripe converter): per-sample batches,
    # or run-length-encoded interval rows. flush_fn() drains any state held back
    # between read chunks (only RLE carries a trailing open run).
    emit_fn, flush_fn = _make_signal_emitters(
        file_uri, output_schema, pa_time, pa_value, np_time, np_value,
        run_length_encoding, prof)

    block_groups = {}
    for ch_spec in channels_spec:
        if ch_spec["sample_count"] == 0:
            continue
        block_groups.setdefault(ch_spec["group_idx"], []).append(ch_spec)

    with open(file_path, "rb") as f:
        for _group_idx, group_channels in block_groups.items():
            ch0 = group_channels[0]
            data_block_addr = ch0["data_block_addr"]
            record_size = ch0["record_size"]
            sample_count = ch0["sample_count"]
            master_info = ch0.get("master_info")
            rec_id_size = ch0.get("rec_id_size", 0)
            us = _unsorted_kwargs(ch0)

            if rec_id_size > 0:
                try:
                    t0 = _now()
                    raw_data = read_raw_data(f, data_block_addr, record_size, sample_count)
                    prof["read"] += _now() - t0
                except Exception as e:
                    log.warning("read_raw_data failed at %d in %s: %s",
                                data_block_addr, file_path, e)
                    continue
                raw_data, index_offset = prepare_cg_records(
                    raw_data,
                    record_size=record_size,
                    row_start=spec_row_start,
                    row_end=spec_row_end,
                    **us,
                )
                yield from _emit_prepared_signal_group(
                    raw_data, group_channels, record_size, master_info,
                    time_offset, emit_fn, prof, log, data_block_addr, _now,
                    index_offset=index_offset,
                )
                continue

            try:
                extent = dt_data_extent(f, data_block_addr)
            except Exception as e:
                log.warning("DT extent probe failed at %d in %s: %s",
                            data_block_addr, file_path, e)
                extent = None

            if extent is not None:
                # Fast path: stream the contiguous DT block in record-aligned chunks.
                data_start, data_size = extent
                total_records = data_size // record_size if record_size else 0
                if total_records == 0:
                    continue
                # Honor the planner's record-range slice (defaults to whole block).
                lo = 0 if spec_row_start is None else max(0, min(spec_row_start, total_records))
                hi = total_records if spec_row_end is None else max(lo, min(spec_row_end, total_records))
                if hi <= lo:
                    continue
                chunk_records = max(1, _CHUNK_BYTES // record_size)
                for rec0 in range(lo, hi, chunk_records):
                    recN = min(rec0 + chunk_records, hi)
                    nrec = recN - rec0
                    t0 = _now()
                    f.seek(data_start + rec0 * record_size)
                    raw_chunk = f.read(nrec * record_size)
                    prof["read"] += _now() - t0
                    actual = len(raw_chunk) // record_size
                    if actual == 0:
                        continue
                    t0 = _now()
                    if master_info is not None:
                        timestamps = extract_timestamps(
                            raw_chunk, record_size, master_info, index_offset=rec0,
                        )
                    else:
                        timestamps = np.arange(rec0, rec0 + actual, dtype=np.float64)
                    if time_offset:
                        timestamps = timestamps + time_offset
                    prof["decode"] += _now() - t0
                    for ch_spec in group_channels:
                        try:
                            t0 = _now()
                            values = extract_signal(raw_chunk, record_size, ch_spec)
                            prof["decode"] += _now() - t0
                            if values is None:
                                continue
                            yield from emit_fn(
                                timestamps, values, ch_spec["channel_id"],
                            )
                        except Exception as e:
                            log.warning("extract failed ch=%s block=%d: %s",
                                        ch_spec.get("channel_id"), data_block_addr, e)
                            continue
                continue

            # ##DL/##HL with a planner record-range slice: read only the
            # sub-blocks overlapping [row_start, row_end). This lets many tasks
            # split one compressed channel group with no redundant decompression.
            if spec_row_start is not None:
                dl_addr = resolve_dl_addr(f, data_block_addr)
                if dl_addr is not None:
                    re_hi = spec_row_end if spec_row_end is not None else (1 << 62)
                    t0 = _now()
                    raw_data, start_rec = read_data_list_range(
                        f, dl_addr, record_size, spec_row_start, re_hi,
                    )
                    prof["read"] += _now() - t0
                    actual = len(raw_data) // record_size if record_size else 0
                    if actual == 0:
                        continue
                    t0 = _now()
                    if master_info is not None:
                        timestamps = extract_timestamps(
                            raw_data, record_size, master_info, index_offset=start_rec,
                        )
                    else:
                        timestamps = np.arange(start_rec, start_rec + actual, dtype=np.float64)
                    if time_offset:
                        timestamps = timestamps + time_offset
                    prof["decode"] += _now() - t0
                    for ch_spec in group_channels:
                        try:
                            t0 = _now()
                            values = extract_signal(raw_data, record_size, ch_spec)
                            prof["decode"] += _now() - t0
                            if values is None:
                                continue
                            yield from emit_fn(
                                timestamps, values, ch_spec["channel_id"],
                            )
                        except Exception as e:
                            log.warning("extract failed ch=%s block=%d: %s",
                                        ch_spec.get("channel_id"), data_block_addr, e)
                            continue
                    continue

            # Fallback: standalone ##DZ / unknown blocks read whole. If the
            # planner assigned this partition a record range (it splits large
            # groups blindly, without peeking block type), slice the whole block
            # to that range so range partitions don't duplicate rows.
            try:
                t0 = _now()
                raw_data = read_raw_data(f, data_block_addr, record_size, sample_count)
                prof["read"] += _now() - t0
            except Exception as e:
                log.warning("read_raw_data failed at %d in %s: %s",
                            data_block_addr, file_path, e)
                continue
            total = len(raw_data) // record_size if record_size else 0
            if total == 0:
                continue
            if spec_row_start is not None:
                lo = max(0, min(spec_row_start, total))
                hi = total if spec_row_end is None else max(lo, min(spec_row_end, total))
                raw_data, start_rec = prepare_cg_records(
                    raw_data,
                    record_size=record_size,
                    row_start=lo,
                    row_end=hi,
                )
            else:
                raw_data, start_rec = prepare_cg_records(
                    raw_data, record_size=record_size,
                )
            yield from _emit_prepared_signal_group(
                raw_data, group_channels, record_size, master_info,
                time_offset, emit_fn, prof, log, data_block_addr, _now,
                index_offset=start_rec,
            )

        # Drain state held across read chunks (RLE keeps one open trailing run
        # per channel; the per-sample path holds nothing, so this is a no-op).
        yield from flush_fn()


def convert_master_spec_to_arrow_batches(spec, prof=None, time_dtype="float64"):
    """Convert ONE master spec into a pyarrow.RecordBatch iterator with schema
    (file_uri, group_idx, timestamp): the ORIGINAL per-sample timestamps of
    each acquisition group's master channel, one row per sample.

    Stored alongside run-length-encoded signals so the original sample grid can
    be reconstructed ("reverse RLE"): every signal in group g held value v over
    [tstart, tend], so re-expanding it means assigning v to each of this table's
    group-g timestamps that fall within that interval.

    spec: dict with keys file_path, time_dtype, masters (list of
    {group_idx, data_block_addr, record_size, sample_count, master_info}) and an
    optional whole-spec row_start/row_end (set only for single-master record-range
    splits of a large group).
    """
    import time
    import logging
    import pyarrow as pa

    log = logging.getLogger("impulse_ds.mdf.convert")
    _now = time.perf_counter_ns
    if prof is None:
        prof = {"read": 0, "decode": 0, "arrow": 0, "rows": 0}

    output_schema = master_arrow_schema(time_dtype)
    pa_time = _pa_float(time_dtype)
    np_time = _np_float(time_dtype)
    _MAX_BATCH_ROWS = 2_000_000

    file_path = spec["file_path"]
    file_uri = file_path
    masters = spec["masters"]
    r0 = spec.get("row_start")
    r1 = spec.get("row_end")
    time_offset = float(spec.get("time_offset", 0.0))

    _uri_full_cache = []

    def _uri_const():
        if not _uri_full_cache:
            _uri_full_cache.append(
                pa.array(np.full(_MAX_BATCH_ROWS, file_uri, dtype=object), type=pa.string()))
        return _uri_full_cache[0]

    def _emit(ts, group_idx):
        n = len(ts)
        if n == 0:
            return
        cont_full = _uri_const()
        for off in range(0, n, _MAX_BATCH_ROWS):
            end = min(off + _MAX_BATCH_ROWS, n)
            clen = end - off
            t0 = _now()
            seg = ts[off:end]
            if np_time is np.float32:
                seg = seg.astype(np.float32)
            grp = pa.array(np.full(clen, group_idx, dtype=np.int32), type=pa.int32())
            rb = pa.RecordBatch.from_arrays(
                [cont_full.slice(0, clen), grp, pa.array(seg, type=pa_time)],
                schema=output_schema,
            )
            prof["arrow"] += _now() - t0
            prof["rows"] += clen
            yield rb

    with open(file_path, "rb") as f:
        for mspec in masters:
            rs = mspec["record_size"]
            if not rs:
                continue
            gid = mspec["group_idx"]
            minfo = mspec["master_info"]
            sc = mspec["sample_count"]

            # Virtual master: timestamps ARE the sample index — no block read.
            if minfo.get("channel_type") == 3:
                lo = 0 if r0 is None else max(0, min(r0, sc))
                hi = sc if r1 is None else max(lo, min(r1, sc))
                if hi > lo:
                    t0 = _now()
                    ts = np.arange(lo, hi, dtype=np.float64)
                    if time_offset:
                        ts = ts + time_offset
                    prof["decode"] += _now() - t0
                    yield from _emit(ts, gid)
                continue

            for raw, start in _read_block_chunks(
                f, mspec["data_block_addr"], rs, sc, r0, r1, prof, log,
                rec_id_size=mspec.get("rec_id_size", 0),
                record_id=mspec.get("record_id", 0),
                cg_record_sizes=mspec.get("cg_record_sizes"),
            ):
                t0 = _now()
                ts = extract_timestamps(raw, rs, minfo, index_offset=start)
                if time_offset:
                    ts = ts + time_offset
                prof["decode"] += _now() - t0
                yield from _emit(ts, gid)


def convert_stripe_spec_to_arrow_batches(spec, prof=None, time_dtype="float64",
                                         value_dtype="float64", run_length_encoding=False):
    """Decode ONE byte-offset stripe (Design B): a contiguous file region holding
    a set of data sub-blocks from possibly several groups. The whole region is read
    in ONE sequential IO, then each sub-block is decompressed + extracted from RAM.

    spec keys: file_path, byte_start, byte_end, time_dtype,
      value_dtype, run_length_encoding, time_offset,
      groups: {str(group_key): {record_size, master_info, channels:[ch_dicts]}},
      subblocks: [{grp, abs_off, on_disk_len, rec_start, rec_count}].

    Emits the SAME schema as convert_spec_to_arrow_batches (signals, or the RLE
    variant), so it is a drop-in alternative read path.
    """
    import time
    import logging

    log = logging.getLogger("impulse_ds.mdf.convert")
    _now = time.perf_counter_ns
    if prof is None:
        prof = {"read": 0, "decode": 0, "arrow": 0, "rows": 0}

    output_schema = signals_arrow_schema(time_dtype, value_dtype, run_length_encoding)
    pa_time, pa_value = _pa_float(time_dtype), _pa_float(value_dtype)
    np_time, np_value = _np_float(time_dtype), _np_float(value_dtype)

    file_path = spec["file_path"]
    file_uri = file_path
    byte_start = spec["byte_start"]
    byte_end = spec["byte_end"]
    groups = spec["groups"]
    subblocks = spec["subblocks"]
    time_offset = float(spec.get("time_offset", 0.0))

    # Same emission dispatch as convert_spec_to_arrow_batches (per-sample or RLE).
    emit_fn, flush_fn = _make_signal_emitters(
        file_uri, output_schema, pa_time, pa_value, np_time, np_value,
        run_length_encoding, prof)

    # One sequential read of the whole stripe.
    t0 = _now()
    with open(file_path, "rb") as f:
        f.seek(byte_start)
        blob = f.read(byte_end - byte_start)
    prof["read"] += _now() - t0

    by_gidx = {}
    for sb in subblocks:
        by_gidx.setdefault(sb["group_idx"], []).append(sb)

    for gidx, subs in by_gidx.items():
        meta = groups[str(gidx)]
        record_size = meta["record_size"]
        master_info = meta["master_info"]
        channels = meta["channels"]
        rec_id_size = meta.get("rec_id_size", 0)
        us = {
            "rec_id_size": rec_id_size,
            "record_id": meta.get("record_id", 0),
            "cg_record_sizes": meta.get("cg_record_sizes"),
        }

        if rec_id_size > 0:
            parts = []
            for sb in sorted(subs, key=lambda x: x["abs_off"]):
                rel = sb["abs_off"] - byte_start
                try:
                    t0 = _now()
                    parts.append(_decompress_subblock_blob(blob, rel))
                    prof["decode"] += _now() - t0
                except Exception as e:
                    log.warning("stripe decompress failed gidx=%s off=%d: %s",
                                gidx, sb["abs_off"], e)
            if not parts:
                continue
            raw = b"".join(parts)
            raw, index_offset = prepare_cg_records(raw, record_size=record_size, **us)
            yield from _emit_prepared_signal_group(
                raw, channels, record_size, master_info, time_offset,
                emit_fn, prof, log, meta.get("data_block_addr", 0), _now,
                index_offset=index_offset,
            )
            continue

        for sb in sorted(subs, key=lambda x: x["rec_start"]):
            rel = sb["abs_off"] - byte_start
            try:
                t0 = _now()
                raw = _decompress_subblock_blob(blob, rel)
                prof["decode"] += _now() - t0
            except Exception as e:
                log.warning("stripe decompress failed gidx=%s off=%d: %s", gidx, sb["abs_off"], e)
                continue
            actual = len(raw) // record_size if record_size else 0
            if actual == 0:
                continue
            t0 = _now()
            if master_info is not None:
                ts = extract_timestamps(raw, record_size, master_info, index_offset=sb["rec_start"])
            else:
                ts = np.arange(sb["rec_start"], sb["rec_start"] + actual, dtype=np.float64)
            if time_offset:
                ts = ts + time_offset
            prof["decode"] += _now() - t0
            for ch in channels:
                try:
                    t0 = _now()
                    values = extract_signal(raw, record_size, ch)
                    prof["decode"] += _now() - t0
                    if values is None:
                        continue
                    yield from emit_fn(ts, values, ch["channel_id"])
                except Exception as e:
                    log.warning("stripe extract failed ch=%s: %s", ch.get("channel_id"), e)
                    continue
    yield from flush_fn()
