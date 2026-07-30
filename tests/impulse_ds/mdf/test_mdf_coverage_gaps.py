"""Targeted tests for MDF modules still below 95% line coverage."""

import io
import struct
import tempfile
from pathlib import Path

import numpy as np
import pytest

from impulse_ds.mdf.arrow_emit import (
    _eq_nan,
    _emit_prepared_signal_group,
    _make_signal_emitters,
    _rle_compress_chunk,
    _rle_flush,
    _rle_run_starts,
    convert_master_spec_to_arrow_batches,
    convert_spec_to_arrow_batches,
    convert_stripe_spec_to_arrow_batches,
    signals_arrow_schema,
)
from impulse_ds.mdf.mdf4_reader import (
    BLOCK_ID_CC,
    BLOCK_ID_HD,
    ChannelInfo,
    CN_TYPE_MASTER,
    CN_TYPE_VIRTUAL_MASTER,
    MDF4Reader,
)
from impulse_ds.mdf.mdf_blocks import (
    _decompress_subblock_blob,
    _read_block_chunks,
    _read_subblock_file,
    parse_subblocks,
    read_data_list_range,
    read_raw_data,
)
from impulse_ds.mdf.mdf_decode import (
    CC_IDENTITY,
    CC_LINEAR,
    CC_RATIONAL,
    CC_TAB_INTERP,
    CC_TAB_NOINTERP,
    FLOAT_BE,
    FLOAT_LE,
    SINT_BE,
    SINT_LE,
    UINT_BE,
    UINT_LE,
    apply_cc_conversion,
    apply_invalidation,
    convert_values,
    extract_signal,
    extract_timestamps,
    filter_unsorted_records,
    prepare_cg_records,
    read_record_id,
    storage_record_id,
    unsorted_fields_from_ctx,
)
from impulse_ds.mdf.datasources import MdfMastersReader, MdfSignalsReader
from ._block_fixtures import build_dl_file, make_dt_block, make_dz_block
from ._mdf_samples import sample_mdf_dir


def _interleaved_raw():
    rec_id_size = 4
    record_size = 20
    cg_sizes = {1: record_size, 2: record_size}
    r1 = struct.pack("<I", 1) + struct.pack("<d", 1.0) + struct.pack("<d", 10.0)
    r2 = struct.pack("<I", 2) + struct.pack("<d", 2.0) + struct.pack("<d", 20.0)
    return b"".join([r1, r2, r1]), rec_id_size, cg_sizes, record_size


def _unsorted_ch_spec(rec_id_size, cg_sizes, record_size, data_block_addr=0):
    return {
        "channel_id": 0,
        "group_idx": 0,
        "channel_idx": 0,
        "sample_count": 2,
        "data_type": FLOAT_LE,
        "bit_offset": 0,
        "byte_offset": 12,
        "bit_count": 64,
        "channel_type": 0,
        "data_block_addr": data_block_addr,
        "record_size": record_size,
        "master_info": {
            "byte_offset": 4,
            "bit_offset": 0,
            "bit_count": 64,
            "data_type": FLOAT_LE,
            "channel_type": 2,
            "cc_type": -1,
            "cc_params": [],
            "rec_id_size": rec_id_size,
            "record_id": 1,
            "cg_record_sizes": cg_sizes,
        },
        "cn_flags": 0,
        "invalidation_bit_pos": 0,
        "invalidation_bytes": 0,
        "data_bytes": 8,
        "cc_type": -1,
        "cc_params": [],
        "rec_id_size": rec_id_size,
        "record_id": 1,
        "cg_record_sizes": cg_sizes,
    }


class TestMdfDecodeCoverage:
    def test_float_unsupported_bit_count(self):
        col = np.zeros((2, 8), dtype=np.uint8)
        out = convert_values(col, FLOAT_LE, 16, 0, 2)
        assert np.all(out == 0.0)

    def test_uint_be_and_padded_widths(self):
        raw16 = struct.pack(">2H", 100, 200)
        col16 = np.frombuffer(raw16, dtype=np.uint8).reshape(2, 2)
        np.testing.assert_allclose(
            convert_values(col16, UINT_BE, 16, 0, 2),
            [100.0, 200.0],
        )
        # 24-bit little-endian in 3-byte columns
        col24 = np.array([[0x12, 0x34, 0x56], [0x78, 0x9A, 0xBC]], dtype=np.uint8)
        out = convert_values(col24, UINT_LE, 24, 0, 2)
        assert out[0] == pytest.approx(0x563412)
        assert out[1] == pytest.approx(0xBC9A78)

    def test_sint_be_and_padded(self):
        raw = struct.pack(">2h", -3, 7)
        col = np.frombuffer(raw, dtype=np.uint8).reshape(2, 2)
        np.testing.assert_allclose(
            convert_values(col, SINT_BE, 16, 0, 2),
            [-3.0, 7.0],
        )
        raw32 = struct.pack("<i", -42)
        col32 = np.frombuffer(raw32, dtype=np.uint8).reshape(1, 4)
        assert convert_values(col32, SINT_LE, 32, 0, 1)[0] == pytest.approx(-42.0)

    def test_cc_linear_identity_and_scale(self):
        v = np.array([1.0, 2.0])
        assert np.array_equal(apply_cc_conversion(v.copy(), CC_IDENTITY, ()), v)
        out = apply_cc_conversion(np.array([2.0, 4.0]), CC_LINEAR, (0.0, 1.0))
        np.testing.assert_allclose(out, [2.0, 4.0])
        out = apply_cc_conversion(np.array([1.0, 2.0]), CC_LINEAR, (5.0, 2.0))
        np.testing.assert_allclose(out, [7.0, 9.0])

    def test_cc_rational_and_tab_interp(self):
        raw = np.array([1.0, 2.0])
        rat = apply_cc_conversion(
            raw.copy(),
            CC_RATIONAL,
            (0.0, 1.0, 0.0, 0.0, 0.0, 1.0),
        )
        np.testing.assert_allclose(rat, [1.0, 2.0])
        tab = apply_cc_conversion(
            np.array([0.0, 50.0, 100.0]),
            CC_TAB_INTERP,
            (0.0, 0.0, 100.0, 10.0),
        )
        np.testing.assert_allclose(tab, [0.0, 5.0, 10.0])

    def test_invalidation_ndarray_buffer_and_bit_offset(self):
        record_size = 10
        n = 2
        buf = np.zeros(n * record_size, dtype=np.uint8)
        buf[0] = 10
        buf[record_size] = 20
        buf[8] = 0x02  # bit 1 set on first record
        values = np.array([10.0, 20.0])
        info = {
            "cn_flags": 2,
            "invalidation_bit_pos": 1,
            "invalidation_bytes": 1,
            "data_bytes": 8,
        }
        out = apply_invalidation(buf, record_size, n, values.copy(), info)
        assert np.isnan(out[0])
        assert out[1] == 20.0

    def test_prepare_cg_empty_and_empty_slice(self):
        raw, rec_id_size, cg_sizes, record_size = _interleaved_raw()
        empty, off = prepare_cg_records(
            raw,
            record_size=record_size,
            rec_id_size=rec_id_size,
            record_id=1,
            cg_record_sizes=None,
        )
        assert empty == b""
        sliced, off2 = prepare_cg_records(
            raw,
            record_size=record_size,
            rec_id_size=rec_id_size,
            record_id=1,
            cg_record_sizes=cg_sizes,
            row_start=2,
            row_end=2,
        )
        assert sliced == b""
        assert off2 == 2

    def test_unsorted_fields_from_ctx(self):
        assert unsorted_fields_from_ctx(1, 2, None) == {
            "rec_id_size": 0,
            "record_id": 0,
            "cg_record_sizes": None,
        }
        ctx = {5: {"rec_id_size": 4, "cg_sizes": {1: 20}}}
        out = unsorted_fields_from_ctx(5, 1, ctx)
        assert out["rec_id_size"] == 4
        assert out["record_id"] == 1

    def test_extract_signal_fast_paths(self):
        n, rs = 3, 16
        raw = b"".join(
            struct.pack("<d", float(i))
            + struct.pack("<f", float(i))
            + struct.pack("<H", i)
            + b"\x00\x00"
            for i in range(n)
        )
        base = {
            "channel_type": 0,
            "bit_offset": 0,
            "record_size": rs,
            "cn_flags": 0,
            "invalidation_bytes": 0,
            "data_bytes": 8,
            "cc_type": -1,
            "cc_params": [],
        }
        ch_f64 = {**base, "data_type": FLOAT_LE, "bit_count": 64, "byte_offset": 0}
        ch_f32 = {**base, "data_type": FLOAT_LE, "bit_count": 32, "byte_offset": 8}
        ch_u16 = {**base, "data_type": UINT_LE, "bit_count": 16, "byte_offset": 12}
        np.testing.assert_allclose(extract_signal(raw, rs, ch_f64), [0.0, 1.0, 2.0])
        np.testing.assert_allclose(extract_signal(raw, rs, ch_f32), [0.0, 1.0, 2.0])
        np.testing.assert_allclose(extract_signal(raw, rs, ch_u16), [0.0, 1.0, 2.0])

    def test_extract_timestamps_virtual_master(self):
        raw = b"\x00" * 24
        master = {"channel_type": 3, "cc_type": -1, "cc_params": []}
        ts = extract_timestamps(raw, 8, master, index_offset=5)
        np.testing.assert_allclose(ts, [5.0, 6.0, 7.0])


class TestMdfBlocksCoverage:
    def test_dl_with_dz_subblocks(self):
        p1 = make_dz_block(struct.pack("<d", 1.0))
        p2 = make_dz_block(struct.pack("<d", 2.0))
        blob, dl_addr = build_dl_file([p1, p2])
        with io.BytesIO(blob) as f:
            raw = read_raw_data(f, dl_addr, record_size=8, sample_count=2)
        vals = np.frombuffer(raw, dtype=np.float64)
        np.testing.assert_allclose(vals, [1.0, 2.0])

    def test_read_subblock_file_dt(self):
        payload = struct.pack("<2d", 3.0, 4.0)
        blob = make_dt_block(payload)
        with io.BytesIO(blob) as f:
            out = _read_subblock_file(f, 0)
        assert out == payload

    def test_read_block_chunks_unsorted(self):
        raw, rec_id_size, cg_sizes, record_size = _interleaved_raw()
        blob = make_dt_block(raw)
        import logging

        prof = {"read": 0}
        with io.BytesIO(blob) as f:
            chunks = list(
                _read_block_chunks(
                    f,
                    0,
                    record_size,
                    3,
                    None,
                    None,
                    prof,
                    logging.getLogger("t"),
                    rec_id_size=rec_id_size,
                    record_id=1,
                    cg_record_sizes=cg_sizes,
                )
            )
        assert len(chunks) == 1
        data, start = chunks[0]
        assert start == 0
        assert len(data) == 2 * record_size

    def test_parse_subblocks_unknown_block(self):
        blob = b"##XX" + b"\x00" * 4 + struct.pack("<Q", 32) + b"\x00" * 16
        with io.BytesIO(blob) as f:
            subs = parse_subblocks(f, 0, 8, 1)
        assert len(subs) == 1

    def test_read_raw_data_unknown_block_fallback(self):
        payload = struct.pack("<d", 9.0)
        blob = b"##XX" + b"\x00" * 4 + struct.pack("<Q", 32) + b"\x00" * 8 + payload
        with io.BytesIO(blob) as f:
            raw = read_raw_data(f, 0, 8, 1)
        assert raw == payload


class TestArrowEmitCoverage:
    def test_convert_spec_unsorted_and_time_offset(self):
        raw, rec_id_size, cg_sizes, record_size = _interleaved_raw()
        blob = make_dt_block(raw)
        fd, path = tempfile.mkstemp(suffix=".mf4")
        os_write = __import__("os").write
        os_write(fd, blob)
        __import__("os").close(fd)
        try:
            spec = {
                "file_path": path,
                "channels": [_unsorted_ch_spec(rec_id_size, cg_sizes, record_size)],
                "time_offset": 100.0,
            }
            batches = list(convert_spec_to_arrow_batches(spec))
            times = batches[0].column("time").to_pylist()
            assert times[0] == pytest.approx(101.0)
        finally:
            Path(path).unlink(missing_ok=True)

    def test_convert_spec_dl_row_range(self):
        records = [struct.pack("<d", float(i)) for i in range(4)]
        blob, dl_addr = build_dl_file(records)
        fd, path = tempfile.mkstemp(suffix=".mf4")
        __import__("os").write(fd, blob)
        __import__("os").close(fd)
        try:
            ch = _unsorted_ch_spec(0, {}, 8, dl_addr)
            ch["sample_count"] = 4
            ch["byte_offset"] = 0
            ch["master_info"] = {
                "byte_offset": 0,
                "bit_offset": 0,
                "bit_count": 64,
                "data_type": FLOAT_LE,
                "channel_type": 3,
                "cc_type": -1,
                "cc_params": [],
            }
            ch["rec_id_size"] = 0
            ch.pop("cg_record_sizes", None)
            spec = {"file_path": path, "channels": [ch], "row_start": 1, "row_end": 3}
            batches = list(convert_spec_to_arrow_batches(spec))
            assert batches[0].num_rows == 2
        finally:
            Path(path).unlink(missing_ok=True)

    def test_stripe_on_compressed_sample(self):
        d, files = sample_mdf_dir()
        if len(files) < 2:
            pytest.skip("no compressed sample")
        path = f"{d}/{files[1]}"
        with open(path, "rb") as fh:
            fb = fh.read()
        from impulse_ds.mdf.bin_packer import plan_stripes_for_file

        specs = plan_stripes_for_file(path, file_bytes=fb, stripe_target_mb=0.001)
        total = sum(b.num_rows for s in specs for b in convert_stripe_spec_to_arrow_batches(s))
        assert total > 0

    def test_rle_helpers_edge_cases(self):
        assert _eq_nan(float("nan"), float("nan"))
        assert not _eq_nan(1.0, 2.0)
        assert len(_rle_run_starts(np.array([1.0]))) == 1
        closed, carry = _rle_compress_chunk(np.array([]), np.array([]), None)
        assert closed is None and carry is None
        ts = np.array([0.0, 1.0])
        vs = np.array([3.0, 7.0])
        closed, carry = _rle_compress_chunk(ts, vs, [3.0, 0.0, 1.0])
        assert closed is not None
        assert _rle_flush(None) is None

    def test_rle_integration_small_partition(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        reader = MdfSignalsReader(
            {
                "path": d,
                "files": files[0],
                "target_partition_mb": "0.0001",
                "run_length_encoding": "true",
            }
        )
        rows = sum(b.num_rows for p in reader.partitions() for b in reader.read(p))
        assert rows > 0

    def test_signals_float32_and_masters_schema(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        from impulse_ds.mdf.datasources import MdfMastersDataSource, MdfSignalsDataSource

        opts = {"path": d, "files": files[0], "time_dtype": "float32", "value_dtype": "float32"}
        assert MdfSignalsDataSource(opts).schema()["value"].dataType.simpleString() == "float"
        assert MdfMastersDataSource(opts).schema()["timestamp"].dataType.simpleString() == "float"


class TestMdf4ReaderCoverage:
    def _make_cc_block(self, cc_type, params):
        val_count = len(params)
        body = (
            struct.pack("<B", cc_type)
            + struct.pack("<B", 0)
            + struct.pack("<H", 0)
            + struct.pack("<H", 0)
            + struct.pack("<H", val_count)
            + struct.pack("<d", 0.0)
            + struct.pack("<d", 100.0)
            + struct.pack(f"<{val_count}d", *params)
        )
        link_count = 0
        length = 24 + len(body)
        return (
            BLOCK_ID_CC
            + b"\x00" * 4
            + struct.pack("<Q", length)
            + struct.pack("<Q", link_count)
            + body
        )

    def test_parse_cc_block_variants(self):
        cc = self._make_cc_block(1, (0.0, 2.0))
        blob = b"\x00" * 8 + cc
        with io.BytesIO(blob) as f:
            cc_type, params = MDF4Reader._parse_cc_block(f, 8)
        assert cc_type == 1
        assert params == (0.0, 2.0)
        with io.BytesIO(b"##XX" + b"\x00" * 20) as f:
            assert MDF4Reader._parse_cc_block(f, 0) == (-1, ())
        assert MDF4Reader._parse_cc_block(io.BytesIO(blob), 0) == (-1, ())
        bad = self._make_cc_block(99, (1.0,))
        with io.BytesIO(b"\x00" * 8 + bad) as f:
            assert MDF4Reader._parse_cc_block(f, 8) == (-1, ())

    def test_read_text_block_cache_and_md(self):
        tx = b"##TX" + b"\x00" * 4 + struct.pack("<Q", 32) + struct.pack("<Q", 0) + b"hello\x00"
        md = b"##MD" + b"\x00" * 4 + struct.pack("<Q", 32) + struct.pack("<Q", 0) + b"note\x00"
        blob = b"\x00" * 8 + tx + md
        reader = MDF4Reader(file_bytes=b"MDF     " + b"\x00" * 56)
        with io.BytesIO(blob) as f:
            assert reader._read_text_block(f, 8) == "hello"
            assert reader._read_text_block(f, 8) == "hello"  # cache hit
            assert reader._read_text_block(f, 8 + len(tx)) == "note"
            assert reader._read_text_block(f, 0) == ""

    def test_read_channel_data_with_file_handle(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        reader = MDF4Reader(path)
        ch = [c for c in reader.scan_metadata() if c.channel_type == 0][0]
        with open(path, "rb") as fh:
            vals = MDF4Reader.read_channel_data(
                path,
                ch.data_block_addr,
                ch.record_size,
                ch.byte_offset,
                ch.bit_offset,
                ch.bit_count,
                ch.data_type,
                ch.channel_type,
                ch.sample_count,
                f=fh,
            )
        assert len(vals) == ch.sample_count

    def test_read_channel_pair_no_master(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        reader = MDF4Reader(path)
        sig = [c for c in reader.scan_metadata() if c.channel_type == 0][0]
        sig_d = MDF4Reader.channel_to_dict(sig)
        with open(path, "rb") as fh:
            ts, vals = MDF4Reader.read_channel_pair(
                path,
                None,
                sig_d,
                sig.sample_count,
                f=fh,
            )
        assert len(ts) == sig.sample_count
        assert len(vals) == sig.sample_count

    def test_master_to_dict(self):
        ch = ChannelInfo(
            group_idx=0,
            channel_idx=0,
            channel_name="t",
            unit="s",
            sample_count=1,
            data_type=4,
            bit_offset=0,
            byte_offset=0,
            bit_count=64,
            channel_type=CN_TYPE_VIRTUAL_MASTER,
            cn_block_addr=0,
            cg_block_addr=0,
            dg_block_addr=50,
            data_block_addr=100,
            record_size=8,
            rec_id_size=4,
            record_id=2,
        )
        ctx = {50: {"rec_id_size": 4, "cg_sizes": {2: 8}}}
        d = MDF4Reader.master_to_dict(ch, ctx)
        assert d["channel_type"] == CN_TYPE_VIRTUAL_MASTER
        assert d["rec_id_size"] == 4

    def test_hd_start_time_none(self):
        blob = bytearray(128)
        blob[0:8] = b"MDF     "
        blob[64:68] = BLOCK_ID_HD
        struct.pack_into("<Q", blob, 72, 104)  # hd block length
        struct.pack_into("<Q", blob, 80, 0)  # link_count
        struct.pack_into("<Q", blob, 88, 0)  # start_time_ns = unset
        reader = MDF4Reader(file_bytes=bytes(blob))
        assert reader.read_header_start_epoch_seconds() is None
        assert reader.read_header_datetime() is None

    def test_sample_d_integer_channels(self):
        d, files = sample_mdf_dir()
        if "sample_d.mf4" not in files:
            pytest.skip("no int sample")
        path = f"{d}/sample_d.mf4"
        reader = MDF4Reader(path)
        channels = {c.channel_name: c for c in reader.scan_metadata()}
        assert "UInt16Ramp" in channels
        assert "Int32Signed" in channels

    def test_metadata_reader_empty_path(self):
        from pyspark.sql.datasource import InputPartition
        from impulse_ds.mdf.datasources import MdfMetadataReader

        reader = MdfMetadataReader({"path": "/tmp", "files": "x.mf4"})
        assert list(reader.read(InputPartition({"file_path": ""}))) == []

    def test_masters_partitions_no_masters(self, monkeypatch):
        def _no_masters(self):
            return {
                "master_channels": {},
                "signal_channels": [],
                "channel_id_map": {},
                "unsorted_dg_ctx": {},
            }

        monkeypatch.setattr(
            "impulse_ds.mdf.datasources._resolve_file_list",
            lambda _o: ["/x.mf4"],
        )
        monkeypatch.setattr(
            "impulse_ds.mdf.mdf4_reader.MDF4Reader.scan_channels_organized",
            _no_masters,
        )
        reader = MdfMastersReader({"path": "/x", "files": "a.mf4"})
        parts = reader.partitions()
        assert len(parts) == 1
        assert list(reader.read(parts[0])) == []


class TestMdfDecodeMoreCoverage:
    def test_storage_record_id_and_filter_edges(self):
        assert storage_record_id(0x100000001, 8) == 0x100000001
        assert storage_record_id(0x100000001, 4) == 1
        raw, rec_id_size, cg_sizes, record_size = _interleaved_raw()
        assert filter_unsorted_records(raw, 0, 1, cg_sizes) == raw
        truncated = raw[: record_size + 2]
        assert len(filter_unsorted_records(truncated, rec_id_size, 1, cg_sizes)) == record_size

    def test_apply_cc_unknown_type(self):
        v = np.array([1.0])
        assert apply_cc_conversion(v.copy(), 99, (1.0,))[0] == 1.0

    def test_prepare_cg_zero_record_size(self):
        assert prepare_cg_records(b"\x00", record_size=0) == (b"", 0)

    def test_extract_signal_all_fast_paths(self):
        base = {
            "channel_type": 0,
            "bit_offset": 0,
            "cn_flags": 0,
            "invalidation_bytes": 0,
            "data_bytes": 8,
            "cc_type": -1,
            "cc_params": [],
        }
        cases = [
            (
                8,
                struct.pack("<d", 1.0),
                {**base, "data_type": FLOAT_LE, "bit_count": 64, "byte_offset": 0},
                1.0,
            ),
            (
                8,
                struct.pack("<f", 2.0),
                {**base, "data_type": FLOAT_LE, "bit_count": 32, "byte_offset": 0},
                2.0,
            ),
            (
                4,
                struct.pack("<H", 3),
                {**base, "data_type": UINT_LE, "bit_count": 16, "byte_offset": 0},
                3.0,
            ),
            (
                4,
                struct.pack("<I", 4),
                {**base, "data_type": UINT_LE, "bit_count": 32, "byte_offset": 0},
                4.0,
            ),
            (
                2,
                struct.pack("<B", 5),
                {**base, "data_type": UINT_LE, "bit_count": 8, "byte_offset": 0},
                5.0,
            ),
            (
                2,
                struct.pack("<b", -1),
                {**base, "data_type": SINT_LE, "bit_count": 8, "byte_offset": 0},
                -1.0,
            ),
            (
                4,
                struct.pack("<h", -2),
                {**base, "data_type": SINT_LE, "bit_count": 16, "byte_offset": 0},
                -2.0,
            ),
            (
                8,
                struct.pack("<i", -3),
                {**base, "data_type": SINT_LE, "bit_count": 32, "byte_offset": 0},
                -3.0,
            ),
            (
                8,
                struct.pack("<Q", 5),
                {**base, "data_type": UINT_LE, "bit_count": 64, "byte_offset": 0},
                5.0,
            ),
            (
                8,
                struct.pack("<q", -4),
                {**base, "data_type": SINT_LE, "bit_count": 64, "byte_offset": 0},
                -4.0,
            ),
        ]
        for rs, raw, ch, expected in cases:
            ch = {**ch, "record_size": rs}
            padded = raw + b"\x00" * (rs - len(raw))
            vals = extract_signal(padded, rs, ch)
            assert vals[0] == pytest.approx(expected)

    def test_extract_signal_slow_path_and_cc(self):
        rs = 4
        raw = struct.pack("<H", 0x1234) + b"\x00\x00"
        ch = {
            "channel_type": 0,
            "data_type": UINT_LE,
            "bit_count": 12,
            "byte_offset": 0,
            "bit_offset": 4,
            "record_size": rs,
            "cn_flags": 0,
            "invalidation_bytes": 0,
            "data_bytes": 2,
            "cc_type": CC_LINEAR,
            "cc_params": [1.0, 2.0],
        }
        raw_val = 0x1234 >> 4
        vals = extract_signal(raw, rs, ch)
        assert vals[0] == pytest.approx(raw_val * 2.0 + 1.0)

    def test_extract_timestamps_with_cc(self):
        raw = struct.pack("<d", 2.0)
        master = {
            "channel_type": 2,
            "byte_offset": 0,
            "bit_offset": 0,
            "bit_count": 64,
            "data_type": FLOAT_LE,
            "cc_type": CC_LINEAR,
            "cc_params": [1.0, 10.0],
        }
        ts = extract_timestamps(raw, 8, master)
        assert ts[0] == pytest.approx(21.0)

    def test_extract_timestamps_fast_paths_and_empty(self):
        assert (
            len(
                extract_timestamps(
                    b"",
                    8,
                    {
                        "channel_type": 2,
                        "data_type": FLOAT_LE,
                        "bit_count": 64,
                        "byte_offset": 0,
                        "bit_offset": 0,
                    },
                )
            )
            == 0
        )
        raw = struct.pack("<f", 1.5) + b"\x00" * 4
        master = {
            "channel_type": 2,
            "byte_offset": 0,
            "bit_offset": 0,
            "bit_count": 32,
            "data_type": FLOAT_LE,
            "cc_type": -1,
            "cc_params": [],
        }
        np.testing.assert_allclose(extract_timestamps(raw, 8, master), [1.5])
        raw2 = struct.pack("<Q", 42)
        master64 = {**master, "bit_count": 64, "data_type": UINT_LE}
        assert extract_timestamps(raw2, 8, master64)[0] == 42.0

    def test_apply_invalidation_early_returns(self):
        v = np.array([1.0])
        info = {"cn_flags": 0, "invalidation_bytes": 1, "data_bytes": 8}
        assert apply_invalidation(b"\x00", 8, 1, v.copy(), info)[0] == 1.0
        info2 = {"cn_flags": 2, "invalidation_bytes": 0, "data_bytes": 8}
        assert apply_invalidation(b"\x00", 8, 1, v.copy(), info2)[0] == 1.0

    def test_convert_values_float_be64_and_uint64_padded(self):
        col = np.frombuffer(struct.pack(">d", 2.5), dtype=np.uint8).reshape(1, 8)
        np.testing.assert_allclose(convert_values(col, FLOAT_BE, 64, 0, 1), [2.5])
        col5 = np.zeros((1, 5), dtype=np.uint8)
        col5[0, :5] = [1, 2, 3, 4, 5]
        assert convert_values(col5, UINT_LE, 40, 0, 1)[0] > 0
        cols = np.array([[0xFF]], dtype=np.uint8)
        assert convert_values(cols, SINT_LE, 8, 0, 1)[0] == -1.0

    def test_read_record_id_zero_size(self):
        assert read_record_id(b"\x05", 0, 0) == 0

    def test_filter_truncated_tail(self):
        raw, rec_id_size, cg_sizes, record_size = _interleaved_raw()
        bad = raw + struct.pack("<I", 1)  # truncated record id only
        assert len(filter_unsorted_records(bad, rec_id_size, 1, cg_sizes)) == 2 * record_size

    def test_prepare_cg_empty_after_filter(self):
        assert prepare_cg_records(
            b"\x00\x00", record_size=8, rec_id_size=4, record_id=99, cg_record_sizes={1: 8}
        ) == (b"", 0)

    def test_extract_signal_returns_none_when_empty(self):
        assert (
            extract_signal(
                b"",
                8,
                {
                    "channel_type": 0,
                    "data_type": FLOAT_LE,
                    "bit_count": 64,
                    "byte_offset": 0,
                    "bit_offset": 0,
                },
            )
            is None
        )

    def test_storage_record_id_zero_size(self):
        assert storage_record_id(99, 0) == 0

    def test_convert_values_float_be32_unsupported(self):
        col = np.zeros((1, 4), dtype=np.uint8)
        assert convert_values(col, FLOAT_BE, 16, 0, 1)[0] == 0.0

    def test_convert_values_sint_be_padded(self):
        col = np.array([[0xFF, 0x80]], dtype=np.uint8)
        out = convert_values(col, SINT_BE, 16, 0, 1)
        assert out[0] < 0
        col32 = np.zeros((1, 3), dtype=np.uint8)
        col32[0, :3] = [0xFF, 0xFF, 0x7F]
        assert convert_values(col32, SINT_BE, 24, 0, 1)[0] < 0
        col64 = np.zeros((1, 5), dtype=np.uint8)
        col64[0, :5] = [0, 0, 0, 0, 128]
        assert convert_values(col64, SINT_BE, 40, 0, 1)[0] != 0.0
        colu = np.zeros((1, 3), dtype=np.uint8)
        colu[0, :3] = [1, 2, 3]
        assert convert_values(colu, UINT_LE, 24, 0, 1)[0] > 0
        colf = np.frombuffer(struct.pack(">f", 2.0), dtype=np.uint8).reshape(1, 4)
        np.testing.assert_allclose(convert_values(colf, FLOAT_BE, 32, 0, 1), [2.0])
        col = np.array([[0x12, 0x34, 0x56, 0x00]], dtype=np.uint8)
        out = convert_values(col, UINT_BE, 24, 0, 1)
        assert out[0] == pytest.approx(3429888.0)


class TestMdfBlocksMoreCoverage:
    def test_dz_transpose_remainder_in_dl(self):
        payload = bytes(range(10))
        dz = make_dz_block(payload, zip_type=1, zip_parameter=3)
        blob, dl_addr = build_dl_file([dz])
        with io.BytesIO(blob) as f:
            raw = read_raw_data(f, dl_addr, record_size=1, sample_count=10)
        assert raw == payload

    def test_hl_zero_link_returns_empty(self):
        hl = (
            b"##HL"
            + b"\x00" * 4
            + struct.pack("<Q", 32)
            + struct.pack("<Q", 1)
            + struct.pack("<Q", 0)
        )
        with io.BytesIO(hl) as f:
            assert read_raw_data(f, 0, 8, 1) == b""

    def test_read_data_list_range_empty(self):
        empty_dl = b"##DL" + b"\x00" * 4 + struct.pack("<Q", 32) + struct.pack("<Q", 1)
        empty_dl += struct.pack("<Q", 0) + b"\x00" * 4 + struct.pack("<I", 0)
        with io.BytesIO(empty_dl) as f:
            raw, start = read_data_list_range(f, 0, 8, 0, 1)
        assert raw == b""

    def test_read_block_chunks_dz_whole_block_slice(self):
        payload = b"".join(struct.pack("<d", float(i)) for i in range(5))
        blob = make_dz_block(payload)
        import logging

        prof = {"read": 0}
        with io.BytesIO(blob) as f:
            chunks = list(
                _read_block_chunks(
                    f,
                    0,
                    8,
                    5,
                    1,
                    4,
                    prof,
                    logging.getLogger("t"),
                )
            )
        assert len(chunks) == 1
        assert len(chunks[0][0]) == 3 * 8

    def test_decompress_subblock_dz_transpose_remainder(self):
        payload = bytes(range(10))
        dz = make_dz_block(payload, zip_type=1, zip_parameter=3)
        out = _decompress_subblock_blob(dz, 0)
        assert out == payload

    def test_collect_dl_stops_on_non_dl_block(self):
        from impulse_ds.mdf.mdf_blocks import _collect_dl_block_addrs
        from ._block_fixtures import make_dl_block

        bad = b"##XX" + b"\x00" * 20
        dt = make_dt_block(b"\x01\x02\x03\x04" + b"\x00" * 4)
        blob = bad + dt
        dl_addr = len(blob)
        blob += make_dl_block(len(bad), [len(bad)])
        with io.BytesIO(blob) as f:
            addrs = _collect_dl_block_addrs(f, dl_addr)
        assert addrs == [len(bad)]

    def test_read_dl_blob_short_header(self):
        from impulse_ds.mdf.mdf_blocks import _read_dl_blob

        blob, dl_addr = build_dl_file([struct.pack("<d", 1.0)])
        with io.BytesIO(blob[:20]) as f:
            b, lo, addrs = _read_dl_blob(f, dl_addr)
        assert b == b""

    def test_read_data_list_raw_empty_meta(self):
        from impulse_ds.mdf.mdf_blocks import read_data_list_raw

        empty_dl = b"##DL" + b"\x00" * 4 + struct.pack("<Q", 32) + struct.pack("<Q", 1)
        empty_dl += struct.pack("<Q", 0) + b"\x00" * 4 + struct.pack("<I", 0)
        with io.BytesIO(empty_dl) as f:
            assert read_data_list_raw(f, 0) == b""

    def test_read_block_chunks_dl_row_range(self):
        records = [struct.pack("<d", float(i)) for i in range(4)]
        blob, dl_addr = build_dl_file(records)
        import logging

        prof = {"read": 0}
        with io.BytesIO(blob) as f:
            chunks = list(
                _read_block_chunks(
                    f,
                    dl_addr,
                    8,
                    4,
                    1,
                    3,
                    prof,
                    logging.getLogger("t"),
                )
            )
        assert len(chunks) == 1
        assert len(chunks[0][0]) == 2 * 8

    def test_parse_subblocks_dz_and_hl_none(self):
        dz = make_dz_block(struct.pack("<d", 1.0))
        with io.BytesIO(dz) as f:
            subs = parse_subblocks(f, 0, 8, 1)
        assert subs[0][3] == 1
        hl = (
            b"##HL"
            + b"\x00" * 4
            + struct.pack("<Q", 32)
            + struct.pack("<Q", 1)
            + struct.pack("<Q", 0)
        )
        with io.BytesIO(hl) as f:
            assert parse_subblocks(f, 0, 8, 1) == []

    def test_read_data_list_range_no_overlap(self):
        records = [struct.pack("<d", float(i)) for i in range(3)]
        blob, dl_addr = build_dl_file(records)
        with io.BytesIO(blob) as f:
            raw, start = read_data_list_range(f, dl_addr, 8, 5, 10)
        assert raw == b""
        assert start == 3

    def test_read_block_chunks_extent_error_and_empty_dt(self, monkeypatch):
        import logging
        from impulse_ds.mdf import mdf_blocks as mb

        def _raise(*a, **k):
            raise OSError("extent")

        monkeypatch.setattr(mb, "dt_data_extent", _raise)
        payload = make_dt_block(b"\x00" * 8)
        prof = {"read": 0}
        with io.BytesIO(payload) as f:
            chunks = list(
                _read_block_chunks(
                    f,
                    0,
                    8,
                    1,
                    None,
                    None,
                    prof,
                    logging.getLogger("t"),
                )
            )
        assert len(chunks) == 1

    def test_parse_subblocks_on_dz_block(self):
        dz = make_dz_block(struct.pack("<d", 1.0))
        with io.BytesIO(dz) as f:
            subs = parse_subblocks(f, 0, 8, 1)
        assert subs[0][3] == 1


class TestArrowEmitMoreCoverage:
    def test_convert_spec_dz_row_slice(self):
        d, files = sample_mdf_dir()
        if len(files) < 2:
            pytest.skip("no compressed sample")
        path = f"{d}/{files[1]}"
        from impulse_ds.mdf.bin_packer import plan_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        org = MDF4Reader(path).scan_channels_organized()
        specs = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=0.00001,
        )
        batches = list(convert_spec_to_arrow_batches(specs[0]))
        assert batches
        assert batches[0].num_rows > 0

    def test_convert_master_virtual_and_float32(self):
        spec = {
            "file_path": "/dev/null",
            "time_dtype": "float32",
            "time_offset": 1.5,
            "row_start": 2,
            "row_end": 5,
            "masters": [
                {
                    "group_idx": 0,
                    "record_size": 8,
                    "sample_count": 10,
                    "data_block_addr": 0,
                    "master_info": {"channel_type": 3, "cc_type": -1, "cc_params": []},
                }
            ],
        }
        batches = list(convert_master_spec_to_arrow_batches(spec))
        assert len(batches) == 1
        ts = batches[0].column("timestamp").to_pylist()
        assert ts == pytest.approx([3.5, 4.5, 5.5])

    def test_convert_master_from_sample(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_master_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader
        from impulse_ds.mdf.udf_helpers import convert_master_spec_to_arrow_batches

        org = MDF4Reader(path).scan_channels_organized()
        specs = plan_master_partitions(
            path,
            org["master_channels"],
            target_partition_mb=16,
            unsorted_dg_ctx=org["unsorted_dg_ctx"],
        )
        batches = list(convert_master_spec_to_arrow_batches(specs[0]))
        assert batches[0].num_rows > 0

    def test_stripe_unsorted_interleaved(self):
        raw, rec_id_size, cg_sizes, record_size = _interleaved_raw()
        blob = make_dt_block(raw)
        fd, path = tempfile.mkstemp(suffix=".mf4")
        __import__("os").write(fd, blob)
        __import__("os").close(fd)
        try:
            ch = _unsorted_ch_spec(rec_id_size, cg_sizes, record_size, 0)
            master = ch["master_info"]
            spec = {
                "file_path": path,
                "byte_start": 0,
                "byte_end": len(blob),
                "groups": {
                    "0": {
                        "record_size": record_size,
                        "master_info": master,
                        "channels": [ch],
                        "rec_id_size": rec_id_size,
                        "record_id": 1,
                        "cg_record_sizes": cg_sizes,
                    },
                },
                "subblocks": [
                    {
                        "group_idx": 0,
                        "abs_off": 0,
                        "on_disk_len": len(blob),
                        "rec_start": 0,
                        "rec_count": 3,
                    }
                ],
            }
            batches = list(convert_stripe_spec_to_arrow_batches(spec))
            assert batches[0].num_rows == 2
        finally:
            Path(path).unlink(missing_ok=True)

    def test_stripe_without_master_info(self):
        payload = b"".join(struct.pack("<d", float(i)) for i in range(3))
        blob = make_dt_block(payload)
        fd, path = tempfile.mkstemp(suffix=".mf4")
        __import__("os").write(fd, blob)
        __import__("os").close(fd)
        try:
            ch = _unsorted_ch_spec(0, {}, 8, 0)
            ch["sample_count"] = 3
            ch["byte_offset"] = 0
            ch.pop("master_info", None)
            spec = {
                "file_path": path,
                "byte_start": 0,
                "byte_end": len(blob),
                "groups": {
                    "0": {
                        "record_size": 8,
                        "master_info": None,
                        "channels": [ch],
                    },
                },
                "subblocks": [
                    {
                        "group_idx": 0,
                        "abs_off": 0,
                        "on_disk_len": len(blob),
                        "rec_start": 0,
                        "rec_count": 3,
                    }
                ],
            }
            batches = list(convert_stripe_spec_to_arrow_batches(spec))
            assert batches[0].num_rows == 3
        finally:
            Path(path).unlink(missing_ok=True)

    def test_convert_spec_skips_zero_sample_channels(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        org = MDF4Reader(path).scan_channels_organized()
        specs = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=16,
        )
        spec = dict(specs[0])
        spec["channels"] = [dict(spec["channels"][0])]
        spec["channels"][0]["sample_count"] = 0
        assert list(convert_spec_to_arrow_batches(spec)) == []

    def test_convert_spec_read_failure_is_resilient(self, monkeypatch):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        org = MDF4Reader(path).scan_channels_organized()
        specs = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=16,
        )
        spec = dict(specs[0])
        spec["channels"] = [dict(spec["channels"][0])]
        spec["channels"][0]["rec_id_size"] = 4
        spec["channels"][0]["record_id"] = 1
        spec["channels"][0]["cg_record_sizes"] = {1: spec["channels"][0]["record_size"]}

        def _boom(*a, **k):
            raise OSError("read fail")

        monkeypatch.setattr("impulse_ds.mdf.arrow_emit.read_raw_data", _boom)
        assert list(convert_spec_to_arrow_batches(spec)) == []

    def test_rle_carry_value_change_at_boundary(self):
        ts = np.array([0.0])
        vs = np.array([3.0])
        closed, _ = _rle_compress_chunk(ts, vs, [1.0, 0.0, 0.0])
        assert closed is not None
        assert closed[2][0] == pytest.approx(1.0)

    def test_emit_prepared_signal_group_paths(self):
        import logging
        import time

        prof = {"decode": 0}
        log = logging.getLogger("t")
        now = time.perf_counter_ns
        ch = {
            "channel_id": 1,
            "channel_type": 0,
            "data_type": FLOAT_LE,
            "bit_count": 64,
            "byte_offset": 0,
            "bit_offset": 0,
            "cn_flags": 0,
            "invalidation_bytes": 0,
            "data_bytes": 8,
            "cc_type": -1,
            "cc_params": [],
        }
        raw = struct.pack("<2d", 1.0, 2.0)
        out = list(
            _emit_prepared_signal_group(
                raw,
                [ch],
                8,
                None,
                1.0,
                lambda t, v, c: [(t, v, c)],
                prof,
                log,
                0,
                now,
            )
        )
        assert len(out) == 1
        assert len(out[0][0]) == 2
        assert out[0][0][0] == pytest.approx(1.0)  # time offset applied
        assert (
            list(
                _emit_prepared_signal_group(
                    b"", [ch], 8, None, 0, lambda *_: [], prof, log, 0, now
                )
            )
            == []
        )
        bad_ch = {**ch, "bit_count": 999}
        assert (
            list(
                _emit_prepared_signal_group(
                    raw,
                    [bad_ch],
                    8,
                    None,
                    0,
                    lambda *_: [],
                    prof,
                    log,
                    0,
                    now,
                )
            )
            == []
        )
        broken = {**ch, "byte_offset": 999}
        assert (
            list(
                _emit_prepared_signal_group(
                    raw,
                    [broken],
                    8,
                    None,
                    0,
                    lambda *_: [],
                    prof,
                    log,
                    0,
                    now,
                )
            )
            == []
        )

    def test_convert_spec_dt_streaming_on_sample(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        org = MDF4Reader(path).scan_channels_organized()
        specs = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=16,
        )
        batches = list(
            convert_spec_to_arrow_batches(specs[0], time_dtype="float32", value_dtype="float32")
        )
        assert batches[0].num_rows > 0

    def test_stripe_decompress_failure_skipped(self, monkeypatch):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_stripes_for_file

        specs = plan_stripes_for_file(path, stripe_target_mb=0.001)

        def _bad_decompress(*a, **k):
            raise ValueError("bad")

        monkeypatch.setattr("impulse_ds.mdf.arrow_emit._decompress_subblock_blob", _bad_decompress)
        assert list(convert_stripe_spec_to_arrow_batches(specs[0])) == []

    def test_convert_spec_extent_error_and_empty_slice(self, monkeypatch):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        org = MDF4Reader(path).scan_channels_organized()
        specs = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=16,
        )
        spec = dict(specs[0])
        spec["row_start"] = 0
        spec["row_end"] = 0

        def _raise(*a, **k):
            raise OSError("extent")

        monkeypatch.setattr("impulse_ds.mdf.arrow_emit.dt_data_extent", _raise)
        assert list(convert_spec_to_arrow_batches(spec)) == []

    def test_master_spec_zero_record_size(self):
        spec = {
            "file_path": "/dev/null",
            "masters": [
                {
                    "group_idx": 0,
                    "record_size": 0,
                    "sample_count": 1,
                    "data_block_addr": 0,
                    "master_info": {"channel_type": 3, "cc_type": -1, "cc_params": []},
                }
            ],
        }
        assert list(convert_master_spec_to_arrow_batches(spec)) == []

    def test_convert_spec_without_master_info(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        org = MDF4Reader(path).scan_channels_organized()
        specs = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=16,
        )
        spec = dict(specs[0])
        ch = dict(spec["channels"][0])
        ch["master_info"] = None
        spec["channels"] = [ch]
        batches = list(convert_spec_to_arrow_batches(spec))
        assert batches[0].num_rows > 0

    def test_convert_spec_rle_flush_and_float32(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        org = MDF4Reader(path).scan_channels_organized()
        specs = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=16,
        )
        batches = list(
            convert_spec_to_arrow_batches(
                specs[0],
                run_length_encoding=True,
                time_dtype="float32",
                value_dtype="float32",
            )
        )
        assert batches

    def test_stripe_unsorted_decompress_failure(self, monkeypatch):
        raw, rec_id_size, cg_sizes, record_size = _interleaved_raw()
        blob = make_dt_block(raw)
        fd, path = tempfile.mkstemp(suffix=".mf4")
        __import__("os").write(fd, blob)
        __import__("os").close(fd)
        try:
            ch = _unsorted_ch_spec(rec_id_size, cg_sizes, record_size, 0)
            spec = {
                "file_path": path,
                "byte_start": 0,
                "byte_end": len(blob),
                "groups": {
                    "0": {
                        "record_size": record_size,
                        "master_info": ch["master_info"],
                        "channels": [ch],
                        "rec_id_size": rec_id_size,
                        "record_id": 1,
                        "cg_record_sizes": cg_sizes,
                    },
                },
                "subblocks": [
                    {
                        "group_idx": 0,
                        "abs_off": 0,
                        "on_disk_len": len(blob),
                        "rec_start": 0,
                        "rec_count": 3,
                    }
                ],
            }

            def _bad(*a, **k):
                raise ValueError("bad")

            monkeypatch.setattr("impulse_ds.mdf.arrow_emit._decompress_subblock_blob", _bad)
            assert list(convert_stripe_spec_to_arrow_batches(spec)) == []
        finally:
            Path(path).unlink(missing_ok=True)

    def test_emit_prepared_skips_vlsd_channel(self):
        import logging
        import time

        prof = {"decode": 0}
        log = logging.getLogger("t")
        now = time.perf_counter_ns
        vlsd = {
            "channel_id": 2,
            "channel_type": 1,
            "data_type": FLOAT_LE,
            "bit_count": 64,
            "byte_offset": 0,
            "bit_offset": 0,
            "cn_flags": 0,
            "invalidation_bytes": 0,
            "data_bytes": 8,
            "cc_type": -1,
            "cc_params": [],
        }
        good = {
            "channel_id": 1,
            "channel_type": 0,
            "data_type": FLOAT_LE,
            "bit_count": 64,
            "byte_offset": 0,
            "bit_offset": 0,
            "cn_flags": 0,
            "invalidation_bytes": 0,
            "data_bytes": 8,
            "cc_type": -1,
            "cc_params": [],
        }
        raw = struct.pack("<d", 3.0)
        master = {
            "channel_type": 2,
            "byte_offset": 0,
            "bit_offset": 0,
            "bit_count": 64,
            "data_type": FLOAT_LE,
            "cc_type": -1,
            "cc_params": [],
        }
        out = list(
            _emit_prepared_signal_group(
                raw,
                [vlsd, good],
                8,
                master,
                0,
                lambda _t, _v, c: [(c,)],
                prof,
                log,
                0,
                now,
            )
        )
        assert (2,) in out and (1,) in out

    def test_convert_spec_dz_fallback_row_slice(self):
        d, files = sample_mdf_dir()
        if len(files) < 2:
            pytest.skip("no compressed sample")
        path = f"{d}/{files[1]}"
        from impulse_ds.mdf.bin_packer import plan_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        org = MDF4Reader(path).scan_channels_organized()
        specs = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=0.00001,
        )
        row_specs = [s for s in specs if "row_start" in s]
        assert row_specs
        batches = list(convert_spec_to_arrow_batches(row_specs[0]))
        assert batches[0].num_rows > 0

    def test_convert_spec_dl_row_range_integration(self):
        records = [struct.pack("<d", float(i)) for i in range(6)]
        blob, dl_addr = build_dl_file(records)
        fd, path = tempfile.mkstemp(suffix=".mf4")
        __import__("os").write(fd, blob)
        __import__("os").close(fd)
        try:
            ch = _unsorted_ch_spec(0, {}, 8, dl_addr)
            ch["sample_count"] = 6
            ch["byte_offset"] = 0
            ch["master_info"] = {
                "byte_offset": 0,
                "bit_offset": 0,
                "bit_count": 64,
                "data_type": FLOAT_LE,
                "channel_type": 3,
                "cc_type": -1,
                "cc_params": [],
            }
            spec = {"file_path": path, "channels": [ch], "row_start": 1, "row_end": 4}
            batches = list(convert_spec_to_arrow_batches(spec))
            assert batches[0].num_rows == 3
        finally:
            Path(path).unlink(missing_ok=True)

    def test_convert_and_stripe_extract_errors(self, monkeypatch):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_partitions, plan_stripes_for_file
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        def _boom(*a, **k):
            raise ValueError("extract failed")

        monkeypatch.setattr("impulse_ds.mdf.arrow_emit.extract_signal", _boom)
        org = MDF4Reader(path).scan_channels_organized()
        spec = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=16,
        )[0]
        assert list(convert_spec_to_arrow_batches(spec)) == []
        stripe = plan_stripes_for_file(path, stripe_target_mb=0.001)[0]
        assert list(convert_stripe_spec_to_arrow_batches(stripe)) == []

    def test_master_virtual_row_slice_with_offset(self):
        spec = {
            "file_path": "/dev/null",
            "time_offset": 2.0,
            "row_start": 1,
            "row_end": 4,
            "masters": [
                {
                    "group_idx": 0,
                    "record_size": 8,
                    "sample_count": 10,
                    "data_block_addr": 0,
                    "master_info": {"channel_type": 3, "cc_type": -1, "cc_params": []},
                }
            ],
        }
        batches = list(convert_master_spec_to_arrow_batches(spec))
        ts = batches[0].column("timestamp").to_pylist()
        assert ts == pytest.approx([3.0, 4.0, 5.0])

    def test_convert_spec_empty_dt_and_zero_row_window(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        org = MDF4Reader(path).scan_channels_organized()
        specs = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=16,
        )
        empty_dt_spec = dict(specs[0])
        empty_dt_spec["channels"] = [dict(empty_dt_spec["channels"][0])]
        empty_dt_spec["channels"][0]["data_block_addr"] = 0
        empty_path = path + ".empty"
        with open(empty_path, "wb") as fh:
            fh.write(make_dt_block(b""))
        empty_dt_spec["file_path"] = empty_path
        assert list(convert_spec_to_arrow_batches(empty_dt_spec)) == []
        zero_window = dict(specs[0])
        zero_window["row_start"] = 5
        zero_window["row_end"] = 5
        assert list(convert_spec_to_arrow_batches(zero_window)) == []
        Path(empty_path).unlink(missing_ok=True)

    def test_make_signal_emitters_empty_and_rle_edges(self, monkeypatch):
        prof = {"arrow": 0, "rows": 0}
        schema = signals_arrow_schema()
        emit_fn, flush_fn = _make_signal_emitters(
            "/f.mf4",
            schema,
            schema.field("time").type,
            schema.field("value").type,
            np.float64,
            np.float64,
            False,
            prof,
        )
        assert list(emit_fn(np.array([]), np.array([]), 1)) == []
        emit_rle, flush_rle = _make_signal_emitters(
            "/f.mf4",
            signals_arrow_schema(run_length_encoding=True),
            schema.field("time").type,
            schema.field("value").type,
            np.float64,
            np.float64,
            True,
            prof,
        )
        empty_closed = (np.array([]), np.array([]), np.array([]))

        def _empty_closed(*a, **k):
            return empty_closed, None

        monkeypatch.setattr(
            "impulse_ds.mdf.arrow_emit._rle_compress_chunk",
            _empty_closed,
        )
        assert list(emit_rle(np.array([0.0]), np.array([1.0]), 1)) == []
        monkeypatch.setattr("impulse_ds.mdf.arrow_emit._rle_flush", lambda _c: None)
        emit_rle2, flush_rle2 = _make_signal_emitters(
            "/f.mf4",
            signals_arrow_schema(run_length_encoding=True),
            schema.field("time").type,
            schema.field("value").type,
            np.float64,
            np.float64,
            True,
            prof,
        )
        monkeypatch.setattr(
            "impulse_ds.mdf.arrow_emit._rle_compress_chunk",
            lambda _ts, _vs, _carry: (None, [1.0, 0.0, 0.0]),
        )
        assert list(emit_rle2(np.array([0.0]), np.array([1.0]), 1)) == []
        assert list(flush_rle2()) == []

    def test_emit_prepared_none_and_exception(self, monkeypatch):
        import logging
        import time

        prof = {"decode": 0}
        log = logging.getLogger("t")
        now = time.perf_counter_ns
        raw = struct.pack("<d", 1.0)
        ch = {
            "channel_id": 1,
            "channel_type": 0,
            "data_type": FLOAT_LE,
            "bit_count": 64,
            "byte_offset": 0,
            "bit_offset": 0,
            "cn_flags": 0,
            "invalidation_bytes": 0,
            "data_bytes": 8,
            "cc_type": -1,
            "cc_params": [],
        }

        def _none(*a, **k):
            return None

        monkeypatch.setattr("impulse_ds.mdf.arrow_emit.extract_signal", _none)
        assert (
            list(
                _emit_prepared_signal_group(
                    raw,
                    [ch],
                    8,
                    None,
                    0,
                    lambda _t, _v, c: [(c,)],
                    prof,
                    log,
                    0,
                    now,
                )
            )
            == []
        )

        def _boom(*a, **k):
            raise RuntimeError("boom")

        monkeypatch.setattr("impulse_ds.mdf.arrow_emit.extract_signal", _boom)
        assert (
            list(
                _emit_prepared_signal_group(
                    raw,
                    [ch],
                    8,
                    None,
                    0,
                    lambda _t, _v, c: [(c,)],
                    prof,
                    log,
                    0,
                    now,
                )
            )
            == []
        )

    def test_dl_row_range_remaining_branches(self, monkeypatch):
        records = [struct.pack("<d", float(i)) for i in range(4)]
        blob, dl_addr = build_dl_file(records)
        fd, path = tempfile.mkstemp(suffix=".mf4")
        __import__("os").write(fd, blob)
        __import__("os").close(fd)
        try:
            base = _unsorted_ch_spec(0, {}, 8, dl_addr)
            base["sample_count"] = 4
            base["byte_offset"] = 0
            empty = dict(base)
            empty["master_info"] = {
                "byte_offset": 0,
                "bit_offset": 0,
                "bit_count": 64,
                "data_type": FLOAT_LE,
                "channel_type": 3,
                "cc_type": -1,
                "cc_params": [],
            }
            assert (
                list(
                    convert_spec_to_arrow_batches(
                        {
                            "file_path": path,
                            "channels": [empty],
                            "row_start": 2,
                            "row_end": 2,
                        }
                    )
                )
                == []
            )
            no_master = dict(base)
            no_master["master_info"] = None
            batches = list(
                convert_spec_to_arrow_batches(
                    {
                        "file_path": path,
                        "channels": [no_master],
                        "row_start": 1,
                        "row_end": 3,
                        "time_offset": 10.0,
                    }
                )
            )
            assert batches[0].num_rows == 2
            assert batches[0].column("time").to_pylist() == pytest.approx([11.0, 12.0])

            def _none_sig(raw_data, record_size, ch_spec):
                return None

            monkeypatch.setattr("impulse_ds.mdf.arrow_emit.extract_signal", _none_sig)
            assert (
                list(
                    convert_spec_to_arrow_batches(
                        {
                            "file_path": path,
                            "channels": [empty],
                            "row_start": 0,
                            "row_end": 2,
                        }
                    )
                )
                == []
            )

            def _boom_sig(raw_data, record_size, ch_spec):
                raise ValueError("extract")

            monkeypatch.setattr("impulse_ds.mdf.arrow_emit.extract_signal", _boom_sig)
            assert (
                list(
                    convert_spec_to_arrow_batches(
                        {
                            "file_path": path,
                            "channels": [empty],
                            "row_start": 0,
                            "row_end": 2,
                        }
                    )
                )
                == []
            )
        finally:
            Path(path).unlink(missing_ok=True)

    def test_dz_fallback_errors_and_empty(self, monkeypatch):
        payload = struct.pack("<d", 1.0)
        dz = make_dz_block(payload)
        fd, path = tempfile.mkstemp(suffix=".mf4")
        __import__("os").write(fd, dz)
        __import__("os").close(fd)
        try:
            ch = _unsorted_ch_spec(0, {}, 8, 0)
            ch["sample_count"] = 1
            ch["byte_offset"] = 0
            ch["master_info"] = None
            spec = {"file_path": path, "channels": [ch]}

            def _read_fail(*a, **k):
                raise OSError("read fail")

            monkeypatch.setattr("impulse_ds.mdf.arrow_emit.read_raw_data", _read_fail)
            assert list(convert_spec_to_arrow_batches(spec)) == []
        finally:
            Path(path).unlink(missing_ok=True)

        fd2, empty_path = tempfile.mkstemp(suffix=".mf4")
        __import__("os").write(fd2, make_dz_block(b""))
        __import__("os").close(fd2)
        try:
            ch2 = _unsorted_ch_spec(0, {}, 8, 0)
            ch2["sample_count"] = 0
            ch2["byte_offset"] = 0
            ch2["master_info"] = None
            assert (
                list(
                    convert_spec_to_arrow_batches(
                        {
                            "file_path": empty_path,
                            "channels": [ch2],
                        }
                    )
                )
                == []
            )
        finally:
            Path(empty_path).unlink(missing_ok=True)

    def test_dt_stream_short_read(self, monkeypatch):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        org = MDF4Reader(path).scan_channels_organized()
        spec = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=16,
        )[0]

        real_open = open

        class ShortRead:
            def __init__(self, fh):
                self._fh = fh

            def read(self, n=-1):
                return b""

            def seek(self, *a, **k):
                return self._fh.seek(*a, **k)

            def __enter__(self):
                return self

            def __exit__(self, *a):
                self._fh.close()

        def _open_short(p, mode="rb"):
            if p == path and "b" in mode:
                return ShortRead(real_open(p, mode))
            return real_open(p, mode)

        monkeypatch.setattr("builtins.open", _open_short)
        assert list(convert_spec_to_arrow_batches(spec)) == []

    def test_dt_stream_none_values(self, monkeypatch):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        from impulse_ds.mdf.bin_packer import plan_partitions
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        org = MDF4Reader(path).scan_channels_organized()
        spec = plan_partitions(
            path,
            org["master_channels"],
            org["signal_channels"],
            org["channel_id_map"],
            target_partition_mb=16,
        )[0]
        monkeypatch.setattr("impulse_ds.mdf.arrow_emit.extract_signal", lambda *_a, **_k: None)
        assert list(convert_spec_to_arrow_batches(spec)) == []

    def test_stripe_short_subblock_and_none_values(self, monkeypatch):
        blob = make_dt_block(b"\x01")
        fd, path = tempfile.mkstemp(suffix=".mf4")
        __import__("os").write(fd, blob)
        __import__("os").close(fd)
        try:
            ch = _unsorted_ch_spec(0, {}, 8, 0)
            ch["byte_offset"] = 0
            spec = {
                "file_path": path,
                "byte_start": 0,
                "byte_end": len(blob),
                "groups": {
                    "0": {
                        "record_size": 8,
                        "master_info": None,
                        "channels": [ch],
                        "rec_id_size": 0,
                    },
                },
                "subblocks": [
                    {
                        "group_idx": 0,
                        "abs_off": 0,
                        "on_disk_len": len(blob),
                        "rec_start": 0,
                        "rec_count": 1,
                    }
                ],
            }
            assert list(convert_stripe_spec_to_arrow_batches(spec)) == []
            monkeypatch.setattr(
                "impulse_ds.mdf.arrow_emit.extract_signal",
                lambda *_a, **_k: None,
            )
            assert list(convert_stripe_spec_to_arrow_batches(spec)) == []
        finally:
            Path(path).unlink(missing_ok=True)

    def test_master_virtual_empty_window(self):
        spec = {
            "file_path": "/dev/null",
            "row_start": 5,
            "row_end": 5,
            "masters": [
                {
                    "group_idx": 0,
                    "record_size": 8,
                    "sample_count": 10,
                    "data_block_addr": 0,
                    "master_info": {"channel_type": 3, "cc_type": -1, "cc_params": []},
                }
            ],
        }
        assert list(convert_master_spec_to_arrow_batches(spec)) == []


class TestMdf4ReaderMoreCoverage:
    def test_hd_invalid_and_timezone(self):
        with pytest.raises(ValueError, match="Not a valid MDF"):
            MDF4Reader(file_bytes=b"BAD")._read_hd_start_utc_ns()
        blob = bytearray(128)
        blob[0:8] = b"MDF     "
        blob[64:68] = b"##XX"
        with pytest.raises(ValueError, match="Expected HD block"):
            MDF4Reader(file_bytes=bytes(blob))._read_hd_start_utc_ns()
        blob[64:68] = BLOCK_ID_HD
        struct.pack_into("<Q", blob, 72, 120)
        struct.pack_into("<Q", blob, 80, 0)
        start_ns = 3_600_000_000_000_000_000
        struct.pack_into("<Q", blob, 88, start_ns)
        struct.pack_into("<h", blob, 96, 60)
        struct.pack_into("<h", blob, 98, 0)
        struct.pack_into("<B", blob, 100, 0x03)
        reader = MDF4Reader(file_bytes=bytes(blob))
        assert reader._read_hd_start_utc_ns() == start_ns - 60 * 60 * 1_000_000_000

    def test_text_block_edge_cases(self):
        empty = b"##TX" + b"\x00" * 4 + struct.pack("<Q", 24) + struct.pack("<Q", 0)
        reader1 = MDF4Reader(file_bytes=b"MDF     " + b"\x00" * 56)
        with io.BytesIO(b"\x00" * 8 + empty) as f:
            assert reader1._read_text_block(f, 8) == ""
        bad = b"##ZZ" + b"\x00" * 20
        reader2 = MDF4Reader(file_bytes=b"MDF     " + b"\x00" * 56)
        with io.BytesIO(b"\x00" * 8 + bad) as f:
            assert reader2._read_text_block(f, 8) == ""
        latin = b"##TX" + b"\x00" * 4 + struct.pack("<Q", 25) + struct.pack("<Q", 0) + b"\xe9"
        reader3 = MDF4Reader(file_bytes=b"MDF     " + b"\x00" * 56)
        with io.BytesIO(b"\x00" * 8 + latin) as f:
            assert reader3._read_text_block(f, 8) == "\xe9"

    def test_read_channel_data_empty_paths(self):
        empty = MDF4Reader.read_channel_data(
            "/dev/null",
            0,
            0,
            0,
            0,
            64,
            FLOAT_LE,
            0,
            0,
            rec_id_size=4,
            record_id=1,
            cg_record_sizes={1: 8},
        )
        assert len(empty) == 0
        blob = make_dt_block(struct.pack("<d", 1.0))
        fd, path = tempfile.mkstemp(suffix=".mf4")
        __import__("os").write(fd, blob)
        __import__("os").close(fd)
        try:
            vlsd = MDF4Reader.read_channel_data(
                path,
                0,
                8,
                0,
                0,
                64,
                FLOAT_LE,
                1,
                1,
            )
            assert np.isnan(vlsd[0])
        finally:
            Path(path).unlink(missing_ok=True)
