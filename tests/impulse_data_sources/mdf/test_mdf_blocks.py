"""Unit tests for impulse_data_sources.mdf.mdf_blocks binary I/O."""

import io
import logging
import struct

import numpy as np
import pytest

from impulse_data_sources.mdf.mdf_blocks import (
    _collect_dl_block_addrs,
    _read_block_chunks,
    _read_dl_blob,
    decompress_dz,
    dt_data_extent,
    parse_subblocks,
    read_data_list_range,
    read_raw_data,
    resolve_dl_addr,
)
from ._block_fixtures import (
    build_dl_file,
    cyclic_dl_file,
    make_dt_block,
    make_dz_block,
    make_unknown_block,
)
from ._mdf_samples import sample_mdf_dir


class TestDtDzBlocks:
    def test_read_raw_data_dt(self):
        payload = struct.pack("<4d", 1.0, 2.0, 3.0, 4.0)
        blob = make_dt_block(payload)
        with io.BytesIO(blob) as f:
            raw = read_raw_data(f, 0, record_size=8, sample_count=4)
        assert raw == payload
        assert len(raw) == 32

    def test_dt_data_extent(self):
        payload = b"\xab" * 16
        blob = make_dt_block(payload)
        with io.BytesIO(blob) as f:
            extent = dt_data_extent(f, 0)
        assert extent == (24, 16)

    def test_read_raw_data_dz_plain(self):
        payload = struct.pack("<2d", 3.14, 2.71)
        blob = make_dz_block(payload, zip_type=0)
        with io.BytesIO(blob) as f:
            raw = read_raw_data(f, 0, record_size=8, sample_count=2)
        assert raw == payload

    def test_decompress_dz_transpose_even(self):
        # 2x3 row-major payload -> zip_type=1 with cols=3
        payload = bytes(range(6))
        blob = make_dz_block(payload, zip_type=1, zip_parameter=3)
        with io.BytesIO(blob) as f:
            out = decompress_dz(f, 0)
        assert out == payload

    def test_decompress_dz_transpose_remainder(self):
        # 10 bytes, cols=3 -> uneven column layout branch
        payload = bytes(range(10))
        blob = make_dz_block(payload, zip_type=1, zip_parameter=3)
        with io.BytesIO(blob) as f:
            out = decompress_dz(f, 0)
        assert out == payload

    def test_read_raw_data_dz_from_sample_b(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[1]}"  # sample_b compressed
        from impulse_data_sources.mdf.mdf4_reader import MDF4Reader

        ch = MDF4Reader(path).scan_metadata()[0]
        with open(path, "rb") as f:
            raw = read_raw_data(f, ch.data_block_addr, ch.record_size, ch.sample_count)
        assert len(raw) == ch.record_size * ch.sample_count


class TestDlHlBlocks:
    def test_resolve_dl_addr_variants(self):
        p1 = struct.pack("<d", 1.0)
        p2 = struct.pack("<d", 2.0)
        blob, dl_addr = build_dl_file([p1, p2], wrap_hl=False)
        with io.BytesIO(blob) as f:
            assert resolve_dl_addr(f, dl_addr) == dl_addr
        blob_hl, hl_addr = build_dl_file([p1, p2], wrap_hl=True)
        with io.BytesIO(blob_hl) as f:
            assert resolve_dl_addr(f, hl_addr) is not None
            assert resolve_dl_addr(f, 0) is None

    def test_read_raw_data_dl_chain(self):
        rec = struct.pack("<d", 1.5) + struct.pack("<d", 2.5)
        blob, dl_addr = build_dl_file([rec[:8], rec[8:]])
        with io.BytesIO(blob) as f:
            raw = read_raw_data(f, dl_addr, record_size=8, sample_count=2)
        assert raw == rec

    def test_read_raw_data_hl_wrapper(self):
        rec = struct.pack("<2d", 1.0, 2.0)
        blob, hl_addr = build_dl_file([rec[:8], rec[8:]], wrap_hl=True)
        with io.BytesIO(blob) as f:
            raw = read_raw_data(f, hl_addr, record_size=8, sample_count=2)
        assert raw == rec

    def test_read_data_list_range_partial(self):
        records = [struct.pack("<d", float(i)) for i in range(5)]
        blob, dl_addr = build_dl_file(records)
        with io.BytesIO(blob) as f:
            raw, start = read_data_list_range(f, dl_addr, 8, 1, 4)
        assert start == 1
        assert len(raw) == 3 * 8
        vals = np.frombuffer(raw, dtype=np.float64)
        np.testing.assert_allclose(vals, [1.0, 2.0, 3.0])

    def test_collect_dl_cyclic_guard(self):
        blob, dl_addr = cyclic_dl_file()
        with io.BytesIO(blob) as f:
            addrs = _collect_dl_block_addrs(f, dl_addr)
        assert len(addrs) == 1

    def test_parse_subblocks_dt_dz_dl_hl(self):
        dt_blob = make_dt_block(struct.pack("<2d", 1.0, 2.0))
        with io.BytesIO(dt_blob) as f:
            subs = parse_subblocks(f, 0, 8, 2)
        assert subs == [(0, len(dt_blob), 0, 2)]

        dz_blob = make_dz_block(struct.pack("<2d", 3.0, 4.0))
        with io.BytesIO(dz_blob) as f:
            subs = parse_subblocks(f, 0, 8, 2)
        assert subs[0][3] == 2

        p1, p2 = struct.pack("<d", 1.0), struct.pack("<d", 2.0)
        dl_blob, dl_addr = build_dl_file([p1, p2])
        with io.BytesIO(dl_blob) as f:
            subs = parse_subblocks(f, dl_addr, 8, 2)
        assert len(subs) == 2
        assert sum(s[3] for s in subs) == 2

        hl_blob, hl_addr = build_dl_file([p1, p2], wrap_hl=True)
        with io.BytesIO(hl_blob) as f:
            subs_hl = parse_subblocks(f, hl_addr, 8, 2)
        assert len(subs_hl) == 2


class TestReadBlockChunks:
    def test_dt_streaming_with_row_range(self):
        payload = b"".join(struct.pack("<d", float(i)) for i in range(10))
        blob = make_dt_block(payload)
        prof = {"read": 0}
        log = logging.getLogger("test")
        with io.BytesIO(blob) as f:
            chunks = list(
                _read_block_chunks(
                    f,
                    0,
                    8,
                    10,
                    row_start=2,
                    row_end=5,
                    prof=prof,
                    log=log,
                )
            )
        assert len(chunks) == 1
        raw, start = chunks[0]
        assert start == 2
        assert len(raw) == 3 * 8

    def test_dl_row_range_chunks(self):
        records = [struct.pack("<d", float(i)) for i in range(4)]
        blob, dl_addr = build_dl_file(records)
        prof = {"read": 0}
        log = logging.getLogger("test")
        with io.BytesIO(blob) as f:
            chunks = list(
                _read_block_chunks(
                    f,
                    dl_addr,
                    8,
                    4,
                    row_start=1,
                    row_end=3,
                    prof=prof,
                    log=log,
                )
            )
        assert len(chunks) == 1
        raw, start = chunks[0]
        assert start == 1
        np.testing.assert_allclose(
            np.frombuffer(raw, dtype=np.float64),
            [1.0, 2.0],
        )


class TestEdgeBlockHandling:
    def test_parse_subblocks_unknown_block_type(self):
        blob = make_unknown_block(b"\xab\xcd")
        with io.BytesIO(blob) as f:
            subs = parse_subblocks(f, 0, 8, 5)
        assert subs == [(0, len(blob), 0, 5)]

    def test_read_dl_blob_truncated_header(self):
        blob, dl_addr = build_dl_file([struct.pack("<d", 1.0)])
        # DT sub-block starts at offset 8; truncate before its 16-byte header is readable.
        truncated = blob[:20]
        with io.BytesIO(truncated) as f:
            out, lo, addrs = _read_dl_blob(f, dl_addr)
        assert out == b""
        assert lo == 0
        assert addrs == []
