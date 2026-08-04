"""Unit tests for impulse_data_sources.mdf.mdf_decode."""

import struct

import numpy as np
import pytest

from impulse_data_sources.mdf.mdf_decode import (
    CN_FLAG_ALL_INVALID,
    CN_FLAG_INVALIDATION_PRESENT,
    CC_RANGE_TO_VALUE,
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
    read_record_id,
)
from ._block_fixtures import build_dl_file, make_dt_block


def _build_interleaved_block():
    """Two CGs interleaved: rec_id_size=4, record_size=20."""
    rec_id_size = 4
    record_size = 20
    cg_sizes = {1: record_size, 2: record_size}
    r1 = struct.pack("<I", 1) + struct.pack("<d", 1.0) + struct.pack("<d", 10.0)
    r2 = struct.pack("<I", 2) + struct.pack("<d", 2.0) + struct.pack("<d", 20.0)
    return b"".join([r1, r2, r1]), rec_id_size, cg_sizes, record_size


class TestConvertValues:
    @pytest.mark.parametrize(
        "data_type,bit_count,bit_offset,raw,expected",
        [
            (FLOAT_LE, 64, 0, struct.pack("<2d", 1.5, 2.5), [1.5, 2.5]),
            (FLOAT_LE, 32, 0, struct.pack("<2f", 1.0, 2.0), [1.0, 2.0]),
            (FLOAT_BE, 64, 0, struct.pack(">2d", 1.0, 2.0), [1.0, 2.0]),
            (UINT_LE, 16, 0, struct.pack("<2H", 100, 200), [100.0, 200.0]),
            (UINT_BE, 16, 0, struct.pack(">2H", 300, 400), [300.0, 400.0]),
            (UINT_LE, 32, 0, struct.pack("<2I", 1, 42), [1.0, 42.0]),
            (UINT_BE, 32, 0, struct.pack(">2I", 5, 6), [5.0, 6.0]),
            (SINT_LE, 32, 0, struct.pack("<2i", -1, 42), [-1.0, 42.0]),
            (SINT_BE, 32, 0, struct.pack(">2i", -5, 7), [-5.0, 7.0]),
            (UINT_LE, 8, 0, bytes([1, 255]), [1.0, 255.0]),
        ],
    )
    def test_known_types(self, data_type, bit_count, bit_offset, raw, expected):
        n = len(expected)
        vb = (bit_count + 7) // 8
        col = np.frombuffer(raw, dtype=np.uint8).reshape(n, len(raw) // n)
        if col.shape[1] != vb:
            col = col[:, :vb]
        out = convert_values(col, data_type, bit_count, bit_offset, n)
        np.testing.assert_allclose(out, expected, rtol=1e-5)

    @pytest.mark.parametrize(
        "data_type,bit_count,col,expected",
        [
            (UINT_BE, 24, [[0, 0, 1], [0, 1, 0]], [256.0, 65536.0]),
            (UINT_BE, 48, [[0, 0, 0, 0, 0, 1], [0, 0, 0, 0, 1, 0]], [65536.0, 16777216.0]),
            (SINT_BE, 24, [[0xFF, 0xFF, 0xFF], [0, 1, 0]], [-256.0, 65536.0]),
        ],
    )
    def test_padded_big_endian_widths(self, data_type, bit_count, col, expected):
        arr = np.array(col, dtype=np.uint8)
        out = convert_values(arr, data_type, bit_count, 0, len(expected))
        np.testing.assert_allclose(out, expected, rtol=1e-5)

    def test_unsupported_type_returns_zeros(self):
        col = np.zeros((3, 8), dtype=np.uint8)
        out = convert_values(col, 99, 64, 0, 3)
        np.testing.assert_array_equal(out, [0.0, 0.0, 0.0])

    def test_uint_with_bit_offset(self):
        # 12-bit values 15 and 255 stored in uint16 with 4-bit offset
        col = np.array([[0xF0, 0x00], [0xFF, 0x0F]], dtype=np.uint8)
        out = convert_values(col, UINT_LE, 12, 4, 2)
        np.testing.assert_allclose(out, [15.0, 255.0])


class TestApplyInvalidation:
    def test_invalidation_bit_sets_nan(self):
        record_size = 9
        n = 2
        buf = bytearray(n * record_size)
        # data byte 0, invalidation at data_bytes=8 byte 0 bit 0
        buf[0] = 10
        buf[record_size] = 20
        buf[8] = 0x01  # first record invalid
        values = np.array([10.0, 20.0])
        info = {
            "cn_flags": CN_FLAG_INVALIDATION_PRESENT,
            "invalidation_bit_pos": 0,
            "invalidation_bytes": 1,
            "data_bytes": 8,
        }
        out = apply_invalidation(buf, record_size, n, values.copy(), info)
        assert np.isnan(out[0])
        assert out[1] == 20.0

    def test_all_invalid_flag(self):
        values = np.array([1.0, 2.0])
        info = {"cn_flags": CN_FLAG_ALL_INVALID, "invalidation_bytes": 1, "data_bytes": 8}
        out = apply_invalidation(b"\x00" * 18, 9, 2, values.copy(), info)
        assert np.all(np.isnan(out))


class TestApplyCcConversionExtended:
    def test_tab_nointerp(self):
        raw = np.array([10.0, 120.0, 250.0])
        params = (0.0, 0.0, 100.0, 50.0, 200.0, 100.0)
        out = apply_cc_conversion(raw.copy(), CC_TAB_NOINTERP, params)
        np.testing.assert_allclose(out, [0.0, 50.0, 100.0])

    def test_range_to_value(self):
        raw = np.array([5.0, 15.0, 25.0])
        params = (0.0, 10.0, 1.0, 10.0, 20.0, 2.0, 99.0)
        out = apply_cc_conversion(raw.copy(), CC_RANGE_TO_VALUE, params)
        np.testing.assert_allclose(out, [1.0, 2.0, 99.0])

    def test_range_empty_returns_default(self):
        raw = np.array([1.0])
        out = apply_cc_conversion(raw.copy(), CC_RANGE_TO_VALUE, (99.0,))
        assert out[0] == 99.0


class TestFilterUnsortedRecords:
    def test_unknown_record_id_stops(self):
        raw, rec_id_size, cg_sizes, record_size = _build_interleaved_block()
        # Append garbage record id not in cg_sizes
        bad = struct.pack("<I", 99) + b"\x00" * (record_size - 4)
        truncated = raw + bad
        filtered = filter_unsorted_records(truncated, rec_id_size, 1, cg_sizes)
        assert len(filtered) == 2 * record_size

    def test_unsupported_rec_id_size_raises(self):
        with pytest.raises(ValueError, match="Unsupported rec_id_size"):
            read_record_id(b"\x00", 0, 3)


class TestExtractSignal:
    def test_vlsd_returns_nan(self):
        n = 3
        record_size = 8
        raw = b"\x00" * (n * record_size)
        ch = {
            "channel_type": 1,
            "data_type": FLOAT_LE,
            "bit_count": 64,
            "byte_offset": 0,
            "bit_offset": 0,
        }
        vals = extract_signal(raw, record_size, ch)
        assert len(vals) == n
        assert np.all(np.isnan(vals))

    def test_invalidation_and_cc_combined(self):
        record_size = 16
        n = 2
        raw = bytearray(n * record_size)
        struct.pack_into("<d", raw, 0, 1.0)
        struct.pack_into("<d", raw, record_size, 2.0)
        raw[8] = 0x01
        ch = {
            "channel_type": 0,
            "data_type": FLOAT_LE,
            "bit_count": 64,
            "byte_offset": 0,
            "bit_offset": 0,
            "cn_flags": CN_FLAG_INVALIDATION_PRESENT,
            "invalidation_bit_pos": 0,
            "invalidation_bytes": 1,
            "data_bytes": 8,
            "cc_type": 1,
            "cc_params": (10.0, 2.0),
        }
        vals = extract_signal(bytes(raw), record_size, ch)
        assert np.isnan(vals[0])
        assert vals[1] == pytest.approx(14.0)


class TestExtractTimestamps:
    def test_non_fast_path_with_cc(self):
        record_size = 8
        n = 3
        raw = b"".join(struct.pack("<d", float(i)) for i in range(n))
        master = {
            "channel_type": 2,
            "byte_offset": 1,  # unaligned -> slow path
            "bit_offset": 0,
            "bit_count": 64,
            "data_type": FLOAT_LE,
            "cc_type": 1,
            "cc_params": (0.0, 10.0),
        }
        ts = extract_timestamps(raw, record_size, master)
        assert len(ts) == n
