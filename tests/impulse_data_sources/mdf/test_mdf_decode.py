"""Unit tests for impulse_data_sources.mdf.mdf_decode."""

import struct

import numpy as np
import pytest

from impulse_data_sources.mdf.mdf_decode import (
    CN_FLAG_ALL_INVALID,
    CN_FLAG_INVALIDATION_PRESENT,
    CC_LINEAR,
    CC_RANGE_TO_VALUE,
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
    extract_column_strided,
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
            (UINT_BE, 24, [[0, 0, 1], [0, 1, 0]], [1.0, 256.0]),
            (UINT_BE, 48, [[0, 0, 0, 0, 0, 1], [0, 0, 0, 0, 1, 0]], [1.0, 256.0]),
            (SINT_BE, 24, [[0xFF, 0xFF, 0xFF], [0, 1, 0]], [-1.0, 256.0]),
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

    @staticmethod
    def _pack(value, bit_count, bit_offset, big_endian, signed=False):
        """Encode one value at bit_offset into its byte span (asammdf semantics)."""
        n_bytes = (bit_offset + bit_count + 7) // 8
        if signed and value < 0:
            value += 1 << bit_count
        word = value << bit_offset
        return list(word.to_bytes(n_bytes, "big" if big_endian else "little"))

    @pytest.mark.parametrize("data_type", [SINT_LE, SINT_BE])
    @pytest.mark.parametrize(
        "bit_count,bit_offset,values",
        [
            (64, 0, [-(1 << 63), -1, (1 << 63) - 1, 0]),  # review case 0x8000...
            (64, 4, [-(1 << 63), -1, (1 << 63) - 1]),  # 9-byte span
            (63, 0, [-(1 << 62), -1, (1 << 62) - 1]),  # overflowed under NumPy 2
            (63, 5, [-(1 << 62), -1, (1 << 62) - 1]),
            (24, 0, [-(1 << 23), -1, (1 << 23) - 1]),
            (1, 0, [-1, 0]),
        ],
    )
    def test_signed_extremes(self, data_type, bit_count, bit_offset, values):
        big_endian = data_type == SINT_BE
        rows = [self._pack(v, bit_count, bit_offset, big_endian, signed=True) for v in values]
        col = np.array(rows, dtype=np.uint8)
        out = convert_values(col, data_type, bit_count, bit_offset, len(values))
        np.testing.assert_array_equal(out, np.array(values, dtype=np.float64))

    @pytest.mark.parametrize(
        "data_type,bit_count,bit_offset,values",
        [
            (UINT_LE, 8, 4, [200, 1, 255]),  # review example: 200 must not decode to 12
            (UINT_LE, 12, 6, [4095, 1234, 0]),  # span 18 bits -> 3 bytes
            (UINT_LE, 20, 7, [(1 << 20) - 1, 54321]),  # span 27 bits -> 4 bytes
            (UINT_LE, 33, 3, [(1 << 33) - 1, 123456789]),  # span 36 bits -> 5 bytes
            (SINT_LE, 8, 3, [-100, 127, -128]),
            (SINT_LE, 12, 5, [-2048, 2047, -1]),
            (UINT_BE, 8, 4, [200, 1, 255]),
            (UINT_BE, 12, 6, [4095, 1234, 0]),
            (SINT_BE, 12, 5, [-2048, 2047, -1]),
            (UINT_LE, 64, 4, [(1 << 63) + 12345, 7]),  # 9-byte span
            (UINT_LE, 62, 3, [(1 << 62) - 1, 99]),  # 9-byte span
            (UINT_BE, 60, 7, [(1 << 60) - 1, 42]),  # 9-byte span, big-endian
            (SINT_LE, 64, 2, [-5, 1 << 40]),  # 9-byte span, signed 64-bit
        ],
    )
    def test_bit_offset_spanning_extra_byte(self, data_type, bit_count, bit_offset, values):
        big_endian = data_type in (UINT_BE, SINT_BE)
        signed = data_type in (SINT_LE, SINT_BE)
        rows = [self._pack(v, bit_count, bit_offset, big_endian, signed) for v in values]
        col = np.array(rows, dtype=np.uint8)
        out = convert_values(col, data_type, bit_count, bit_offset, len(values))
        np.testing.assert_array_equal(out, np.array(values, dtype=np.float64))

    def test_too_narrow_column_raises(self):
        # UINT_LE 16-bit at bit_offset 4 spans 3 bytes; the old width (2) is rejected.
        col = np.zeros((2, 2), dtype=np.uint8)
        with pytest.raises(ValueError, match="needs >= 3 bytes per row"):
            convert_values(col, UINT_LE, 16, 4, 2)

    def test_extract_column_strided_bounds(self):
        raw = bytes(range(12))  # 3 records of 4 bytes
        np.testing.assert_array_equal(
            extract_column_strided(raw, 4, 2, 2, 3), [[2, 3], [6, 7], [10, 11]]
        )
        with pytest.raises(ValueError, match="exceed record_size 4"):
            extract_column_strided(raw, 4, 3, 2, 3)
        with pytest.raises(ValueError, match="too small"):
            extract_column_strided(raw, 4, 0, 2, 4)


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

    @pytest.mark.parametrize(
        "cc_type,cc_params,expected_valid",
        [
            (CC_LINEAR, (1.0, 2.0), 3.0),
            (CC_RATIONAL, (0.0, 1.0, 0.0, 0.0, 0.0, 1.0), 1.0),
            (CC_TAB_INTERP, (1.0, 20.0, 2.0, 30.0), 20.0),
            (CC_TAB_NOINTERP, (1.0, 20.0, 2.0, 30.0), 20.0),  # NaN used to become 30
            (CC_RANGE_TO_VALUE, (0.0, 5.0, 7.0, -1.0), 7.0),  # NaN used to become -1
            (CC_RANGE_TO_VALUE, (-1.0,), -1.0),  # no ranges, default only
        ],
    )
    def test_nan_preserved(self, cc_type, cc_params, expected_valid):
        out = apply_cc_conversion(np.array([np.nan, 1.0]), cc_type, cc_params)
        assert np.isnan(out[0])
        assert out[1] == pytest.approx(expected_valid)


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

    @pytest.mark.parametrize(
        "cc_type,cc_params,expected_valid",
        [
            (CC_TAB_NOINTERP, (1.0, 20.0, 2.0, 30.0), 30.0),
            (CC_RANGE_TO_VALUE, (0.0, 1.5, 7.0, 1.5, 5.0, 8.0, -1.0), 8.0),
        ],
    )
    def test_invalidation_survives_table_conversion(self, cc_type, cc_params, expected_valid):
        # Two records: 8-byte float value + 1 invalidation byte. Record 0 is invalid.
        record_size = 9
        raw = bytearray(2 * record_size)
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
            "cc_type": cc_type,
            "cc_params": cc_params,
        }
        vals = extract_signal(bytes(raw), record_size, ch)
        assert np.isnan(vals[0])
        assert vals[1] == pytest.approx(expected_valid)

    def test_packed_record_with_bit_offsets(self):
        # One 4-byte record per sample holding three bit-packed LE signals:
        #   a: 8 bits at byte 0, bit 4  (crosses into byte 1)
        #   b: 6 bits at byte 1, bit 4  (crosses into byte 2)
        #   c: 10 bits at byte 2, bit 2 (bits 18..27 of the record)
        record_size = 4
        samples = [(200, 45, 1000), (17, 63, 1), (255, 0, 1023)]
        raw = b"".join(
            ((a << 4) | (b << 12) | (c << 18)).to_bytes(record_size, "little")
            for a, b, c in samples
        )
        specs = {
            "a": (0, 4, 8),
            "b": (1, 4, 6),
            "c": (2, 2, 10),
        }
        for idx, (byte_offset, bit_offset, bit_count) in enumerate(specs.values()):
            ch = {
                "channel_type": 0,
                "data_type": UINT_LE,
                "bit_count": bit_count,
                "byte_offset": byte_offset,
                "bit_offset": bit_offset,
            }
            vals = extract_signal(raw, record_size, ch)
            np.testing.assert_array_equal(vals, [float(s[idx]) for s in samples])

    def test_field_past_record_end_raises(self):
        record_size = 2
        raw = bytes([0xFF, 0xFF]) * 3
        ch = {
            "channel_type": 0,
            "data_type": UINT_LE,
            "bit_count": 16,
            "byte_offset": 0,
            "bit_offset": 4,  # span 20 bits -> 3 bytes > record_size
        }
        with pytest.raises(ValueError, match="exceed record_size 2"):
            extract_signal(raw, record_size, ch)

    @pytest.mark.parametrize("bit_count,fmt", [(64, "<d"), (32, "<f")])
    def test_float_with_bit_offset_decodes(self, bit_count, fmt):
        # Regression: floats must not get the integer bit-packed span. A
        # non-zero bit_offset on a float is ignored (MDF4 floats are byte-aligned).
        width = bit_count // 8
        record_size = 1 + width
        vals_in = [1.5, -2.25]
        raw = b"".join(b"\x00" + struct.pack(fmt, v) for v in vals_in)
        ch = {
            "channel_type": 0,
            "data_type": FLOAT_LE,
            "bit_count": bit_count,
            "byte_offset": 1,
            "bit_offset": 1,
        }
        np.testing.assert_allclose(extract_signal(raw, record_size, ch), vals_in)

    @pytest.mark.parametrize("bit_offset", [8, 12, -1])
    def test_bit_offset_outside_mdf4_range_raises(self, bit_offset):
        ch = {
            "channel_type": 0,
            "data_type": UINT_LE,
            "bit_count": 64,
            "byte_offset": 0,
            "bit_offset": bit_offset,
        }
        with pytest.raises(ValueError, match="outside MDF4 range 0..7"):
            extract_signal(b"\xff" * 20, 10, ch)

    def test_invalidation_bit_outside_record_raises(self):
        # invalidation_bit_pos 64 -> byte 8 after data_bytes 8 = column 16,
        # beyond record_size 9: must raise, not read past the buffer.
        record_size = 9
        raw = bytearray(2 * record_size)
        ch = {
            "channel_type": 0,
            "data_type": FLOAT_LE,
            "bit_count": 64,
            "byte_offset": 0,
            "bit_offset": 0,
            "cn_flags": CN_FLAG_INVALIDATION_PRESENT,
            "invalidation_bit_pos": 64,
            "invalidation_bytes": 1,
            "data_bytes": 8,
        }
        with pytest.raises(ValueError, match="exceed record_size 9"):
            extract_signal(bytes(raw), record_size, ch)


class TestExtractTimestamps:
    def test_non_fast_path_with_cc(self):
        record_size = 9
        n = 3
        raw = b"".join(b"\x00" + struct.pack("<d", float(i)) for i in range(n))
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
        np.testing.assert_allclose(ts, [0.0, 10.0, 20.0])

    def test_malformed_master_raises(self):
        master = {
            "channel_type": 2,
            "byte_offset": 4,
            "bit_offset": 0,
            "bit_count": 64,
            "data_type": FLOAT_LE,
        }
        with pytest.raises(ValueError, match="master channel"):
            extract_timestamps(b"\x00" * 16, 8, master)

    def test_uint_master_with_bit_offset(self):
        # 16-bit LE counter master at bit_offset 4 spans 3 bytes.
        record_size = 3
        ticks = [0, 1000, 65535]
        raw = b"".join((t << 4).to_bytes(record_size, "little") for t in ticks)
        master = {
            "channel_type": 2,
            "byte_offset": 0,
            "bit_offset": 4,
            "bit_count": 16,
            "data_type": UINT_LE,
        }
        ts = extract_timestamps(raw, record_size, master)
        np.testing.assert_array_equal(ts, [0.0, 1000.0, 65535.0])
