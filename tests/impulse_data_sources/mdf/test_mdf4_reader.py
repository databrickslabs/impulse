"""
Tests for the MDF4 binary reader.

Validates metadata scanning and data extraction against MDF4 files generated on
the fly with asammdf (_mdf_samples.py) — no pre-built fixtures required.
This also cross-validates our reader against asammdf's own output.
"""

import io
import os
import pytest
import numpy as np

from impulse_data_sources.mdf.mdf4_reader import MDF4Reader, CN_TYPE_MASTER, CN_TYPE_VIRTUAL_MASTER
from ._mdf_samples import sample_mdf_dir, START_TIME

_DIR, _FILES = sample_mdf_dir()
EXAMPLE_DIR = _DIR
EXAMPLE_FILES = [os.path.join(_DIR, f) for f in _FILES]  # full paths


@pytest.fixture(
    params=EXAMPLE_FILES if EXAMPLE_FILES else [],
    ids=[os.path.basename(p) for p in EXAMPLE_FILES],
)
def mdf_file(request):
    return request.param


class TestMDF4Reader:
    def test_scan_metadata_returns_channels(self, mdf_file):
        reader = MDF4Reader(mdf_file)
        channels = reader.scan_metadata()
        assert len(channels) > 0, f"No channels found in {mdf_file}"

    def test_channels_have_valid_fields(self, mdf_file):
        reader = MDF4Reader(mdf_file)
        channels = reader.scan_metadata()
        for ch in channels:
            assert ch.group_idx >= 0
            assert ch.channel_idx >= 0
            assert ch.sample_count >= 0
            assert ch.bit_count >= 0  # virtual masters can have bit_count=0
            assert ch.record_size > 0
            assert ch.data_block_addr > 0

    def test_has_master_channels(self, mdf_file):
        reader = MDF4Reader(mdf_file)
        channels = reader.scan_metadata()
        masters = [
            ch for ch in channels if ch.channel_type in (CN_TYPE_MASTER, CN_TYPE_VIRTUAL_MASTER)
        ]
        assert len(masters) > 0, "No master (time) channels found"

    def test_read_channel_data(self, mdf_file):
        reader = MDF4Reader(mdf_file)
        channels = reader.scan_metadata()

        # Find a signal channel with data
        signal_ch = None
        for ch in channels:
            if (
                ch.channel_type not in (CN_TYPE_MASTER, CN_TYPE_VIRTUAL_MASTER)
                and ch.sample_count > 0
            ):
                signal_ch = ch
                break

        if signal_ch is None:
            pytest.skip("No signal channels with data")

        values = MDF4Reader.read_channel_data(
            mdf_file,
            signal_ch.data_block_addr,
            signal_ch.record_size,
            signal_ch.byte_offset,
            signal_ch.bit_offset,
            signal_ch.bit_count,
            signal_ch.data_type,
            signal_ch.channel_type,
            signal_ch.sample_count,
        )

        assert len(values) > 0
        assert values.dtype == np.float64
        assert not np.all(np.isnan(values))

    def test_read_channel_pair(self, mdf_file):
        reader = MDF4Reader(mdf_file)
        channels = reader.scan_metadata()

        # Find master and signal in same group
        masters = {}
        for ch in channels:
            if ch.channel_type in (CN_TYPE_MASTER, CN_TYPE_VIRTUAL_MASTER):
                masters[ch.group_idx] = ch

        signal_ch = None
        for ch in channels:
            if (
                ch.channel_type not in (CN_TYPE_MASTER, CN_TYPE_VIRTUAL_MASTER)
                and ch.sample_count > 0
                and ch.group_idx in masters
            ):
                signal_ch = ch
                break

        if signal_ch is None:
            pytest.skip("No suitable channel pair found")

        master = masters[signal_ch.group_idx]
        master_info = {
            "byte_offset": master.byte_offset,
            "bit_offset": master.bit_offset,
            "bit_count": master.bit_count,
            "data_type": master.data_type,
            "channel_type": master.channel_type,
            "cc_type": master.cc_type,
            "cc_params": list(master.cc_params) if master.cc_params else [],
        }
        signal_info = {
            "data_block_addr": signal_ch.data_block_addr,
            "record_size": signal_ch.record_size,
            "byte_offset": signal_ch.byte_offset,
            "bit_offset": signal_ch.bit_offset,
            "bit_count": signal_ch.bit_count,
            "data_type": signal_ch.data_type,
            "channel_type": signal_ch.channel_type,
            "cc_type": signal_ch.cc_type,
            "cc_params": list(signal_ch.cc_params) if signal_ch.cc_params else [],
        }

        timestamps, values = MDF4Reader.read_channel_pair(
            mdf_file, master_info, signal_info, signal_ch.sample_count
        )

        assert len(timestamps) == len(values)
        assert len(timestamps) > 0
        # Timestamps should be monotonically non-decreasing
        assert np.all(np.diff(timestamps) >= 0), "Timestamps not monotonic"


class TestCrossValidation:
    """Cross-validate our reader against asammdf (when available)."""

    @pytest.fixture(autouse=True)
    def _check_asammdf(self):
        try:
            import asammdf

            self.asammdf = asammdf
        except ImportError:
            pytest.skip("asammdf not installed")

    def test_values_match_asammdf(self, mdf_file):
        """Verify our extracted values match asammdf's output."""
        import asammdf

        # Read with asammdf
        mdf = asammdf.MDF(mdf_file)

        # Read with our reader
        reader = MDF4Reader(mdf_file)
        channels = reader.scan_metadata()

        masters = {}
        for ch in channels:
            if ch.channel_type in (CN_TYPE_MASTER, CN_TYPE_VIRTUAL_MASTER):
                masters[ch.group_idx] = ch

        # Compare first 5 signal channels
        compared = 0
        for ch in channels:
            if ch.channel_type in (CN_TYPE_MASTER, CN_TYPE_VIRTUAL_MASTER):
                continue
            if ch.sample_count == 0:
                continue
            if compared >= 5:
                break

            # Our reader
            master = masters.get(ch.group_idx)
            if master:
                master_info = {
                    "byte_offset": master.byte_offset,
                    "bit_offset": master.bit_offset,
                    "bit_count": master.bit_count,
                    "data_type": master.data_type,
                    "channel_type": master.channel_type,
                    "cc_type": master.cc_type,
                    "cc_params": list(master.cc_params) if master.cc_params else [],
                }
                signal_info = {
                    "data_block_addr": ch.data_block_addr,
                    "record_size": ch.record_size,
                    "byte_offset": ch.byte_offset,
                    "bit_offset": ch.bit_offset,
                    "bit_count": ch.bit_count,
                    "data_type": ch.data_type,
                    "channel_type": ch.channel_type,
                    "cc_type": ch.cc_type,
                    "cc_params": list(ch.cc_params) if ch.cc_params else [],
                }
                try:
                    our_times, our_values = MDF4Reader.read_channel_pair(
                        mdf_file, master_info, signal_info, ch.sample_count
                    )
                except Exception:
                    continue
            else:
                continue

            # asammdf
            try:
                sig = mdf.get(ch.channel_name, group=ch.group_idx, index=ch.channel_idx)
            except Exception:
                continue

            ref_values = sig.samples.astype(np.float64)

            # Compare (allow small floating point differences)
            n = min(len(our_values), len(ref_values))
            if n == 0:
                continue

            np.testing.assert_allclose(
                our_values[:n],
                ref_values[:n],
                rtol=1e-5,
                atol=1e-10,
                err_msg=f"Mismatch for channel {ch.channel_name} "
                f"(group={ch.group_idx}, idx={ch.channel_idx})",
            )
            compared += 1

        assert compared > 0, "No channels could be compared"


class TestCCConversion:
    """Test CC (Channel Conversion) block support."""

    def test_cc_fields_present_on_channel_info(self, mdf_file):
        reader = MDF4Reader(mdf_file)
        channels = reader.scan_metadata()
        for ch in channels:
            assert hasattr(ch, "cc_type")
            assert hasattr(ch, "cc_params")
            assert ch.cc_type in (-1, 0, 1, 2, 3, 4, 5, 6)
            assert isinstance(ch.cc_params, tuple)

    def test_apply_cc_linear(self):
        from impulse_data_sources.mdf.udf_helpers import apply_cc_conversion

        raw = np.array([0.0, 1.0, 2.0, 100.0])
        # linear: phys = 2.0 * raw + 10.0, params = (b=10.0, a=2.0)
        result = apply_cc_conversion(raw, 1, (10.0, 2.0))
        np.testing.assert_allclose(result, [10.0, 12.0, 14.0, 210.0])

    def test_apply_cc_rational(self):
        from impulse_data_sources.mdf.udf_helpers import apply_cc_conversion

        raw = np.array([1.0, 2.0, 10.0])
        # rational: (0*X^2 + 2*X + 1) / (0*X^2 + 0*X + 1) = 2*X + 1
        result = apply_cc_conversion(raw, 2, (0.0, 2.0, 1.0, 0.0, 0.0, 1.0))
        np.testing.assert_allclose(result, [3.0, 5.0, 21.0])

    def test_apply_cc_identity(self):
        from impulse_data_sources.mdf.udf_helpers import apply_cc_conversion

        raw = np.array([42.0, -1.5, 0.0])
        result = apply_cc_conversion(raw, 0, ())
        np.testing.assert_array_equal(result, raw)

    def test_apply_cc_tabular_interp(self):
        from impulse_data_sources.mdf.udf_helpers import apply_cc_conversion

        # Interleaved per spec: (key_0, val_0, key_1, val_1, key_2, val_2)
        raw = np.array([0.0, 50.0, 100.0, 150.0, 200.0])
        result = apply_cc_conversion(raw, 4, (0.0, 0.0, 100.0, 50.0, 200.0, 100.0))
        np.testing.assert_allclose(result, [0.0, 25.0, 50.0, 75.0, 100.0])

    def test_apply_cc_no_conversion(self):
        from impulse_data_sources.mdf.udf_helpers import apply_cc_conversion

        raw = np.array([1.0, 2.0, 3.0])
        result = apply_cc_conversion(raw, -1, ())
        np.testing.assert_array_equal(result, raw)
        result2 = apply_cc_conversion(raw, -1, None)
        np.testing.assert_array_equal(result2, raw)

    def test_virtual_master_cc_applied(self):
        """Verify CC conversion is applied to virtual master timestamps."""
        from impulse_data_sources.mdf.udf_helpers import extract_timestamps

        # Simulate raw_data for 5 samples with 8-byte records (any content)
        raw_data = b"\x00" * 40
        record_size = 8
        master_info = {
            "channel_type": 3,  # virtual master
            "byte_offset": 0,
            "bit_offset": 0,
            "bit_count": 0,
            "data_type": 0,
            "cc_type": 1,  # linear
            "cc_params": [0.0, 10.0],  # physical = 10 * index + 0
        }
        timestamps = extract_timestamps(raw_data, record_size, master_info)
        np.testing.assert_allclose(timestamps, [0, 10, 20, 30, 40])

    def test_virtual_master_no_cc(self):
        """Virtual master without CC returns raw indices."""
        from impulse_data_sources.mdf.udf_helpers import extract_timestamps

        raw_data = b"\x00" * 24
        record_size = 8
        master_info = {
            "channel_type": 3,
            "byte_offset": 0,
            "bit_offset": 0,
            "bit_count": 0,
            "data_type": 0,
            "cc_type": -1,
            "cc_params": [],
        }
        timestamps = extract_timestamps(raw_data, record_size, master_info)
        np.testing.assert_array_equal(timestamps, [0, 1, 2])


class TestPlanPartitionsCoalescing:
    """plan_partitions must coalesce many small groups into shared tasks while
    bounding both the output rows and the number of groups (block reads) per
    spec, and still split big groups by record range."""

    @staticmethod
    def _ch(gi, ci, samples, addr, ctype=0):
        from impulse_data_sources.mdf.mdf4_reader import ChannelInfo

        return ChannelInfo(
            group_idx=gi,
            channel_idx=ci,
            channel_name=f"s{gi}_{ci}",
            unit="",
            sample_count=samples,
            data_type=4,
            bit_offset=0,
            byte_offset=0,
            bit_count=64,
            channel_type=ctype,
            cn_block_addr=0,
            cg_block_addr=0,
            dg_block_addr=0,
            data_block_addr=addr,
            record_size=16,
            cn_flags=0,
            invalidation_bit_pos=0,
            invalidation_bytes=0,
            data_bytes=8,
            cc_type=-1,
            cc_params=[],
            rec_id_size=0,
            record_id=0,
        )

    def _layout(self):
        signal, masters, cidmap, gi = [], {}, {}, 0
        for _ in range(2000):  # many tiny groups
            addr = 1000 + gi
            signal.append(self._ch(gi, 1, 100, addr))
            masters[gi] = self._ch(gi, 0, 100, addr, ctype=2)
            cidmap[(gi, 1)] = gi
            gi += 1
        for _ in range(2):  # big groups -> record-range split
            addr = 1000 + gi
            signal.append(self._ch(gi, 1, 50_000_000, addr))
            masters[gi] = self._ch(gi, 0, 50_000_000, addr, ctype=2)
            cidmap[(gi, 1)] = gi
            gi += 1
        return masters, signal, cidmap

    def test_coalesces_and_respects_bounds(self):
        from impulse_data_sources.mdf.bin_packer import plan_partitions

        masters, signal, cidmap = self._layout()
        cap = 64
        target_mb = 256
        target_rows = target_mb * 1024 * 1024 // 16
        specs = plan_partitions(
            "f.mf4",
            masters,
            signal,
            cidmap,
            target_partition_mb=target_mb,
            max_groups_per_partition=cap,
        )

        # Far fewer specs than groups (2000 tiny would-be tasks collapse).
        assert len(specs) < 2000 / 10
        whole = [s for s in specs if "row_start" not in s]
        for s in whole:
            blocks = {c["data_block_addr"] for c in s["channels"]}
            assert len(blocks) <= cap  # group-count bound
            rows = sum(c["sample_count"] for c in s["channels"])
            assert rows <= target_rows  # output-row bound

        # Big groups still split by record range.
        assert any("row_start" in s for s in specs)

    def test_full_channel_coverage_exactly_once(self):
        from impulse_data_sources.mdf.bin_packer import plan_partitions

        masters, signal, cidmap = self._layout()
        specs = plan_partitions(
            "f.mf4", masters, signal, cidmap, target_partition_mb=256, max_groups_per_partition=64
        )
        # Every channel appears (whole-group once; ranged groups contiguously).
        whole_seen = set()
        ranges = {}
        for s in specs:
            for c in s["channels"]:
                key = (c["group_idx"], c["channel_idx"])
                if "row_start" in s:
                    ranges.setdefault(key, []).append((s["row_start"], s["row_end"]))
                else:
                    assert key not in whole_seen, "channel double-counted"
                    whole_seen.add(key)
        for _key, ivs in ranges.items():
            ivs.sort()
            assert ivs[0][0] == 0
            for a, b in zip(ivs, ivs[1:], strict=False):
                assert a[1] == b[0]  # contiguous, no gaps/overlap
        all_keys = whole_seen | set(ranges)
        assert len(all_keys) == len(signal)


class TestMDF4ReaderExtended:
    def test_file_bytes_matches_path(self, mdf_file):
        with open(mdf_file, "rb") as fh:
            data = fh.read()
        by_path = MDF4Reader(mdf_file).scan_metadata()
        by_bytes = MDF4Reader(file_bytes=data).scan_metadata()
        assert len(by_path) == len(by_bytes)
        assert [c.channel_name for c in by_path] == [c.channel_name for c in by_bytes]

    def test_invalid_signature_raises(self):
        with pytest.raises(ValueError, match="Not a valid MDF"):
            MDF4Reader(file_bytes=b"NOTMDF!!" + b"\x00" * 56).scan_metadata()

    def test_header_datetime_from_samples(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        reader = MDF4Reader(path)
        dt = reader.read_header_datetime()
        assert dt is not None
        assert dt.year == START_TIME.year
        epoch = reader.read_header_start_epoch_seconds()
        assert epoch is not None

    def test_linear_cc_on_sample_c(self):
        d, files = sample_mdf_dir()
        if "sample_c.mf4" not in files:
            pytest.skip("no cc sample")
        path = f"{d}/sample_c.mf4"
        reader = MDF4Reader(path)
        channels = reader.scan_metadata()
        scaled = [c for c in channels if c.channel_name == "Scaled"][0]
        assert scaled.cc_type == 1
        values = MDF4Reader.read_channel_data(
            path,
            scaled.data_block_addr,
            scaled.record_size,
            scaled.byte_offset,
            scaled.bit_offset,
            scaled.bit_count,
            scaled.data_type,
            scaled.channel_type,
            scaled.sample_count,
            cc_type=scaled.cc_type,
            cc_params=scaled.cc_params,
        )
        np.testing.assert_allclose(values, np.arange(10) * 2.0 + 10.0)

    def test_channel_to_dict_unsorted_fields(self):
        from impulse_data_sources.mdf.mdf4_reader import ChannelInfo

        ch = ChannelInfo(
            group_idx=0,
            channel_idx=0,
            channel_name="x",
            unit="",
            sample_count=1,
            data_type=4,
            bit_offset=0,
            byte_offset=4,
            bit_count=64,
            channel_type=0,
            cn_block_addr=0,
            cg_block_addr=0,
            dg_block_addr=100,
            data_block_addr=1000,
            record_size=20,
            rec_id_size=4,
            record_id=1,
        )
        ctx = {100: {"rec_id_size": 4, "cg_sizes": {1: 20}}}
        d = MDF4Reader.channel_to_dict(ch, ctx)
        assert d["rec_id_size"] == 4
        assert d["cg_record_sizes"] == {1: 20}


class TestParseCcBlock:
    @staticmethod
    def _cc_at_offset(blob: bytes, offset: int = 8):
        return b"\x00" * offset + blob

    def test_zero_addr_returns_identity(self):
        with io.BytesIO(b"") as f:
            assert MDF4Reader._parse_cc_block(f, 0) == (-1, ())

    def test_wrong_block_id(self):
        blob = self._cc_at_offset(b"##DT" + b"\x00" * 20)
        with io.BytesIO(blob) as f:
            assert MDF4Reader._parse_cc_block(f, 8) == (-1, ())

    def test_empty_params_and_unsupported_type(self):
        from ._block_fixtures import make_cc_block

        no_params = self._cc_at_offset(make_cc_block(cc_type=0, cc_val_count=0, params=()))
        with io.BytesIO(no_params) as f:
            assert MDF4Reader._parse_cc_block(f, 8) == (0, ())

        unsupported = self._cc_at_offset(make_cc_block(cc_type=9, cc_val_count=0, params=()))
        with io.BytesIO(unsupported) as f:
            assert MDF4Reader._parse_cc_block(f, 8) == (-1, ())

    def test_linear_params(self):
        from ._block_fixtures import make_cc_block

        blob = self._cc_at_offset(make_cc_block(cc_type=1, cc_val_count=2, params=(10.0, 2.0)))
        with io.BytesIO(blob) as f:
            cc_type, params = MDF4Reader._parse_cc_block(f, 8)
        assert cc_type == 1
        assert params == (10.0, 2.0)
