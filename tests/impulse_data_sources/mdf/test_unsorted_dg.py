"""Tests for unsorted MDF4 data group record filtering."""

import struct
import tempfile
from pathlib import Path

import numpy as np
import pytest

from impulse_data_sources.mdf.mdf_decode import (
    filter_unsorted_records,
    prepare_cg_records,
    read_record_id,
    storage_record_id,
    extract_signal,
    extract_timestamps,
)
from impulse_data_sources.mdf.mdf4_reader import MDF4Reader
from impulse_data_sources.mdf.bin_packer import plan_partitions
from impulse_data_sources.mdf.udf_helpers import convert_spec_to_arrow_batches


def _build_interleaved_block():
    """Two CGs interleaved: rec_id_size=4, both CGs record_size=20."""
    rec_id_size = 4
    record_size = 20
    cg_sizes = {1: record_size, 2: record_size}
    records = []
    # CG1: time=1.0, value=10.0
    r1 = struct.pack("<I", 1) + struct.pack("<d", 1.0) + struct.pack("<d", 10.0)
    # CG2: time=2.0, value=20.0
    r2 = struct.pack("<I", 2) + struct.pack("<d", 2.0) + struct.pack("<d", 20.0)
    records.extend([r1, r2, r1])
    return b"".join(records), rec_id_size, cg_sizes, record_size


class TestFilterUnsortedRecords:
    def test_filters_matching_record_id(self):
        raw, rec_id_size, cg_sizes, record_size = _build_interleaved_block()
        filtered = filter_unsorted_records(raw, rec_id_size, 1, cg_sizes)
        assert len(filtered) == 2 * record_size
        assert filtered[0:4] == struct.pack("<I", 1)
        assert struct.unpack("<d", filtered[4:12])[0] == pytest.approx(1.0)
        assert struct.unpack("<d", filtered[12:20])[0] == pytest.approx(10.0)

    def test_read_record_id_sizes(self):
        buf = struct.pack("<B", 5) + struct.pack("<H", 0x0102) + struct.pack("<I", 0x03040506)
        assert read_record_id(buf, 0, 1) == 5
        assert read_record_id(buf, 1, 2) == 0x0102
        assert read_record_id(buf, 3, 4) == 0x03040506

    def test_storage_record_id_masks(self):
        assert storage_record_id(0x100000001, 4) == 1


class TestPrepareCgRecords:
    def test_row_slice_after_filter(self):
        raw, rec_id_size, cg_sizes, record_size = _build_interleaved_block()
        prepared, offset = prepare_cg_records(
            raw,
            record_size=record_size,
            rec_id_size=rec_id_size,
            record_id=1,
            cg_record_sizes=cg_sizes,
            row_start=1,
            row_end=2,
        )
        assert offset == 1
        assert len(prepared) == record_size
        assert struct.unpack("<d", prepared[12:20])[0] == pytest.approx(10.0)

    def test_json_string_keys_in_cg_sizes(self):
        raw, rec_id_size, cg_sizes, record_size = _build_interleaved_block()
        str_key_sizes = {str(k): v for k, v in cg_sizes.items()}
        prepared, _ = prepare_cg_records(
            raw,
            record_size=record_size,
            rec_id_size=rec_id_size,
            record_id=1,
            cg_record_sizes=str_key_sizes,
        )
        assert len(prepared) == 2 * record_size


class TestPlanPartitionsUnsortedFields:
    def test_specs_carry_unsorted_metadata(self):
        from impulse_data_sources.mdf.mdf4_reader import ChannelInfo, CN_TYPE_MASTER

        ch = ChannelInfo(
            group_idx=0,
            channel_idx=0,
            channel_name="Sig",
            unit="",
            sample_count=2,
            data_type=4,
            bit_offset=0,
            byte_offset=12,
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
        master = ChannelInfo(
            group_idx=0,
            channel_idx=0,
            channel_name="Time",
            unit="s",
            sample_count=2,
            data_type=4,
            bit_offset=0,
            byte_offset=4,
            bit_count=64,
            channel_type=2,
            cn_block_addr=0,
            cg_block_addr=0,
            dg_block_addr=100,
            data_block_addr=1000,
            record_size=20,
            rec_id_size=4,
            record_id=1,
        )
        ctx = {100: {"rec_id_size": 4, "cg_sizes": {1: 20, 2: 20}}}
        specs = plan_partitions(
            "/tmp/x.mf4",
            {0: master},
            [ch],
            {(0, 0): 0},
            unsorted_dg_ctx=ctx,
        )
        assert len(specs) == 1
        ch_dict = specs[0]["channels"][0]
        assert ch_dict["rec_id_size"] == 4
        assert ch_dict["record_id"] == 1
        assert ch_dict["cg_record_sizes"] == {1: 20, 2: 20}


class TestConvertSpecUnsorted:
    def test_decode_interleaved_dt_block(self):
        raw, rec_id_size, cg_sizes, record_size = _build_interleaved_block()

        def _make_dt_file(payload: bytes) -> str:
            dt = b"##DT" + b"\x00" * 4
            dt += struct.pack("<Q", 24 + len(payload))
            dt += struct.pack("<Q", 0)
            dt += payload
            fd, path = tempfile.mkstemp(suffix=".mf4")
            import os

            os.write(fd, dt)
            os.close(fd)
            return path

        path = _make_dt_file(raw)
        try:
            ch_spec = {
                "channel_id": 0,
                "group_idx": 0,
                "channel_idx": 0,
                "sample_count": 2,
                "data_type": 4,
                "bit_offset": 0,
                "byte_offset": 12,
                "bit_count": 64,
                "channel_type": 0,
                "data_block_addr": 0,
                "record_size": record_size,
                "master_info": {
                    "byte_offset": 4,
                    "bit_offset": 0,
                    "bit_count": 64,
                    "data_type": 4,
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
            spec = {"file_path": path, "channels": [ch_spec]}
            batches = list(convert_spec_to_arrow_batches(spec))
            assert len(batches) == 1
            table = batches[0]
            values = table.column("value").to_pylist()
            times = table.column("time").to_pylist()
            assert values == pytest.approx([10.0, 10.0])
            assert times == pytest.approx([1.0, 1.0])
        finally:
            Path(path).unlink(missing_ok=True)


class TestExtractAfterFilter:
    def test_signal_and_timestamps_from_filtered_bytes(self):
        raw, rec_id_size, cg_sizes, record_size = _build_interleaved_block()
        prepared, offset = prepare_cg_records(
            raw,
            record_size=record_size,
            rec_id_size=rec_id_size,
            record_id=1,
            cg_record_sizes=cg_sizes,
        )
        master = {
            "byte_offset": 4,
            "bit_offset": 0,
            "bit_count": 64,
            "data_type": 4,
            "channel_type": 2,
            "cc_type": -1,
            "cc_params": [],
        }
        ch = {
            "channel_type": 0,
            "data_type": 4,
            "bit_count": 64,
            "byte_offset": 12,
            "bit_offset": 0,
            "record_size": record_size,
            "cn_flags": 0,
            "invalidation_bit_pos": 0,
            "invalidation_bytes": 0,
            "data_bytes": 16,
            "cc_type": -1,
            "cc_params": [],
        }
        ts = extract_timestamps(prepared, record_size, master, index_offset=offset)
        vals = extract_signal(prepared, record_size, ch)
        assert ts.tolist() == pytest.approx([1.0, 1.0])
        assert vals.tolist() == pytest.approx([10.0, 10.0])
