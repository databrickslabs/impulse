"""Tests for impulse_ds.mdf.bin_packer partition planners."""

import pytest

from impulse_ds.mdf.bin_packer import (
    plan_master_partitions,
    plan_partitions,
    plan_stripes_for_file,
)
from impulse_ds.mdf.mdf4_reader import ChannelInfo, CN_TYPE_MASTER
from ._mdf_samples import sample_mdf_dir


def _ch(gi, ci, samples, addr, ctype=0, dg=0, rec_id=0, rec_id_size=0):
    return ChannelInfo(
        group_idx=gi, channel_idx=ci, channel_name=f"s{gi}_{ci}", unit="",
        sample_count=samples, data_type=4, bit_offset=0, byte_offset=8,
        bit_count=64, channel_type=ctype, cn_block_addr=0, cg_block_addr=0,
        dg_block_addr=dg, data_block_addr=addr, record_size=16,
        cn_flags=0, invalidation_bit_pos=0, invalidation_bytes=0, data_bytes=8,
        cc_type=-1, cc_params=(), rec_id_size=rec_id_size, record_id=rec_id,
    )


class TestPlanMasterPartitions:
    def test_skips_zero_sample_groups(self):
        m = _ch(0, 0, 0, 1000, ctype=CN_TYPE_MASTER)
        specs = plan_master_partitions("f.mf4", {0: m})
        assert specs == []

    def test_splits_big_master_by_record_range(self):
        m = _ch(0, 0, 100_000, 1000, ctype=CN_TYPE_MASTER)
        specs = plan_master_partitions("f.mf4", {0: m}, target_partition_mb=0.001)
        assert len(specs) > 1
        assert all("row_start" in s for s in specs)

    def test_coalesces_small_masters(self):
        masters = {i: _ch(i, 0, 50, 1000 + i, ctype=CN_TYPE_MASTER) for i in range(10)}
        specs = plan_master_partitions(
            "f.mf4", masters, target_partition_mb=256, max_groups_per_partition=64,
        )
        assert len(specs) < 10

    def test_coalesce_flush_when_target_exceeded(self):
        masters = {
            0: _ch(0, 0, 50, 1000, ctype=CN_TYPE_MASTER),
            1: _ch(1, 0, 50, 2000, ctype=CN_TYPE_MASTER),
            2: _ch(2, 0, 50, 3000, ctype=CN_TYPE_MASTER),
        }
        specs = plan_master_partitions("f.mf4", masters, target_partition_mb=0.001)
        assert len(specs) >= 2
        total_masters = sum(len(s["masters"]) for s in specs)
        assert total_masters == 3


class TestPlanStripesForFile:
    def test_stripes_on_sample_files(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        for fn in files:
            path = f"{d}/{fn}"
            with open(path, "rb") as fh:
                fb = fh.read()
            specs = plan_stripes_for_file(path, file_bytes=fb, stripe_target_mb=0.001)
            assert specs
            for s in specs:
                assert s["byte_start"] < s["byte_end"]
                assert s["subblocks"]
                total_recs = sum(
                    sb["rec_count"] * s["groups"][str(sb["group_idx"])]["n_channels"]
                    for sb in s["subblocks"]
                )
                assert total_recs > 0


class TestPlanPartitionsExtended:
    def test_skips_zero_sample_signal_group(self):
        ch = _ch(0, 1, 0, 1000)
        master = _ch(0, 0, 0, 1000, ctype=CN_TYPE_MASTER)
        specs = plan_partitions("f.mf4", {0: master}, [ch], {(0, 1): 0})
        assert specs == []

    def test_wide_group_splits_by_channel_subset(self):
        addr = 5000
        signals = [_ch(0, i, 1000, addr) for i in range(1, 300)]
        master = _ch(0, 0, 1000, addr, ctype=CN_TYPE_MASTER)
        cidmap = {(0, i): i - 1 for i in range(1, 300)}
        specs = plan_partitions(
            "f.mf4", {0: master}, signals, cidmap,
            target_partition_mb=0.01, channel_threshold=16,
        )
        multi = [s for s in specs if len(s["channels"]) < len(signals)]
        assert multi
        seen = set()
        for s in specs:
            for c in s["channels"]:
                key = (c["group_idx"], c["channel_idx"])
                assert key not in seen
                seen.add(key)
        assert len(seen) == len(signals)

    def test_signal_without_matching_master(self):
        signals = [_ch(1, 1, 100, 1000)]
        specs = plan_partitions("f.mf4", {}, signals, {(1, 1): 0})
        assert len(specs) == 1
        assert specs[0]["channels"][0]["master_info"] is None

    def test_stripes_skip_zero_sample_group(self, monkeypatch):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        with open(path, "rb") as fh:
            fb = fh.read()
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        organized = MDF4Reader(file_bytes=fb).scan_channels_organized()
        organized = dict(organized)
        organized["signal_channels"] = list(organized["signal_channels"]) + [
            _ch(99, 1, 0, 0),
        ]
        monkeypatch.setattr(
            MDF4Reader, "scan_channels_organized", lambda self: organized,
        )
        specs = plan_stripes_for_file(path, file_bytes=fb, stripe_target_mb=0.001)
        assert specs
        for spec in specs:
            assert "99" not in spec["groups"]
