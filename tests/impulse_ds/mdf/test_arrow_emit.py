"""Integration tests for impulse_ds.mdf.arrow_emit decode paths."""

import os
import struct
from collections import defaultdict

import numpy as np
import pytest

from impulse_ds.mdf.arrow_emit import _rle_compress_chunk, _rle_flush
from impulse_ds.mdf.bin_packer import plan_stripes_for_file
from impulse_ds.mdf.datasources import MdfSignalsReader
from impulse_ds.mdf.udf_helpers import (
    convert_spec_to_arrow_batches,
    convert_stripe_spec_to_arrow_batches,
)
from ._block_fixtures import build_dl_file
from ._mdf_samples import sample_mdf_dir


def _read_signals(opts):
    reader = MdfSignalsReader(opts)
    cols = defaultdict(list)
    for p in reader.partitions():
        for b in reader.read(p):
            for name in b.schema.names:
                cols[name].append(b.column(name).to_numpy(zero_copy_only=False))
    return {n: (np.concatenate(v) if v else np.array([])) for n, v in cols.items()}


def _by_channel(cols):
    out = defaultdict(lambda: {"time": [], "value": []})
    for i, cid in enumerate(cols["channel_id"]):
        k = int(cid)
        out[k]["time"].append(float(cols["time"][i]))
        out[k]["value"].append(float(cols["value"][i]))
    return out


class TestStripeParity:
    @pytest.mark.parametrize("fname", ["sample_a.mf4", "sample_b.mf4"])
    def test_stripe_matches_group_mode(self, fname):
        d, files = sample_mdf_dir()
        if fname not in files:
            pytest.skip("no sample")
        base = {"path": d, "files": fname, "target_partition_mb": "16", "stripe_target_mb": "2"}
        group = _by_channel(_read_signals({**base, "partitioning": "group"}))
        stripe = _by_channel(_read_signals({**base, "partitioning": "stripe"}))
        assert set(group) == set(stripe)
        for cid in group:
            gt = np.array(group[cid]["time"])
            st = np.array(stripe[cid]["time"])
            np.testing.assert_allclose(np.sort(gt), np.sort(st), rtol=1e-5)
            g_order = np.argsort(group[cid]["time"])
            s_order = np.argsort(stripe[cid]["time"])
            gv = np.array(group[cid]["value"])[g_order]
            sv = np.array(stripe[cid]["value"])[s_order]
            assert ((gv == sv) | (np.isnan(gv) & np.isnan(sv))).all()


class TestDlRowRangeEmit:
    def test_convert_spec_honors_row_range_on_dl(self):
        records = [struct.pack("<d", float(i)) for i in range(6)]
        blob, dl_addr = build_dl_file(records)
        import tempfile

        fd, path = tempfile.mkstemp(suffix=".mf4")
        os.write(fd, blob)
        os.close(fd)
        try:
            ch = {
                "channel_id": 0,
                "group_idx": 0,
                "channel_idx": 1,
                "sample_count": 6,
                "data_type": 4,
                "bit_offset": 0,
                "byte_offset": 0,
                "bit_count": 64,
                "channel_type": 0,
                "data_block_addr": dl_addr,
                "record_size": 8,
                "master_info": {
                    "byte_offset": 0,
                    "bit_offset": 0,
                    "bit_count": 64,
                    "data_type": 4,
                    "channel_type": 3,
                    "cc_type": -1,
                    "cc_params": [],
                },
                "cn_flags": 0,
                "invalidation_bit_pos": 0,
                "invalidation_bytes": 0,
                "data_bytes": 8,
                "cc_type": -1,
                "cc_params": [],
            }
            spec = {
                "file_path": path,
                "channels": [ch],
                "row_start": 2,
                "row_end": 5,
            }
            batches = list(convert_spec_to_arrow_batches(spec))
            assert batches
            times = batches[0].column("time").to_pylist()
            assert len(times) == 3
            assert times == pytest.approx([2.0, 3.0, 4.0])
        finally:
            os.unlink(path)


class TestRleCrossChunk:
    def test_rle_carry_across_read_chunks(self):
        ts1 = np.array([0.0, 1.0, 2.0])
        vs1 = np.array([5.0, 5.0, 5.0])
        closed1, carry = _rle_compress_chunk(ts1, vs1, None)
        assert closed1 is None
        assert carry is not None

        ts2 = np.array([3.0, 4.0, 5.0])
        vs2 = np.array([5.0, 5.0, 7.0])
        closed2, carry2 = _rle_compress_chunk(ts2, vs2, carry)
        assert closed2 is not None
        t0, t1, v = closed2
        assert len(t0) == 1
        assert t0[0] == pytest.approx(0.0)
        assert t1[0] == pytest.approx(5.0)
        assert v[0] == pytest.approx(5.0)

        flushed = _rle_flush(carry2)
        assert flushed is not None
        ft0, ft1, fv = flushed
        assert fv[-1] == pytest.approx(7.0)
        assert ft1[-1] == pytest.approx(5.0)


class TestStripeDecode:
    def test_convert_stripe_spec_on_sample(self):
        d, files = sample_mdf_dir()
        if not files:
            pytest.skip("no samples")
        path = f"{d}/{files[0]}"
        with open(path, "rb") as fh:
            fb = fh.read()
        specs = plan_stripes_for_file(path, file_bytes=fb, stripe_target_mb=0.001)
        assert specs
        batches = list(convert_stripe_spec_to_arrow_batches(specs[0]))
        assert batches
        assert batches[0].num_rows > 0
