"""
Tests for the custom PySpark data sources (non-Spark components).

Tests the file resolution, metadata scanning, and signal reading logic
without requiring a running Spark session.
"""

import os
import pytest
import numpy as np

from impulse_ds.mdf.mdf4_reader import MDF4Reader
from impulse_ds.mdf.datasources import (
    _resolve_file_list,
    MdfMetadataReader,
    MdfSignalsReader,
)

# Sample MDF files are generated on the fly with asammdf (see _mdf_samples.py)
# so the tests don't depend on any pre-built fixtures. EXAMPLE_FILES is empty when
# asammdf is unavailable, and the `if not EXAMPLE_FILES: skip` guards handle that.
from ._mdf_samples import sample_mdf_dir

EXAMPLE_DIR, EXAMPLE_FILES = sample_mdf_dir()


class TestResolveFileList:
    def test_missing_path_raises(self):
        with pytest.raises(ValueError, match="'path' is required"):
            _resolve_file_list({})

    def test_auto_discovery(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        paths = _resolve_file_list({"path": EXAMPLE_DIR})
        assert len(paths) >= 1
        assert all(p.endswith(".mf4") for p in paths)

    def test_explicit_file_list(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        first = EXAMPLE_FILES[0]
        paths = _resolve_file_list({"path": EXAMPLE_DIR, "files": first})
        assert len(paths) == 1
        assert paths[0] == os.path.join(EXAMPLE_DIR, first)

    def test_comma_separated_files(self):
        if len(EXAMPLE_FILES) < 2:
            pytest.skip("Need at least 2 example files")
        files_str = f"{EXAMPLE_FILES[0]}, {EXAMPLE_FILES[1]}"
        paths = _resolve_file_list({"path": EXAMPLE_DIR, "files": files_str})
        assert len(paths) == 2

    def test_absolute_file_uris(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        abs_path = os.path.join(EXAMPLE_DIR, EXAMPLE_FILES[0])
        paths = _resolve_file_list({"path": "/unused/base", "files": abs_path})
        assert paths == [abs_path]

    def test_mixed_absolute_and_relative_files(self):
        if len(EXAMPLE_FILES) < 2:
            pytest.skip("Need at least 2 example files")
        abs_path = os.path.join(EXAMPLE_DIR, EXAMPLE_FILES[0])
        rel_path = EXAMPLE_FILES[1]
        files_str = f"{abs_path}, {rel_path}"
        paths = _resolve_file_list({"path": EXAMPLE_DIR, "files": files_str})
        assert paths == [abs_path, os.path.join(EXAMPLE_DIR, rel_path)]

    def test_auto_discovery_recursive(self, tmp_path):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        import shutil

        nested = tmp_path / "batch_a" / "run_1"
        nested.mkdir(parents=True)
        for name in EXAMPLE_FILES:
            shutil.copy(os.path.join(EXAMPLE_DIR, name), nested / name)

        paths = _resolve_file_list({"path": str(tmp_path)})
        assert len(paths) == len(EXAMPLE_FILES)
        assert all(p.endswith(".mf4") for p in paths)
        assert all("batch_a" in p and "run_1" in p for p in paths)

    def test_empty_dir_raises(self, tmp_path):
        with pytest.raises(ValueError, match="No MDF4 files found"):
            _resolve_file_list({"path": str(tmp_path)})


class TestScanChannelsOrganized:
    def test_returns_organized_structure(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        file_path = os.path.join(EXAMPLE_DIR, EXAMPLE_FILES[0])
        reader = MDF4Reader(file_path)
        organized = reader.scan_channels_organized()
        assert "master_channels" in organized
        assert "signal_channels" in organized
        assert "channel_id_map" in organized
        assert len(organized["signal_channels"]) > 0
        assert len(organized["master_channels"]) > 0

    def test_channel_ids_sequential(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        file_path = os.path.join(EXAMPLE_DIR, EXAMPLE_FILES[0])
        reader = MDF4Reader(file_path)
        organized = reader.scan_channels_organized()
        ids = sorted(organized["channel_id_map"].values())
        assert ids == list(range(len(organized["signal_channels"])))


class TestMetadataReader:
    def test_read_partition(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        reader = MdfMetadataReader({"path": EXAMPLE_DIR, "files": EXAMPLE_FILES[0]})
        partitions = reader.partitions()
        assert len(partitions) == 1
        rows = list(reader.read(partitions[0]))
        assert len(rows) > 0
        from impulse_ds.mdf.schemas import METADATA_SCHEMA

        assert len(rows[0]) == len(METADATA_SCHEMA.fields)  # md_comment included
        (
            file_uri,
            channel_id,
            group_idx,
            channel_idx,
            channel_name,
            unit,
            header_datetime,
            md_comment,
        ) = rows[0]
        assert isinstance(file_uri, str) and file_uri.endswith(".mf4")
        assert channel_id == 0
        assert isinstance(channel_name, str)
        assert len(channel_name) > 0
        assert md_comment is None or isinstance(md_comment, str)


class TestSignalsReader:
    def test_partitions_created(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        reader = MdfSignalsReader({"path": EXAMPLE_DIR, "files": EXAMPLE_FILES[0]})
        partitions = reader.partitions()
        assert len(partitions) >= 1
        # partitions() now returns InputPartition objects carrying the spec JSON
        assert "file_path" in partitions[0].value

    def test_read_produces_arrow_batches(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        import pyarrow as pa

        reader = MdfSignalsReader({"path": EXAMPLE_DIR, "files": EXAMPLE_FILES[0]})
        partitions = reader.partitions()
        batches = []
        for p in partitions[:2]:
            batches.extend(reader.read(p))
        assert len(batches) > 0
        b = batches[0]
        assert isinstance(b, pa.RecordBatch)
        assert b.schema.names == ["file_uri", "channel_id", "time", "value"]
        row0 = {name: b.column(i)[0].as_py() for i, name in enumerate(b.schema.names)}
        assert isinstance(row0["file_uri"], str)
        assert isinstance(row0["channel_id"], int)
        assert isinstance(row0["time"], float)
        assert row0["value"] is None or isinstance(row0["value"], float)

    def test_timestamps_nonnegative(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        reader = MdfSignalsReader({"path": EXAMPLE_DIR, "files": EXAMPLE_FILES[0]})
        partitions = reader.partitions()
        times = []
        for b in reader.read(partitions[0]):
            times.extend(b.column("time").to_pylist())
        assert all(t >= 0 for t in times), "Negative timestamps found"


class TestRunLengthEncoding:
    def _read(self, opts):
        from collections import defaultdict

        reader = MdfSignalsReader(opts)
        cols = defaultdict(list)
        names = None
        for p in reader.partitions():
            for b in reader.read(p):
                names = b.schema.names
                for n in names:
                    cols[n].append(b.column(n).to_numpy(zero_copy_only=False))
        return names, {n: (np.concatenate(v) if v else np.array([])) for n, v in cols.items()}

    def test_rle_schema(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        from impulse_ds.mdf.datasources import MdfSignalsDataSource

        opts = {"path": EXAMPLE_DIR, "files": EXAMPLE_FILES[0], "run_length_encoding": "true"}
        sch = MdfSignalsDataSource(dict(opts)).schema()
        assert [f.name for f in sch.fields] == [
            "file_uri",
            "channel_id",
            "tstart",
            "tend",
            "value",
        ]
        names, _ = self._read(opts)
        assert names == ["file_uri", "channel_id", "tstart", "tend", "value"]

    def test_rle_final_sample_is_point(self):
        """Each channel must end with a zero-width point row (tstart == tend) at
        its last timestamp, so the final sample is recoverable."""
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        from collections import defaultdict

        base = {"path": EXAMPLE_DIR, "files": EXAMPLE_FILES[0], "target_partition_mb": "16"}
        _, plain = self._read(base)
        _, rle = self._read({**base, "run_length_encoding": "true"})

        def per_ch(d, cols):
            out = defaultdict(lambda: defaultdict(list))
            for k in np.unique(d["channel_id"]):
                m = d["channel_id"] == k
                for c in cols:
                    out[int(k)][c] = d[c][m]
            return out

        pch = per_ch(plain, ["time"])
        rch = per_ch(rle, ["tstart", "tend"])
        assert pch
        for ch, p in pch.items():
            last_t = float(np.max(p["time"]))
            ts, te = rch[ch]["tstart"], rch[ch]["tend"]
            points = ts[np.isclose(ts, te)]
            # a point row exists exactly at the channel's last timestamp
            assert np.any(np.isclose(points, last_t)), f"channel {ch}: no final point at {last_t}"

    def test_rle_reconstructs_original_and_compresses(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        from collections import defaultdict
        from impulse_ds.mdf.udf_helpers import _rle_run_starts

        base = {"path": EXAMPLE_DIR, "files": EXAMPLE_FILES[0], "target_partition_mb": "16"}
        _, plain = self._read(base)
        _, rle = self._read({**base, "run_length_encoding": "true"})

        def per_channel(d, cols):
            out = defaultdict(lambda: defaultdict(list))
            for k in np.unique(d["channel_id"]):
                m = d["channel_id"] == k
                for c in cols:
                    out[int(k)][c] = d[c][m]
            return out

        pch = per_channel(plain, ["time", "value"])
        rch = per_channel(rle, ["tstart", "tend", "value"])

        def whole_rle(t, v):
            o = np.argsort(t, kind="stable")
            t, v = t[o], v[o]
            s = _rle_run_starts(v)
            m = len(s)
            return [(t[s[k]], t[s[k + 1]] if k < m - 1 else t[-1], v[s[k]]) for k in range(m)]

        def merge_adjacent(rows):
            rows = sorted(rows, key=lambda r: r[0])
            out = []
            for t0, t1, val in rows:
                same = (
                    out
                    and out[-1][1] == t0
                    and (out[-1][2] == val or (out[-1][2] != out[-1][2] and val != val))
                )
                if same:
                    out[-1] = (out[-1][0], t1, out[-1][2])
                else:
                    out.append((t0, t1, val))
            return out

        total_plain = total_runs = 0
        for k, p in pch.items():
            ref = whole_rle(p["time"], p["value"])
            got = merge_adjacent(
                list(zip(rch[k]["tstart"], rch[k]["tend"], rch[k]["value"], strict=False))
            )
            total_plain += len(p["time"])
            total_runs += len(got)
            assert len(ref) == len(got), f"channel {k}: {len(ref)} runs vs {len(got)}"
            for (a0, a1, av), (b0, b1, bv) in zip(ref, got, strict=False):
                assert abs(a0 - b0) < 1e-6 and abs(a1 - b1) < 1e-6
                assert av == bv or (av != av and bv != bv)  # NaN == NaN
        # This example compresses substantially; guard against a no-op RLE.
        assert total_runs < total_plain


class TestMastersDataSource:
    def _read(self, reader, cols):
        from collections import defaultdict

        out = defaultdict(lambda: defaultdict(list))
        names = None
        for p in reader.partitions():
            for b in reader.read(p):
                names = b.schema.names
                kc = b.column(cols[0]).to_numpy()
                for k in np.unique(kc):
                    m = kc == k
                    for c in cols[1:]:
                        out[int(k)][c].append(b.column(c).to_numpy()[m])
        agg = {k: {c: np.concatenate(v) for c, v in d.items()} for k, d in out.items()}
        return names, agg

    def test_masters_schema_and_grid(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        from impulse_ds.mdf.datasources import (
            MdfMastersDataSource,
            MdfMastersReader,
            MdfSignalsReader,
        )
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        opts = {"path": EXAMPLE_DIR, "files": EXAMPLE_FILES[0], "target_partition_mb": "16"}
        sch = MdfMastersDataSource(dict(opts)).schema()
        assert [f.name for f in sch.fields] == ["file_uri", "group_idx", "timestamp"]

        names, masters = self._read(MdfMastersReader(opts), ["group_idx", "timestamp"])
        assert names == ["file_uri", "group_idx", "timestamp"]
        masters = {g: np.sort(d["timestamp"]) for g, d in masters.items()}
        assert masters and all(np.all(np.diff(ts) >= 0) for ts in masters.values())

        # Each channel's signal-time grid equals its group's master timestamps.
        org = MDF4Reader(os.path.join(EXAMPLE_DIR, EXAMPLE_FILES[0])).scan_channels_organized()
        ch2grp = {cid: g for (g, c), cid in org["channel_id_map"].items()}
        _, sig = self._read(MdfSignalsReader(opts), ["channel_id", "time"])
        for ch, d in sig.items():
            ts = np.sort(d["time"])
            assert np.allclose(ts, masters[ch2grp[ch]])

    def test_reverse_rle_recovers_originals(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        from impulse_ds.mdf.datasources import MdfMastersReader, MdfSignalsReader
        from impulse_ds.mdf.mdf4_reader import MDF4Reader

        opts = {"path": EXAMPLE_DIR, "files": EXAMPLE_FILES[0], "target_partition_mb": "16"}
        _, orig = self._read(MdfSignalsReader(opts), ["channel_id", "time", "value"])
        _, rle = self._read(
            MdfSignalsReader({**opts, "run_length_encoding": "true"}),
            ["channel_id", "tstart", "tend", "value"],
        )
        _, masters = self._read(MdfMastersReader(opts), ["group_idx", "timestamp"])
        masters = {g: np.sort(d["timestamp"]) for g, d in masters.items()}
        org = MDF4Reader(os.path.join(EXAMPLE_DIR, EXAMPLE_FILES[0])).scan_channels_organized()
        ch2grp = {cid: g for (g, c), cid in org["channel_id_map"].items()}

        for ch, d in orig.items():
            o = np.argsort(d["time"], kind="stable")
            ot, ov = d["time"][o], d["value"][o]
            r = rle[ch]
            ro = np.argsort(r["tstart"], kind="stable")
            tstart, value = r["tstart"][ro], r["value"][ro]
            gts = masters[ch2grp[ch]]
            idx = np.clip(np.searchsorted(tstart, gts, side="right") - 1, 0, len(tstart) - 1)
            rt, rv = gts, value[idx]
            assert len(rt) == len(ot) and np.allclose(rt, ot)
            assert ((rv == ov) | (np.isnan(rv) & np.isnan(ov))).all()


class TestAbsoluteTime:
    def _read(self, opts, cols):
        from collections import defaultdict
        from impulse_ds.mdf.datasources import MdfSignalsReader

        rd = MdfSignalsReader(opts)
        out = defaultdict(lambda: defaultdict(list))
        for p in rd.partitions():
            for b in rd.read(p):
                c = b.column("channel_id").to_numpy()
                for k in np.unique(c):
                    m = c == k
                    for col in cols:
                        out[int(k)][col].append(b.column(col).to_numpy()[m])
        return {k: {col: np.concatenate(v) for col, v in d.items()} for k, d in out.items()}

    def test_absolute_time_adds_start_and_forces_float64(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        from impulse_ds.mdf.datasources import MdfSignalsDataSource

        f = EXAMPLE_FILES[0]
        start = MDF4Reader(os.path.join(EXAMPLE_DIR, f)).read_header_start_epoch_seconds()
        if start is None:
            pytest.skip("file has no HD start time")

        # Schema forces float64 time even when float32 requested.
        sch = MdfSignalsDataSource(
            {"path": EXAMPLE_DIR, "files": f, "absolute_time": "true", "time_dtype": "float32"}
        ).schema()
        assert sch["time"].dataType.simpleString() == "double"

        base = {"path": EXAMPLE_DIR, "files": f, "target_partition_mb": "16"}
        rel = self._read(base, ["time", "value"])
        ab = self._read({**base, "absolute_time": "true"}, ["time", "value"])
        for ch in rel:
            rt = np.sort(rel[ch]["time"])
            at = np.sort(ab[ch]["time"])
            assert np.allclose(at, rt + start, atol=1e-3)  # offset applied
            assert np.array_equal(
                np.sort(rel[ch]["value"]), np.sort(ab[ch]["value"])  # values unchanged
            )

    def test_absolute_time_rle_and_masters_align(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        from collections import defaultdict
        from impulse_ds.mdf.datasources import MdfMastersReader

        f = EXAMPLE_FILES[0]
        start = MDF4Reader(os.path.join(EXAMPLE_DIR, f)).read_header_start_epoch_seconds()
        if start is None:
            pytest.skip("file has no HD start time")

        def masters(opts):
            rd = MdfMastersReader(opts)
            out = defaultdict(list)
            for p in rd.partitions():
                for b in rd.read(p):
                    g = b.column("group_idx").to_numpy()
                    ts = b.column("timestamp").to_numpy()
                    for k in np.unique(g):
                        out[int(k)].append(ts[g == k])
            return {k: np.sort(np.concatenate(v)) for k, v in out.items()}

        base = {"path": EXAMPLE_DIR, "files": f, "target_partition_mb": "16"}
        mrel = masters(base)
        mabs = masters({**base, "absolute_time": "true"})
        for g in mrel:
            assert np.allclose(mabs[g], mrel[g] + start, atol=1e-3)


class TestSchemaMatchesEmittedTypes:
    """schema() MUST match the Arrow types read() emits for every option combo;
    a mismatch makes Spark's writer call getDouble on a float vector (or similar)
    and fail. Regression for the absolute_time + value_dtype=float32 case where
    schema() over-forced `value` to double while the decoder emitted float32."""

    def test_all_option_combos_consistent(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        from itertools import product
        from impulse_ds.mdf.datasources import MdfSignalsDataSource

        spark2arrow = {
            "double": "double",
            "float": "float",
            "bigint": "int64",
            "int": "int32",
            "string": "string",
        }
        base = {
            "path": EXAMPLE_DIR,
            "files": EXAMPLE_FILES[0],
            "target_partition_mb": "8",
            "stripe_target_mb": "2",
        }
        for abst, vdt, tdt, rle, part in product(
            ["false", "true"],
            ["float64", "float32"],
            ["float64", "float32"],
            ["false", "true"],
            ["group", "stripe"],
        ):
            opts = {
                **base,
                "absolute_time": abst,
                "value_dtype": vdt,
                "time_dtype": tdt,
                "run_length_encoding": rle,
                "partitioning": part,
            }
            sch = MdfSignalsDataSource(dict(opts)).schema()
            declared = {f.name: spark2arrow[f.dataType.simpleString()] for f in sch.fields}
            reader = MdfSignalsReader(opts)
            emitted = None
            for p in reader.partitions():
                for b in reader.read(p):
                    emitted = {f.name: str(f.type) for f in b.schema}
                    break
                if emitted:
                    break
            if emitted is not None:
                assert emitted == declared, (
                    f"schema/emit mismatch abs={abst} val={vdt} time={tdt} "
                    f"rle={rle} part={part}: {declared} vs {emitted}"
                )


class TestDatasourcesEdgeCases:
    def test_metadata_read_empty_file_path(self):
        from impulse_ds.mdf.datasources import MdfMetadataReader
        from pyspark.sql.datasource import InputPartition

        reader = MdfMetadataReader({"path": "/unused"})
        rows = list(reader.read(InputPartition({"file_path": ""})))
        assert rows == []

    def test_masters_schema_float32(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        from impulse_ds.mdf.datasources import MdfMastersDataSource, MdfMastersReader

        opts = {"path": EXAMPLE_DIR, "files": EXAMPLE_FILES[0], "time_dtype": "float32"}
        sch = MdfMastersDataSource(dict(opts)).schema()
        assert sch["timestamp"].dataType.simpleString() == "float"
        reader = MdfMastersReader(opts)
        for p in reader.partitions():
            for b in reader.read(p):
                assert str(b.schema.field("timestamp").type) == "float"
                return

    def test_absolute_time_raises_without_hd_start(self, tmp_path, monkeypatch):
        from asammdf import MDF, Signal
        import numpy as np

        path = tmp_path / "no_start.mf4"
        mdf = MDF(version="4.10")
        t = np.arange(5, dtype=np.float64) * 0.1
        mdf.append([Signal(samples=t, timestamps=t, name="x")])
        mdf.save(str(path), overwrite=True)
        mdf.close()

        monkeypatch.setattr(
            "impulse_ds.mdf.mdf4_reader.MDF4Reader.read_header_start_epoch_seconds",
            lambda _self: None,
        )
        reader = MdfSignalsReader(
            {"path": str(tmp_path), "files": "no_start.mf4", "absolute_time": "true"}
        )
        with pytest.raises(ValueError, match="no measurement start time"):
            reader.partitions()

    def test_masters_absolute_time_raises_without_hd_start(self, tmp_path, monkeypatch):
        from asammdf import MDF, Signal
        import numpy as np
        from impulse_ds.mdf.datasources import MdfMastersReader

        path = tmp_path / "no_start.mf4"
        mdf = MDF(version="4.10")
        t = np.arange(5, dtype=np.float64) * 0.1
        mdf.append([Signal(samples=t, timestamps=t, name="x")])
        mdf.save(str(path), overwrite=True)
        mdf.close()

        monkeypatch.setattr(
            "impulse_ds.mdf.mdf4_reader.MDF4Reader.read_header_start_epoch_seconds",
            lambda _self: None,
        )
        reader = MdfMastersReader(
            {
                "path": str(tmp_path),
                "files": "no_start.mf4",
                "absolute_time": "true",
            }
        )
        with pytest.raises(ValueError, match="no measurement start time"):
            reader.partitions()

    def test_signals_empty_partitions_when_no_signals(self, monkeypatch):
        from impulse_ds.mdf.datasources import MdfSignalsReader

        def _empty_scan(self):
            return {
                "master_channels": {},
                "signal_channels": [],
                "channel_id_map": {},
                "unsorted_dg_ctx": {},
            }

        monkeypatch.setattr(
            "impulse_ds.mdf.datasources._resolve_file_list",
            lambda _opts: ["/fake/a.mf4"],
        )
        monkeypatch.setattr(
            "impulse_ds.mdf.mdf4_reader.MDF4Reader.scan_channels_organized",
            _empty_scan,
        )
        reader = MdfSignalsReader({"path": "/x", "files": "a.mf4"})
        parts = reader.partitions()
        assert len(parts) == 1
        assert parts[0].value == "[]"
        assert list(reader.read(parts[0])) == []

    def test_masters_empty_when_no_masters(self, monkeypatch):
        from impulse_ds.mdf.datasources import MdfMastersReader

        def _no_masters(self):
            return {
                "master_channels": {},
                "signal_channels": [],
                "channel_id_map": {},
                "unsorted_dg_ctx": {},
            }

        monkeypatch.setattr(
            "impulse_ds.mdf.datasources._resolve_file_list",
            lambda _opts: ["/fake/a.mf4"],
        )
        monkeypatch.setattr(
            "impulse_ds.mdf.mdf4_reader.MDF4Reader.scan_channels_organized",
            _no_masters,
        )
        reader = MdfMastersReader({"path": "/x", "files": "a.mf4"})
        parts = reader.partitions()
        assert parts[0].value == "[]"
