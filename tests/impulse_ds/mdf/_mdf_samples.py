"""
Build small but meaningful MDF4 sample files for the unit tests using asammdf,
so the tests do not depend on any pre-built `.mf4` fixtures in the repo.

Two files are produced (cached for the test session, cleaned up at exit):
  - sample_a.mf4 : uncompressed (##DT data blocks)
  - sample_b.mf4 : compressed   (##DZ data blocks)

Each contains two channel groups so the tests exercise grouping/coalescing,
masters, and multi-file behaviour:
  group 0 (100 samples @ 10 Hz):
    - "Speed"      km/h   float64  monotonic ramp        (all-distinct -> RLE barely compresses)
    - "Gear"       (none) int32    piecewise-constant     (compresses well under RLE)
  group 1 (40 samples @ 5 Hz):
    - "EngineTemp" degC   float64  constant runs          (compresses under RLE)

A known header start time is set so the absolute_time path has something to add,
and one channel carries an XML comment so md_comment is populated.
"""
import atexit
import datetime
import os
import shutil
import tempfile

import numpy as np

try:
    from asammdf import MDF, Signal
    HAS_ASAMMDF = True
except Exception:  # pragma: no cover - asammdf is a dev dependency
    HAS_ASAMMDF = False

# Naive UTC start time (asammdf stores it in the HD block).
START_TIME = datetime.datetime(2024, 3, 1, 12, 30, 15, 500000)

_CACHE = None  # (dir, [filenames]) once built


def _build_file(path: str, compression: int) -> None:
    mdf = MDF(version="4.10")

    # Group 0: 100 samples at 10 Hz.
    t0 = (np.arange(100) * 0.1).astype(np.float64)
    speed = Signal(
        samples=(t0 * 2.0), timestamps=t0, name="Speed", unit="km/h",
        comment="<CNcomment><TX>vehicle speed</TX></CNcomment>",
    )
    gear = np.zeros(100, dtype=np.int32)
    gear[20:60] = 2
    gear[60:] = 4
    gear_sig = Signal(samples=gear, timestamps=t0, name="Gear", unit="")
    mdf.append([speed, gear_sig], comment="powertrain")

    # Group 1: 40 samples at 5 Hz, with constant runs (good for RLE).
    t1 = (np.arange(40) * 0.2).astype(np.float64)
    temp = np.full(40, 20.0)
    temp[10:25] = 21.5
    temp[25:] = 19.0
    temp_sig = Signal(samples=temp, timestamps=t1, name="EngineTemp", unit="degC")
    mdf.append([temp_sig], comment="thermal")

    mdf.header.start_time = START_TIME
    mdf.save(path, overwrite=True, compression=compression)
    mdf.close()


def sample_mdf_dir():
    """Return (directory, sorted_filenames) of the generated sample MDF files.

    Builds them once per process (cached) into a temp dir that is removed at
    interpreter exit. Returns ("", []) if asammdf is unavailable so callers can
    skip the affected tests.
    """
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    if not HAS_ASAMMDF:
        _CACHE = ("", [])
        return _CACHE
    d = tempfile.mkdtemp(prefix="mdf_unit_samples_")
    atexit.register(shutil.rmtree, d, ignore_errors=True)
    _build_file(os.path.join(d, "sample_a.mf4"), compression=0)  # ##DT
    _build_file(os.path.join(d, "sample_b.mf4"), compression=2)  # ##DZ
    files = sorted(f for f in os.listdir(d) if f.lower().endswith(".mf4"))
    _CACHE = (d, files)
    return _CACHE
