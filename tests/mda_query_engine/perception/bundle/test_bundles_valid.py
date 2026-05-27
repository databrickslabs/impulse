"""Validate every bundle under ``bundles/`` parses cleanly via the Databricks CLI.

Auto-discovers any directory containing a ``databricks.yml`` and runs
``databricks bundle validate`` against it. Adding a new bundle requires no
change to this file — drop a ``databricks.yml`` under ``bundles/<name>/`` and
it joins the suite.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
BUNDLES_DIR = REPO_ROOT / "bundles"

DATABRICKS_CLI = shutil.which("databricks")


def _discover_bundles() -> list[Path]:
    if not BUNDLES_DIR.exists():
        return []
    return sorted(p.parent for p in BUNDLES_DIR.glob("*/databricks.yml"))


@pytest.mark.skipif(DATABRICKS_CLI is None, reason="databricks CLI not on PATH")
@pytest.mark.parametrize(
    "bundle_dir",
    _discover_bundles() or [pytest.param(None, marks=pytest.mark.skip(reason="no bundles found"))],
    ids=lambda p: p.name if p else "no-bundles",
)
def test_bundle_validates(bundle_dir: Path) -> None:
    result = subprocess.run(
        [DATABRICKS_CLI, "bundle", "validate"],
        cwd=bundle_dir,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"`databricks bundle validate` failed in {bundle_dir.name}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
