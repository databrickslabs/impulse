from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    __version__ = _pkg_version("databricks-impulse")
except PackageNotFoundError:
    # Source-only mode (PYTHONPATH=src, no install): no dist-info, read VERSION directly.
    from pathlib import Path

    __version__ = (Path(__file__).resolve().parents[2] / "VERSION").read_text().strip()
