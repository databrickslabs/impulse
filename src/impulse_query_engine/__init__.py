from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    __version__ = _pkg_version("databricks-impulse")
except PackageNotFoundError:
    # Source-only mode (PYTHONPATH=src, no install): no dist-info, read VERSION directly.
    from pathlib import Path

    __version__ = (Path(__file__).resolve().parents[2] / "VERSION").read_text().strip()

try:
    import databricks.sdk.useragent as _ua

    _ua.with_extra("databricks-impulse", __version__)
    _ua.with_product("databricks-impulse", __version__)
except Exception:  # noqa: BLE001 - telemetry must never break import
    import logging

    logging.getLogger(__name__).debug("impulse SDK user-agent registration skipped", exc_info=True)
