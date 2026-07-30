"""Register impulse_ds.mdf as an alias for impulse_data_sources.mdf (package rename)."""

import importlib
import pkgutil
import sys
import types


def _set_parent_attr(full_name: str, mod: types.ModuleType) -> None:
    """Attach *mod* on its parent so monkeypatch can resolve dotted paths."""
    parent_name, _, child_name = full_name.rpartition(".")
    parent = sys.modules.get(parent_name)
    if parent is not None:
        setattr(parent, child_name, mod)


def _install_impulse_ds_shim() -> None:
    if "impulse_ds.mdf" in sys.modules:
        return

    impulse_ds = types.ModuleType("impulse_ds")
    impulse_ds.__path__ = []
    sys.modules["impulse_ds"] = impulse_ds

    mdf = importlib.import_module("impulse_data_sources.mdf")
    sys.modules["impulse_ds.mdf"] = mdf
    impulse_ds.mdf = mdf

    for modinfo in pkgutil.walk_packages(mdf.__path__, prefix="impulse_data_sources.mdf."):
        alias = modinfo.name.replace("impulse_data_sources", "impulse_ds", 1)
        mod = importlib.import_module(modinfo.name)
        sys.modules[alias] = mod
        _set_parent_attr(alias, mod)


_install_impulse_ds_shim()
