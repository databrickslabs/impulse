"""
MDF4 → Delta Lake converter for PySpark / Databricks.

Reads ASAM MDF4 measurement files directly from their binary blocks (no
asammdf dependency at runtime) and converts them to long-format Delta tables,
parallelised across Spark workers.

Public API (lazy-loaded to avoid importing PySpark unless needed)::

    from impulse_data_sources.mdf import (
        MDFToDeltaConverter,
        MDF4Reader,
        register_mdf_datasources,
        MdfSignalsDataSource,
        MdfMetadataDataSource,
        MdfMastersDataSource,
    )
"""

__all__ = [
    "MDFToDeltaConverter",
    "MDF4Reader",
    "register_mdf_datasources",
    "MdfSignalsDataSource",
    "MdfMetadataDataSource",
    "MdfMastersDataSource",
]


def __getattr__(name):
    if name == "MDFToDeltaConverter":
        from .converter import MDFToDeltaConverter

        return MDFToDeltaConverter
    if name == "MDF4Reader":
        from .mdf4_reader import MDF4Reader

        return MDF4Reader
    if name == "register_mdf_datasources":
        from .datasources import register_mdf_datasources

        return register_mdf_datasources
    if name in ("MdfSignalsDataSource", "MdfMetadataDataSource", "MdfMastersDataSource"):
        from .datasources import (
            MdfMetadataDataSource,
            MdfMastersDataSource,
            MdfSignalsDataSource,
        )

        return {
            "MdfSignalsDataSource": MdfSignalsDataSource,
            "MdfMetadataDataSource": MdfMetadataDataSource,
            "MdfMastersDataSource": MdfMastersDataSource,
        }[name]
    raise AttributeError(f"module 'impulse_data_sources.mdf' has no attribute {name!r}")
