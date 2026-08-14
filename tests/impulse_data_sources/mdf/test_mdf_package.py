"""Smoke tests for impulse_data_sources.mdf lazy public API."""


def test_lazy_imports():
    import impulse_data_sources.mdf as mdf

    assert mdf.MDF4Reader is not None
    assert mdf.MdfSignalsDataSource is not None
    assert mdf.MdfMetadataDataSource is not None
    assert mdf.MdfMastersDataSource is not None
    assert mdf.register_mdf_datasources is not None
