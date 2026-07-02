"""Tests for MDF data source registration and read telemetry."""

from unittest.mock import MagicMock, create_autospec, patch

import pytest
from databricks.sdk import WorkspaceClient

from impulse_query_engine import __version__
from impulse_ds.mdf import datasources
from impulse_ds.mdf.datasources import (
    MdfMetadataDataSource,
    MdfMetadataReader,
    MdfMastersDataSource,
    MdfMastersReader,
    MdfSignalsDataSource,
    MdfSignalsReader,
    register_mdf_datasources,
)
from ._mdf_samples import sample_mdf_dir

EXAMPLE_DIR, EXAMPLE_FILES = sample_mdf_dir()


@pytest.fixture(autouse=True)
def reset_ws():
    """Isolate module-level workspace client between tests."""
    prev = datasources._ws
    datasources._ws = None
    yield
    datasources._ws = prev


class TestRegisterMdfDatasources:
    @patch("impulse_query_engine.telemetry.verify_workspace_client")
    def test_registers_three_sources_and_verifies_workspace(self, mock_verify):
        ws = create_autospec(WorkspaceClient)
        mock_verify.return_value = ws
        spark = MagicMock()

        result = register_mdf_datasources(spark, ws)

        mock_verify.assert_called_once_with(ws, "databricks-impulse", __version__)
        assert result is ws
        assert datasources._ws is ws
        spark.dataSource.register.assert_any_call(MdfSignalsDataSource)
        spark.dataSource.register.assert_any_call(MdfMetadataDataSource)
        spark.dataSource.register.assert_any_call(MdfMastersDataSource)
        assert spark.dataSource.register.call_count == 3


class TestPartitionsTelemetry:
    @pytest.fixture
    def reader_opts(self):
        if not EXAMPLE_FILES:
            pytest.skip("No example files")
        return {"path": EXAMPLE_DIR, "files": EXAMPLE_FILES[0]}

    @patch("impulse_query_engine.telemetry.log_telemetry")
    def test_signals_partitions_emits_telemetry(self, mock_log, reader_opts):
        ws = create_autospec(WorkspaceClient)
        datasources._ws = ws
        MdfSignalsReader(reader_opts).partitions()
        mock_log.assert_called_once_with(ws, "mdf", "mdf_signals")

    @patch("impulse_query_engine.telemetry.log_telemetry")
    def test_metadata_partitions_emits_telemetry(self, mock_log, reader_opts):
        ws = create_autospec(WorkspaceClient)
        datasources._ws = ws
        MdfMetadataReader(reader_opts).partitions()
        mock_log.assert_called_once_with(ws, "mdf", "mdf_metadata")

    @patch("impulse_query_engine.telemetry.log_telemetry")
    def test_masters_partitions_emits_telemetry(self, mock_log, reader_opts):
        ws = create_autospec(WorkspaceClient)
        datasources._ws = ws
        MdfMastersReader(reader_opts).partitions()
        mock_log.assert_called_once_with(ws, "mdf", "mdf_masters")

    @patch("impulse_query_engine.telemetry.log_telemetry")
    def test_partitions_skips_telemetry_when_ws_unset(self, mock_log, reader_opts):
        assert datasources._ws is None
        MdfSignalsReader(reader_opts).partitions()
        MdfMetadataReader(reader_opts).partitions()
        MdfMastersReader(reader_opts).partitions()
        mock_log.assert_not_called()
