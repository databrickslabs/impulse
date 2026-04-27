from unittest.mock import MagicMock, create_autospec, patch

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from mda_query_engine.telemetry import log_telemetry, telemetry_logger, verify_workspace_client


class TestLogTelemetry:
    def test_sends_beacon_with_user_agent_extra(self):
        ws = create_autospec(WorkspaceClient)
        log_telemetry(ws, "query", "solve")

        ws.config.copy.assert_called_once()
        ws.config.copy().with_user_agent_extra.assert_called_once_with("query", "solve")

    def test_gracefully_handles_databricks_error(self):
        ws = create_autospec(WorkspaceClient)
        inner_ws = create_autospec(WorkspaceClient)
        inner_ws.clusters.select_spark_version.side_effect = DatabricksError("unreachable")

        with patch.object(type(ws), "__call__", return_value=inner_ws):
            log_telemetry(ws, "key", "value")

    def test_does_not_raise_on_workspace_unavailable(self):
        ws = create_autospec(WorkspaceClient)
        ws.config.copy.return_value.with_user_agent_extra.return_value = MagicMock()

        log_telemetry(ws, "some_key", "some_value")


class TestTelemetryLogger:
    def test_decorator_calls_log_telemetry_and_original_function(self):
        class QueryBuilder:
            def __init__(self, ws):
                self.ws = ws

            @telemetry_logger("query", "solve")
            def solve(self):
                return "result"

        ws = create_autospec(WorkspaceClient)
        builder = QueryBuilder(ws)

        with patch("mda_query_engine.telemetry.log_telemetry") as mock_log:
            result = builder.solve()

        mock_log.assert_called_once_with(ws, "query", "solve")
        assert result == "result"

    def test_decorator_raises_attribute_error_when_ws_missing(self):
        class NoWs:
            @telemetry_logger("query", "solve")
            def solve(self):
                return "result"

        obj = NoWs()
        with pytest.raises(AttributeError, match="Workspace client attribute 'ws' not found"):
            obj.solve()

    def test_decorator_uses_custom_attribute_name(self):
        class CustomAttr:
            def __init__(self, client):
                self.my_client = client

            @telemetry_logger("query", "to_pandas", workspace_client_attr="my_client")
            def toPandas(self):
                return "done"

        ws = create_autospec(WorkspaceClient)
        obj = CustomAttr(ws)

        with patch("mda_query_engine.telemetry.log_telemetry") as mock_log:
            result = obj.toPandas()

        mock_log.assert_called_once_with(ws, "query", "to_pandas")
        assert result == "done"

    def test_decorator_preserves_function_metadata(self):
        class QueryBuilder:
            def __init__(self):
                self.ws = create_autospec(WorkspaceClient)

            @telemetry_logger("query", "solve")
            def solve(self):
                """Solve the query."""
                pass

        assert QueryBuilder.solve.__name__ == "solve"
        assert QueryBuilder.solve.__doc__ == "Solve the query."


class TestVerifyWorkspaceClient:
    def test_sets_product_info_and_verifies_connectivity(self):
        ws = create_autospec(WorkspaceClient)
        ws.config._product_info = None

        result = verify_workspace_client(ws, "mda", "0.0.4")

        assert result is ws
        assert ws.config._product_info == ("mda", "0.0.4")
        ws.clusters.select_spark_version.assert_called_once()

    def test_does_not_overwrite_matching_product_info(self):
        ws = create_autospec(WorkspaceClient)
        ws.config._product_info = ("mda", "0.0.3")

        verify_workspace_client(ws, "mda", "0.0.4")

        assert ws.config._product_info == ("mda", "0.0.3")

    def test_overwrites_different_product_info(self):
        ws = create_autospec(WorkspaceClient)
        ws.config._product_info = ("other_product", "1.0.0")

        verify_workspace_client(ws, "mda", "0.0.4")

        assert ws.config._product_info == ("mda", "0.0.4")

    def test_raises_databricks_error_when_workspace_unreachable(self):
        ws = create_autospec(WorkspaceClient)
        ws.config._product_info = None
        ws.clusters.select_spark_version.side_effect = DatabricksError("unreachable")

        with pytest.raises(DatabricksError):
            verify_workspace_client(ws, "mda", "0.0.4")
