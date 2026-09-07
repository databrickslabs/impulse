import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

import databricks.sdk.useragent as ua
import impulse_query_engine
from impulse_query_engine.telemetry import (
    log_telemetry,
    tag_spark_connect_user_agent,
    telemetry_logger,
    verify_workspace_client,
)


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

        with patch("impulse_query_engine.telemetry.log_telemetry") as mock_log:
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

        with patch("impulse_query_engine.telemetry.log_telemetry") as mock_log:
            result = obj.toPandas()

        mock_log.assert_called_once_with(ws, "query", "to_pandas")
        assert result == "done"

    def test_decorator_tags_spark_connect_session_when_spark_arg_present(self):
        builder = SimpleNamespace(_params={"user_agent": "databricks-session"})

        class QueryBuilder:
            def __init__(self, ws):
                self.ws = ws

            @telemetry_logger("query", "solve")
            def solve(self, spark):
                return "result"

        ws = create_autospec(WorkspaceClient)
        spark = SimpleNamespace(_client=SimpleNamespace(_builder=builder))

        with patch("impulse_query_engine.telemetry.log_telemetry"):
            QueryBuilder(ws).solve(spark)

        assert builder._params["user_agent"].startswith("databricks-impulse/")

    def test_decorator_does_not_tag_when_no_spark_arg(self):
        # Methods without a ``spark`` parameter must not raise or attempt tagging.
        class QueryBuilder:
            def __init__(self, ws):
                self.ws = ws

            @telemetry_logger("query", "solve")
            def solve(self):
                return "result"

        ws = create_autospec(WorkspaceClient)
        with (
            patch("impulse_query_engine.telemetry.log_telemetry"),
            patch("impulse_query_engine.telemetry.tag_spark_connect_user_agent") as mock_tag,
        ):
            QueryBuilder(ws).solve()
        mock_tag.assert_not_called()

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


class TestGlobalUserAgentRegistration:
    """Importing the package registers impulse in the SDK's process-global user-agent."""

    def test_import_registers_product_and_extra(self):
        # Package import (at test-collection time) runs the registration block.
        assert ua.product() == ("databricks-impulse", impulse_query_engine.__version__)
        assert ("databricks-impulse", impulse_query_engine.__version__) in ua._extra

    def test_registration_failure_does_not_break_import(self):
        # A failure inside the registration block must never propagate out of import.
        with patch.object(ua, "with_product", side_effect=ValueError("boom")):
            importlib.reload(impulse_query_engine)  # must not raise
        # Restore a clean registration for any subsequent assertions in the session.
        importlib.reload(impulse_query_engine)
        assert ua.product() == ("databricks-impulse", impulse_query_engine.__version__)


class TestTagSparkConnectUserAgent:
    @staticmethod
    def _fake_connect_session(user_agent="databricks-session"):
        builder = SimpleNamespace(_params={"user_agent": user_agent})
        return SimpleNamespace(_client=SimpleNamespace(_builder=builder)), builder

    def test_prepends_tag_to_connect_session(self):
        spark, builder = self._fake_connect_session()
        tag_spark_connect_user_agent(spark, "databricks-impulse", "0.6.1")
        assert builder._params["user_agent"] == "databricks-impulse/0.6.1 databricks-session"

    def test_is_idempotent(self):
        spark, builder = self._fake_connect_session()
        tag_spark_connect_user_agent(spark, "databricks-impulse", "0.6.1")
        tag_spark_connect_user_agent(spark, "databricks-impulse", "0.6.1")
        # Tag appears exactly once; the existing value is preserved.
        assert builder._params["user_agent"] == "databricks-impulse/0.6.1 databricks-session"

    def test_handles_empty_existing_user_agent(self):
        spark, builder = self._fake_connect_session(user_agent="")
        tag_spark_connect_user_agent(spark, "databricks-impulse", "0.6.1")
        assert builder._params["user_agent"] == "databricks-impulse/0.6.1"

    def test_classic_session_is_a_silent_no_op(self):
        # A classic SparkSession has no ``_client`` — must not raise.
        tag_spark_connect_user_agent(object(), "databricks-impulse", "0.6.1")
