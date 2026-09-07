import functools
import inspect
import logging
from collections.abc import Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from impulse_query_engine import __version__

logger = logging.getLogger(__name__)

PRODUCT_NAME = "databricks-impulse"


def log_telemetry(ws: WorkspaceClient, key: str, value: str) -> None:
    """Trace telemetry via the Databricks User-Agent header.

    Copies the workspace config, appends a key-value pair to the
    User-Agent string, then fires a lightweight API call so the
    enriched header reaches the Databricks control plane.

    Parameters
    ----------
    ws : WorkspaceClient
        Authenticated workspace client.
    key : str
        Telemetry key to log.
    value : str
        Telemetry value to log.
    """
    new_config = ws.config.copy().with_user_agent_extra(key, value)
    logger.debug(f"Added User-Agent extra {key}={value}")

    ws = type(ws)(config=new_config)

    try:
        ws.clusters.select_spark_version()
    except DatabricksError as e:
        logger.debug(f"Databricks workspace is not available: {e}")


def tag_spark_connect_user_agent(spark, product: str, version: str) -> None:
    """Best-effort: tag a Spark Connect session's per-request user-agent.

    For an **external Spark Connect / Databricks Connect** client, ``import impulse``
    runs off the Databricks compute, so library-import detection never sees impulse and
    the compute is not attributable. The SDK's global user-agent (set at package import)
    only tags REST traffic, never the Spark Connect gRPC channel.

    The Connect user-agent, however, is not frozen into the gRPC channel: every request
    carries it as ``client_type``, read live from a mutable params dict on the channel
    builder. Prepending ``<product>/<version>`` there makes all subsequent compute
    requests attributable — without the caller having to build their session with
    ``DatabricksSession.builder.userAgent(...)``.

    This reaches into **private** pyspark internals (``spark._client._builder._params``),
    which are not a stable API, so it is strictly best-effort: it is a no-op for classic
    (non-Connect) sessions and never raises. Callers running in a Databricks notebook/job
    are already attributed via import detection and are unaffected.

    Parameters
    ----------
    spark : SparkSession
        The Spark session used for query execution. Only Spark Connect sessions are
        tagged; anything else is silently ignored.
    product : str
        Product identifier (e.g. ``"databricks-impulse"``).
    version : str
        Product version string.
    """
    # The Spark Connect connection-string parameter name for the user agent
    # (``ChannelBuilder.PARAM_USER_AGENT``). Referenced by value to avoid importing
    # ``pyspark.sql.connect.client.core``, which pulls in grpcio and is absent outside a
    # Connect client anyway.
    user_agent_key = "user_agent"
    try:
        builder = spark._client._builder  # SparkConnectClient -> ChannelBuilder
        tag = f"{product}/{version}"
        existing = builder._params.get(user_agent_key, "")
        if tag not in existing:  # idempotent: don't stack the tag on repeated calls
            builder._params[user_agent_key] = f"{tag} {existing}".strip()
            logger.debug(f"Tagged Spark Connect user-agent with {tag}")
    except Exception as e:  # noqa: BLE001 - classic session or internals changed
        logger.debug(f"Skipped Spark Connect user-agent tagging: {e}")


def telemetry_logger(key: str, value: str, workspace_client_attr: str = "ws") -> Callable:
    """Decorator that logs telemetry before executing the wrapped method.

    Expects the instance (``self``) to carry a :class:`WorkspaceClient`
    under the attribute named by *workspace_client_attr* (default ``"ws"``).

    Parameters
    ----------
    key : str
        Telemetry key to log.
    value : str
        Telemetry value to log.
    workspace_client_attr : str
        Name of the ``WorkspaceClient`` attribute on the class.
    """

    def decorator(func: Callable) -> Callable:
        sig = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if hasattr(self, workspace_client_attr):
                workspace_client = getattr(self, workspace_client_attr)
                log_telemetry(workspace_client, key, value)
            else:
                raise AttributeError(
                    f"Workspace client attribute '{workspace_client_attr}' not found "
                    f"on {self.__class__.__name__}. "
                    f"Make sure your class has the specified workspace client attribute."
                )
            # If the wrapped method received a Spark session, tag it so external
            # Spark Connect compute is attributable (best-effort; no-op otherwise).
            spark = sig.bind_partial(self, *args, **kwargs).arguments.get("spark")
            if spark is not None:
                tag_spark_connect_user_agent(spark, PRODUCT_NAME, __version__)
            return func(self, *args, **kwargs)

        return wrapper

    return decorator


def verify_workspace_client(
    ws: WorkspaceClient, product_name: str, version: str
) -> WorkspaceClient:
    """Set product info for telemetry attribution and verify connectivity.

    Sets ``_product_info`` on the SDK config so that **every** subsequent
    API call from this client identifies itself as originating from the
    given product.  Then makes a lightweight API call to verify the
    workspace is reachable (fail-fast).

    Parameters
    ----------
    ws : WorkspaceClient
        Authenticated workspace client.
    product_name : str
        Product identifier (e.g. ``"impulse"``).
    version : str
        Product version string.

    Returns
    -------
    WorkspaceClient
        The same client, now tagged with product info.

    Raises
    ------
    DatabricksError
        If the workspace is not reachable.
    """
    product_info = ws.config._product_info
    if product_info is None or product_info[0] != product_name:
        ws.config._product_info = product_name, version

    ws.clusters.select_spark_version()
    return ws
