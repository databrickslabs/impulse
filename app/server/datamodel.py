import io
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Literal

import yaml
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.workspace import ImportFormat
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from server import sql
from server.errors import ApiError
from server.settings import settings
from server.spec import Window
from server.user import User

CACHE_SECONDS = 60
MAX_CHANNELS = 5000
REQUIRED = ("container_metrics", "channel_metrics", "channels")


SOLVER_CONFIG_EXAMPLE = """solver_config:
  container_metrics:
    column_name_mapping: {measurement_id: container_id}
    filters: {environment: PUMA}
  channels:
    column_name_mapping: {ts: timestamp, val: value}"""


class Tables(BaseModel):
    model_config = ConfigDict(extra="forbid")

    container_metrics: str = Field(
        "container_metrics",
        description="Recordings, one row per container. Every tables entry is a table name in the source schema or a full "
        "catalog.schema.table. Impulse: source.container_metrics_table.",
    )
    channel_metrics: str = Field("channel_metrics", description="One row per channel and container. Impulse: source.channel_metrics_table.")
    channels: str = Field("channels", description="Samples. Impulse: source.channels_uri.")
    container_tags: str | None = Field(
        "container_tags",
        description="Optional EAV recording tags (container_id, key, value). Impulse: source.container_tags_table. null: not used.",
    )
    channel_tags: str | None = Field(
        "channel_tags",
        description="Optional EAV channel tags (container_id, channel_id, key, value). When present, channels are identified by these "
        "tags, otherwise by channel_metrics columns. Impulse: source.channel_tags_table. null: not used.",
    )


class Uut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["tag", "column"] | None = Field(
        None,
        description="column: the unit under test id is a column on container_metrics; units under test are selected with Impulse "
        "metric_filters. tag: a key in container_tags; units under test are selected with tag_filters. Empty: column when "
        "container_metrics has the key column, otherwise tag.",
    )
    key: str = Field(
        "vehicle_key",
        description="Column on container_metrics or tag key in container_tags holding the unit under test id, such as a vehicle, "
        "machine or test bench id.",
    )


class TimeColumns(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: str = Field(
        "start_dt",
        description="Timestamp column on container_metrics with each recording's start. Used for the unit under test list and the time window "
        "(Impulse metric_filters).",
    )
    stop: str = Field(
        "stop_dt",
        description="Timestamp column on container_metrics with each recording's stop. Used for the unit under test list and the time window "
        "(Impulse metric_filters).",
    )


class DataModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    silver_catalog: str = Field("", description='Catalog new reports read from, and the default for "Test against a schema". Empty: choose per report.')
    silver_schema: str = Field("", description='Schema new reports read from, and the default for "Test against a schema". Empty: choose per report.')
    gold_catalog: str = Field("", description="Catalog new reports write their gold tables to. Empty: the source catalog.")
    gold_schema: str = Field("", description="Schema new reports write their gold tables to. Empty: the source schema.")
    tables: Tables = Field(default_factory=Tables)
    data_type: Literal["RAW", "RLE"] | None = Field(
        None,
        description="RLE (channels has tstart, tend, value) or RAW (timestamp, value). Empty: detected from the channels columns. "
        "Impulse: query_engine.data_type.",
    )
    solver_config: dict = Field(
        default_factory=dict,
        description="Passed unchanged to Impulse as query_engine.solver_config; the app applies the same column mappings and filters "
        "in its own queries. Keys: project_id (string, Impulse's project scoping) and one section per table (container_metrics, "
        "container_tags, channel_metrics, channel_tags, channels), each with column_name_mapping {physical column: Impulse internal "
        "name} and filters {internal column: value}, equality filters applied after renaming.",
        examples=[SOLVER_CONFIG_EXAMPLE],
    )
    uut: Uut = Field(default_factory=Uut)
    time_columns: TimeColumns = Field(default_factory=TimeColumns)
    channel_identity: list[str] = Field(
        default_factory=lambda: ["channel_name"],
        min_length=1,
        description="Tag keys or channel_metrics columns that identify a channel. Each channel signal becomes "
        "db.query.channel(<key>=<value>, ...).",
        examples=["channel_identity: [channel_name, data_key]"],
    )
    unit_key: str | None = Field(
        "unit",
        description="Tag key or channel_metrics column holding a channel's unit. Shown in the app and used as the default bins unit. "
        "null: no units.",
    )
    time_unit: Literal["ns", "us", "ms", "s"] = Field(
        "us",
        description="Unit of the timestamps in the channels table (tstart, tend, timestamp): ns, us, ms or s. Impulse's silver schema "
        "uses us (microseconds). Impulse computes durations as tend - tstart in this unit; the app uses it only to show durations "
        "in seconds.",
    )
    measurement_dimensions: list[str] | None = Field(
        None,
        description="container_metrics columns (internal names) copied to the gold measurement_dimension table. Empty: Impulse's "
        "default [container_id, start_ts, stop_ts]. Impulse: measurement_dimensions.",
        examples=["measurement_dimensions: [container_id, vehicle_key, start_ts, stop_ts]"],
    )

    def mapping(self, table: str) -> dict:
        return (self.solver_config.get(table) or {}).get("column_name_mapping") or {}

    def physical(self, table: str, internal: str) -> str:
        return next((phys for phys, mapped in self.mapping(table).items() if mapped == internal), internal)

    def internal(self, table: str, physical: str) -> str:
        return self.mapping(table).get(physical, physical)

    def filters(self, table: str) -> dict:
        return (self.solver_config.get(table) or {}).get("filters") or {}


class Source(BaseModel):
    catalog: str
    schema_: str = Field(alias="schema")
    tables: dict[str, str | None]
    columns: dict[str, list[str]]
    data_type: Literal["RAW", "RLE"] | None
    uut_source: Literal["tag", "column"] | None
    uut_key: str
    issues: list[str]

    model_config = ConfigDict(populate_by_name=True)

    def dump(self) -> dict:
        return self.model_dump(by_alias=True)


_model_cache: dict[str, tuple[float, DataModel]] = {}
_source_cache: dict[tuple[str, str, str, str], tuple[float, Source]] = {}
_lock = threading.Lock()


def config_path() -> str:
    return f"{settings.shared_root}/config.yaml"


def parse(text: str) -> DataModel:
    try:
        raw = yaml.safe_load(text) or {}
    except yaml.YAMLError as e:
        raise ApiError(400, "invalid_yaml", f"YAML syntax error: {e}") from None
    if not isinstance(raw, dict):
        raise ApiError(400, "invalid_config", "The data model must be a mapping of settings")
    try:
        return DataModel.model_validate(raw)
    except ValidationError as e:
        problems = "; ".join(f"{'.'.join(str(p) for p in err['loc']) or 'config'}: {err['msg']}" for err in e.errors())
        raise ApiError(400, "invalid_config", problems) from None


def read_text(user: User) -> str | None:
    try:
        return user.client.workspace.download(config_path()).read().decode()
    except (NotFound, PermissionDenied):
        return None


def load(user: User) -> DataModel:
    now = time.monotonic()
    with _lock:
        hit = _model_cache.get(user.email)
    if hit and now - hit[0] < CACHE_SECONDS:
        return hit[1]
    text = read_text(user)
    if text is None:
        model = DataModel()
    else:
        try:
            model = parse(text)
        except ApiError as e:
            raise ApiError(500, "invalid_config", f"{config_path()} is invalid: {e.message}") from None
    with _lock:
        _model_cache[user.email] = (now, model)
    return model


def _shown(value: object) -> str:
    if value in ("", None, {}, []):
        return "empty"
    return f"[{', '.join(value)}]" if isinstance(value, list) else str(value)


def reference(model: type[BaseModel] = DataModel, prefix: str = "") -> list[dict]:
    rows = []
    for name, field in model.model_fields.items():
        if isinstance(field.annotation, type) and issubclass(field.annotation, BaseModel):
            rows += reference(field.annotation, f"{prefix}{name}.")
            continue
        rows.append(
            {
                "key": prefix + name,
                "default": _shown(field.get_default(call_default_factory=True)),
                "description": field.description,
                "example": field.examples[0] if field.examples else None,
            }
        )
    return rows


def save(user: User, text: str) -> DataModel:
    model = parse(text)
    user.client.workspace.mkdirs(settings.shared_root)
    user.client.workspace.upload(config_path(), io.BytesIO(text.encode()), format=ImportFormat.AUTO, overwrite=True)
    with _lock:
        _model_cache.clear()
        _source_cache.clear()
    return model


def _full_name(catalog: str, schema: str, table: str) -> str:
    return table if "." in table else f"{catalog}.{schema}.{table}"


def _columns(user: User, full_name: str) -> list[str] | None:
    try:
        return [c.name for c in user.client.tables.get(full_name).columns or []]
    except NotFound:
        return None


def _missing(model: DataModel, table: str, columns: list[str], internal: list[str]) -> list[str]:
    return [model.physical(table, c) for c in internal if model.physical(table, c) not in columns]


def resolve(user: User, model: DataModel, catalog: str, schema: str) -> Source:
    key = (user.email, catalog, schema, model.model_dump_json())
    now = time.monotonic()
    with _lock:
        hit = _source_cache.get(key)
    if hit and now - hit[0] < CACHE_SECONDS:
        return hit[1]

    configured = {k: v for k, v in model.tables.model_dump().items() if v}
    names = {k: _full_name(catalog, schema, v) for k, v in configured.items()}
    with ThreadPoolExecutor(max_workers=6) as pool:
        found = dict(zip(names, pool.map(lambda n: _columns(user, n), names.values())))

    issues = [f"Table {names[t]} not found" for t in REQUIRED if found.get(t) is None]
    tables = {t: (names[t] if found.get(t) is not None else None) for t in model.tables.model_dump()}
    columns = {t: cols for t, cols in found.items() if cols is not None}

    data_type = model.data_type
    channel_cols = columns.get("channels", [])
    if data_type is None and channel_cols:
        if model.physical("channels", "tend") in channel_cols:
            data_type = "RLE"
        elif model.physical("channels", "timestamp") in channel_cols:
            data_type = "RAW"
        else:
            issues.append("Cannot detect RAW or RLE: channels has neither timestamp nor tstart/tend")

    uut_source = model.uut.source
    uut_column = model.physical("container_metrics", model.uut.key)
    if uut_source is None:
        if uut_column in columns.get("container_metrics", []):
            uut_source = "column"
        elif tables.get("container_tags"):
            uut_source = "tag"
        else:
            issues.append(
                f"No unit under test id: container_metrics has no {uut_column} column and there is no container_tags table. "
                "Set uut.key on the Data model page."
            )

    checks = {
        "container_metrics": ["container_id", model.time_columns.start, model.time_columns.stop],
        "channel_metrics": ["container_id", "channel_id"],
        "channels": ["container_id", "channel_id", "value"]
        + (["tstart", "tend"] if data_type == "RLE" else ["timestamp"] if data_type == "RAW" else []),
        "container_tags": ["container_id", "key", "value"],
        "channel_tags": ["container_id", "channel_id", "key", "value"],
    }
    for table, internal in checks.items():
        if table in columns:
            missing = _missing(model, table, columns[table], internal)
            if missing:
                issues.append(f"{names[table]} is missing columns: {', '.join(missing)}")

    source = Source(
        catalog=catalog,
        schema=schema,
        tables=tables,
        columns=columns,
        data_type=data_type,
        uut_source=uut_source,
        uut_key=uut_column if uut_source == "column" else model.uut.key,
        issues=issues,
    )
    with _lock:
        _source_cache[key] = (now, source)
    return source


def _where(model: DataModel, table: str, alias: str, params: dict[str, str]) -> list[str]:
    clauses = []
    for i, (internal, value) in enumerate(model.filters(table).items()):
        name = f"{table}_f{i}"
        params[name] = str(value)
        clauses.append(f"{alias}.{sql.ident(model.physical(table, internal))} = :{name}")
    return clauses


def uuts_query(model: DataModel, source: Source) -> tuple[str, dict[str, str]]:
    def col(table: str, internal: str) -> str:
        return sql.ident(model.physical(table, internal))

    def table(name: str) -> str:
        return sql.ident(*source.tables[name].split("."))

    start = col("container_metrics", model.time_columns.start)
    stop = col("container_metrics", model.time_columns.stop)
    container_id = col("container_metrics", "container_id")
    params: dict[str, str] = {}
    where = _where(model, "container_metrics", "cm", params)
    span = f"CAST(MIN(cm.{start}) AS STRING) AS first, CAST(MAX(cm.{stop}) AS STRING) AS last"

    if source.uut_source == "column":
        uut = col("container_metrics", model.uut.key)
        clauses = [f"cm.{uut} IS NOT NULL", *where]
        statement = (
            f"SELECT CAST(cm.{uut} AS STRING) AS id, COUNT(*) AS containers, {span} "
            f"FROM {table('container_metrics')} cm WHERE {' AND '.join(clauses)} GROUP BY 1 ORDER BY 1"
        )
    else:
        params["uut_key"] = model.uut.key
        clauses = [
            f"t.{col('container_tags', 'key')} = :uut_key",
            f"t.{col('container_tags', 'value')} IS NOT NULL",
            *where,
            *_where(model, "container_tags", "t", params),
        ]
        statement = (
            f"SELECT t.{col('container_tags', 'value')} AS id, COUNT(DISTINCT cm.{container_id}) AS containers, {span} "
            f"FROM {table('container_tags')} t JOIN {table('container_metrics')} cm "
            f"ON cm.{container_id} = t.{col('container_tags', 'container_id')} "
            f"WHERE {' AND '.join(clauses)} GROUP BY 1 ORDER BY 1"
        )
    return statement, params


def uuts(user: User, model: DataModel, source: Source) -> list[dict]:
    if source.issues or source.uut_source is None:
        return []
    rows = sql.query(user, *uuts_query(model, source))
    return [{"id": r["id"], "containers": int(r["containers"]), "first": r["first"], "last": r["last"]} for r in rows]


def _containers(model: DataModel, source: Source, uuts: list[str], window: Window, params: dict[str, str]) -> str:
    def col(table: str, internal: str) -> str:
        return sql.ident(model.physical(table, internal))

    container_id = col("container_metrics", "container_id")
    where = _where(model, "container_metrics", "cm", params)
    for key, op, column in (("start", ">=", model.time_columns.start), ("stop", "<=", model.time_columns.stop)):
        value = getattr(window, key)
        if value:
            params[f"w_{key}"] = value
            where.append(f"CAST(cm.{col('container_metrics', column)} AS TIMESTAMP) {op} CAST(:w_{key} AS TIMESTAMP)")
    ids = []
    for i, v in enumerate(uuts):
        params[f"v{i}"] = v
        ids.append(f":v{i}")
    statement = f"SELECT DISTINCT cm.{container_id} AS container_id FROM {sql.ident(*source.tables['container_metrics'].split('.'))} cm"
    if ids and source.uut_source == "column":
        where.append(f"CAST(cm.{col('container_metrics', model.uut.key)} AS STRING) IN ({', '.join(ids)})")
    elif ids:
        params["uut_key"] = model.uut.key
        statement += (
            f" JOIN {sql.ident(*source.tables['container_tags'].split('.'))} t"
            f" ON t.{col('container_tags', 'container_id')} = cm.{container_id}"
        )
        where += [
            f"t.{col('container_tags', 'key')} = :uut_key",
            f"t.{col('container_tags', 'value')} IN ({', '.join(ids)})",
            *_where(model, "container_tags", "t", params),
        ]
    return statement + (f" WHERE {' AND '.join(where)}" if where else "")


def channels_query(model: DataModel, source: Source, uuts: list[str], window: Window) -> tuple[str, dict[str, str]]:
    def col(table: str, internal: str) -> str:
        return sql.ident(model.physical(table, internal))

    def table(name: str) -> str:
        return sql.ident(*source.tables[name].split("."))

    params: dict[str, str] = {}
    containers = _containers(model, source, uuts, window, params)
    identity = model.channel_identity
    aliases = [f"i{i}" for i in range(len(identity))]
    unit = model.unit_key if model.unit_key and model.unit_key not in identity else None
    metric_columns = source.columns.get("channel_metrics", [])

    if source.tables.get("channel_tags"):
        keys = [*identity, *([unit] if unit else [])]
        names = [*aliases, *(["u"] if unit else [])]
        for i, k in enumerate(keys):
            params[f"k{i}"] = k
        key, value = col("channel_tags", "key"), col("channel_tags", "value")
        pivots = ", ".join(f"MAX(CASE WHEN t.{key} = :k{i} THEN t.{value} END) AS {n}" for i, n in enumerate(names))
        where = [f"t.{key} IN ({', '.join(f':k{i}' for i in range(len(keys)))})", *_where(model, "channel_tags", "t", params)]
        tagged = (
            f"SELECT t.{col('channel_tags', 'container_id')} AS container_id, t.{col('channel_tags', 'channel_id')} AS channel_id, {pivots} "
            f"FROM {table('channel_tags')} t JOIN c ON t.{col('channel_tags', 'container_id')} = c.container_id "
            f"WHERE {' AND '.join(where)} GROUP BY 1, 2"
        )
    else:
        unit_column = unit if unit and unit in metric_columns else None
        selected = ", ".join(f"CAST(m.{col('channel_metrics', k)} AS STRING) AS {a}" for k, a in zip(identity, aliases))
        if unit_column:
            selected += f", CAST(m.{sql.ident(unit_column)} AS STRING) AS u"
        unit = unit_column
        where = _where(model, "channel_metrics", "m", params)
        tagged = (
            f"SELECT m.{col('channel_metrics', 'container_id')} AS container_id, m.{col('channel_metrics', 'channel_id')} AS channel_id, {selected} "
            f"FROM {table('channel_metrics')} m JOIN c ON m.{col('channel_metrics', 'container_id')} = c.container_id"
            + (f" WHERE {' AND '.join(where)}" if where else "")
        )

    stats = "min" in metric_columns and "max" in metric_columns
    joins = ""
    if stats:
        joins += (
            f" LEFT JOIN {table('channel_metrics')} m ON m.{col('channel_metrics', 'container_id')} = x.container_id"
            f" AND m.{col('channel_metrics', 'channel_id')} = x.channel_id"
        )
    filters = [f"x.{a} IS NOT NULL" for a in aliases]
    columns = [*(f"x.{a}" for a in aliases), "COUNT(DISTINCT x.container_id) AS containers"]
    if unit:
        columns.append("FIRST(x.u, true) AS unit")
    if stats:
        columns += ["MIN(m.`min`) AS min", "MAX(m.`max`) AS max"]
    statement = (
        f"WITH c AS ({containers}), x AS ({tagged}) "
        f"SELECT {', '.join(columns)} FROM x{joins} WHERE {' AND '.join(filters)} "
        f"GROUP BY {', '.join(f'x.{a}' for a in aliases)} ORDER BY {', '.join(f'x.{a}' for a in aliases)} LIMIT {MAX_CHANNELS}"
    )
    return statement, params


def _number(value: str | None) -> float | None:
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def channels(user: User, model: DataModel, source: Source, uuts: list[str], window: Window) -> list[dict]:
    if source.issues or source.uut_source is None:
        return []
    rows = sql.query(user, *channels_query(model, source, uuts, window))
    return [
        {
            "channel": {k: r[f"i{i}"] for i, k in enumerate(model.channel_identity)},
            "unit": r.get("unit") or "",
            "containers": int(r["containers"]),
            "min": _number(r.get("min")),
            "max": _number(r.get("max")),
        }
        for r in rows
    ]
