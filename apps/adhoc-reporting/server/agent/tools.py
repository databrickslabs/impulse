import difflib
import json
import math
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Literal

from databricks.sdk.errors import DatabricksError
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from server import catalog as uc
from server import codegen, datamodel, results, runs, sql, store
from server.agent import skills
from server.errors import ApiError
from server.settings import settings
from server.spec import NAME_RE, STATISTICS, ReportSpec, analyze
from server.user import User

MAX_ROWS = 50
CHANNEL_CACHE_SECONDS = 300


class ToolError(Exception):
    pass


@dataclass
class Context:
    user: User
    report: str | None
    emit: Callable[[dict], None]


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    args: type[BaseModel]
    run: Callable[[Context, BaseModel], dict]
    needs_report: bool


TOOLS: dict[str, Tool] = {}


class Args(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class NoArgs(Args):
    pass


def tool(name: str, description: str, args: type[BaseModel] = NoArgs, needs_report: bool = True):
    def register(fn: Callable[[Context, BaseModel], dict]) -> Callable[[Context, BaseModel], dict]:
        TOOLS[name] = Tool(name, " ".join(description.split()), args, fn, needs_report)
        return fn

    return register


def _strip_titles(node: object) -> object:
    if isinstance(node, dict):
        return {k: _strip_titles(v) for k, v in node.items() if k != "title"}
    if isinstance(node, list):
        return [_strip_titles(v) for v in node]
    return node


def schemas() -> list[dict]:
    return [
        {"type": "function", "function": {"name": t.name, "description": t.description, "parameters": _strip_titles(t.args.model_json_schema())}}
        for t in TOOLS.values()
    ]


def _validation(error: ValidationError) -> str:
    return "; ".join(f"{'.'.join(str(p) for p in e['loc']) or 'value'}: {e['msg']}" for e in error.errors())


def execute(ctx: Context, name: str, arguments: str) -> dict:
    found = TOOLS.get(name)
    if found is None:
        return {"error": f"Unknown tool {name}"}
    try:
        args = found.args.model_validate(json.loads(arguments or "{}"))
    except ValidationError as e:
        return {"error": f"Invalid arguments: {_validation(e)}"}
    except ValueError:
        return {"error": "Arguments must be a JSON object"}
    if found.needs_report and not ctx.report:
        return {"error": "No report is open. Use list_reports and open_report, or create_report."}
    try:
        return found.run(ctx, args)
    except ToolError as e:
        return {"error": str(e)}
    except ApiError as e:
        return {"error": e.message}
    except DatabricksError as e:
        return {"error": str(e).split(" Config:")[0]}


def _spec(ctx: Context) -> ReportSpec:
    return store.get(ctx.user, ctx.report)


def _change(ctx: Context, mutate: Callable[[dict], None]) -> ReportSpec:
    for attempt in range(2):
        current = _spec(ctx)
        data = current.dump()
        mutate(data)
        try:
            updated = ReportSpec.model_validate(data)
        except ValidationError as e:
            raise ToolError(_validation(e)) from None
        try:
            saved = store.save(ctx.user, updated, expected_revision=current.revision)
        except ApiError as e:
            if e.status == 409 and attempt == 0:
                continue
            raise
        ctx.emit({"type": "spec", "report": ctx.report, "revision": saved.revision})
        return saved
    raise ToolError("The report kept changing while saving; try again")


def _issues(spec: ReportSpec, step: str | None = None) -> list[str]:
    return [i.message for i in analyze(spec).issues if step is None or i.step == step]


def summary(spec: ReportSpec) -> dict:
    def aggregation(a) -> dict:
        base = {"name": a.name, "kind": a.kind, "event": a.event or None}
        if a.kind == "histogram":
            return {**base, "signal": a.signal, "weight": a.weight, "distance": a.distance or None, "bins": _bins_text(a.bins)}
        if a.kind == "histogram_2d":
            return {**base, "x": a.x, "y": a.y, "x_bins": _bins_text(a.x_bins), "y_bins": _bins_text(a.y_bins)}
        if a.kind == "statistics":
            return {**base, "signals": a.signals, "statistics": list(a.statistics)}
        return {**base, "signals": a.signals}

    return {
        "report": spec.name,
        "description": spec.description,
        "source": f"{spec.source.catalog}.{spec.source.schema_}" if spec.source.catalog else None,
        "output": f"{spec.sink.catalog}.{spec.sink.schema_}.{spec.sink.table_prefix}_*" if spec.sink.catalog else None,
        "uut_count": len(spec.uuts),
        "uuts": spec.uuts[:MAX_ROWS],
        "window": spec.window.model_dump(),
        "signals": [
            {"name": s.name, "channel": s.channel} if s.kind == "channel" else {"name": s.name, "expr": s.expr}
            for s in spec.signals
        ],
        "events": [{"name": e.name, "kind": e.kind, "expr": e.expr} for e in spec.events],
        "aggregations": [aggregation(a) for a in spec.aggregations],
        "issues": _issues(spec),
    }


def _bins_text(bins: list[float]) -> str:
    if len(bins) < 2:
        return f"{bins}"
    step = bins[1] - bins[0]
    if all(abs(b - a - step) < 1e-9 * max(1.0, abs(step)) for a, b in zip(bins, bins[1:])):
        return f"{len(bins) - 1} bins from {bins[0]:g} to {bins[-1]:g} step {step:g}"
    return f"{len(bins) - 1} bins: {', '.join(f'{b:g}' for b in bins)}"


def nice_bins(lo: float, hi: float, target: int = 20) -> list[float]:
    if not (math.isfinite(lo) and math.isfinite(hi)) or hi <= lo:
        return [lo, lo + 1]
    rough = (hi - lo) / target
    magnitude = 10 ** math.floor(math.log10(rough))
    step = next(m * magnitude for m in (1, 2, 2.5, 5, 10) if m * magnitude >= rough)
    return regular_bins(math.floor(lo / step) * step, math.ceil(hi / step) * step, step)


def regular_bins(start: float, stop: float, step: float) -> list[float]:
    if step <= 0 or stop <= start:
        raise ToolError("Bins need start < stop and a positive step")
    count = round((stop - start) / step)
    if count > 499:
        raise ToolError("That makes more than 499 bins; use a larger step")
    digits = max(0, -math.floor(math.log10(step)) + 3)
    return [round(start + i * step, digits) for i in range(count + 1)]


def _slug(text: str, taken: set[str]) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")[:40] or "signal"
    if not re.match(r"^[a-z]", base):
        base = f"s_{base}"
    name, i = base, 2
    while name in taken:
        name, i = f"{base}_{i}", i + 1
    return name


_channel_cache: dict[tuple, tuple[float, list[dict]]] = {}
_cache_lock = threading.Lock()


def _source(ctx: Context, spec: ReportSpec):
    if not (spec.source.catalog and spec.source.schema_):
        raise ToolError("Choose the data first with set_data")
    model = datamodel.load(ctx.user)
    found = datamodel.resolve(ctx.user, model, spec.source.catalog, spec.source.schema_)
    return model, found


def _channels(ctx: Context, spec: ReportSpec) -> list[dict]:
    key = (ctx.user.email, spec.source.catalog, spec.source.schema_, tuple(spec.uuts), spec.window.start, spec.window.stop)
    with _cache_lock:
        hit = _channel_cache.get(key)
    if hit and time.monotonic() - hit[0] < CHANNEL_CACHE_SECONDS:
        return hit[1]
    model, found = _source(ctx, spec)
    if found.issues:
        raise ToolError("; ".join(found.issues))
    rows = datamodel.channels(ctx.user, model, found, spec.uuts, spec.window)
    for r in rows:
        r["label"] = " · ".join(v for v in r["channel"].values() if v)
    with _cache_lock:
        _channel_cache[key] = (time.monotonic(), rows)
    return rows


class ReportName(Args):
    name: str = Field(description="Report name: lowercase letters, digits and underscores")


class NewReport(ReportName):
    description: str = ""


@tool("list_reports", "List the user's saved reports.", needs_report=False)
def list_reports(ctx: Context, args: NoArgs) -> dict:
    return {"reports": store.list_reports(ctx.user)}


@tool("create_report", "Create a new report and open it. Its gold table prefix defaults to its name.", NewReport, needs_report=False)
def create_report(ctx: Context, args: NewReport) -> dict:
    if not NAME_RE.match(args.name):
        raise ToolError("Report names use lowercase letters, digits and underscores and start with a letter")
    model = datamodel.load(ctx.user)
    spec = ReportSpec.model_validate(
        {
            "name": args.name,
            "description": args.description,
            "source": {"catalog": model.silver_catalog, "schema": model.silver_schema},
            "sink": {
                "catalog": model.gold_catalog or model.silver_catalog,
                "schema": model.gold_schema or model.silver_schema,
                "table_prefix": args.name,
            },
            "framework_version": settings.framework["version"],
        }
    )
    created = store.create(ctx.user, spec)
    ctx.report = created.name
    ctx.emit({"type": "goto", "report": created.name, "step": "data"})
    return {"opened": created.name, "report": summary(created)}


@tool("open_report", "Open an existing report so later tools act on it.", ReportName, needs_report=False)
def open_report(ctx: Context, args: ReportName) -> dict:
    spec = store.get(ctx.user, args.name)
    ctx.report = spec.name
    ctx.emit({"type": "goto", "report": spec.name, "step": "data"})
    return {"opened": spec.name, "report": summary(spec)}


@tool("get_report", "Get the open report: data, units under test, signals, events, aggregations and open issues.")
def get_report(ctx: Context, args: NoArgs) -> dict:
    return summary(_spec(ctx))


@tool("list_catalogs", "List Unity Catalog catalogs the user can see.", needs_report=False)
def list_catalogs(ctx: Context, args: NoArgs) -> dict:
    return {"catalogs": uc.catalogs(ctx.user)}


class CatalogArgs(Args):
    catalog: str


@tool("list_schemas", "List the schemas of a catalog.", CatalogArgs, needs_report=False)
def list_schemas(ctx: Context, args: CatalogArgs) -> dict:
    return {"catalog": args.catalog, "schemas": uc.schemas(ctx.user, args.catalog)}


class DataArgs(Args):
    catalog: str = Field(description="Silver catalog with the Impulse tables")
    schema_: str = Field(alias="schema", description="Silver schema with the Impulse tables")
    output_catalog: str = Field("", description="Catalog for gold tables; defaults to the silver catalog")
    output_schema: str = Field("", description="Schema for gold tables; defaults to the silver schema")
    table_prefix: str = Field("", description="Gold table prefix; defaults to the report name")


@tool(
    "set_data",
    "Choose the silver schema the report reads and where it writes gold tables. Checks the schema for the Impulse tables and returns what was detected. Changing the source clears the unit under test selection.",
    DataArgs,
)
def set_data(ctx: Context, args: DataArgs) -> dict:
    model = datamodel.load(ctx.user)
    found = datamodel.resolve(ctx.user, model, args.catalog, args.schema_)

    def mutate(d: dict) -> None:
        old, sink = d["source"], d["sink"]
        follows = (not sink["catalog"] and not sink["schema"]) or (sink["catalog"] == old["catalog"] and sink["schema"] == old["schema"])
        if (old["catalog"], old["schema"]) != (args.catalog, args.schema_):
            d["uuts"] = []
            d["window"] = {"start": None, "stop": None}
        d["source"] = {"catalog": args.catalog, "schema": args.schema_}
        if args.output_catalog or args.output_schema:
            sink["catalog"] = args.output_catalog or args.catalog
            sink["schema"] = args.output_schema or args.schema_
        elif follows:
            sink["catalog"], sink["schema"] = args.catalog, args.schema_
        if args.table_prefix:
            sink["table_prefix"] = args.table_prefix

    spec = _change(ctx, mutate)
    return {
        "source": f"{args.catalog}.{args.schema_}",
        "output": f"{spec.sink.catalog}.{spec.sink.schema_}.{spec.sink.table_prefix}_*",
        "tables": {k: v for k, v in found.tables.items() if v},
        "data_type": found.data_type,
        "uuts_by": found.uut_source,
        "issues": found.issues,
    }


class UutQuery(Args):
    query: str = Field("", description="Text the unit under test id contains; empty lists all")


@tool(
    "list_uuts",
    "List the units under test (vehicles, machines, test benches) in the report's data with recording counts and first and last "
    f"recording times, at most {MAX_ROWS} per call.",
    UutQuery,
)
def list_uuts(ctx: Context, args: UutQuery) -> dict:
    spec = _spec(ctx)
    model, found = _source(ctx, spec)
    if found.issues:
        raise ToolError("; ".join(found.issues))
    rows = datamodel.uuts(ctx.user, model, found)
    matches = [r for r in rows if args.query.lower() in r["id"].lower()]
    return {
        "total": len(rows),
        "selected": len(spec.uuts),
        "window": spec.window.model_dump(),
        "uuts": matches[:MAX_ROWS],
        "more": max(0, len(matches) - MAX_ROWS),
    }


class UutArgs(Args):
    uuts: list[str] = Field(default_factory=list, description="Unit under test ids exactly as returned by list_uuts")
    all: bool = Field(False, description="Select every unit under test instead of listing ids")
    start: str | None = Field(None, description="Optional window start in the format of first and last from list_uuts, for example 2026-01-09 00:00:00")
    stop: str | None = Field(None, description="Optional window stop in the same format")


@tool(
    "set_uuts",
    "Select the units under test and an optional time window. Only recordings that start and stop inside the window are included.",
    UutArgs,
)
def set_uuts(ctx: Context, args: UutArgs) -> dict:
    spec = _spec(ctx)
    model, found = _source(ctx, spec)
    known = [r["id"] for r in datamodel.uuts(ctx.user, model, found)]
    chosen = known if args.all else list(dict.fromkeys(args.uuts))
    unknown = sorted(set(chosen) - set(known))
    if unknown:
        raise ToolError(f"Unknown units under test: {', '.join(unknown)}. Find ids with list_uuts.")
    for value in (args.start, args.stop):
        if value:
            try:
                datetime.fromisoformat(value)
            except ValueError:
                raise ToolError(f"{value} is not an ISO date and time") from None

    def mutate(d: dict) -> None:
        d["uuts"] = chosen
        d["window"] = {"start": args.start, "stop": args.stop}

    saved = _change(ctx, mutate)
    return {
        "selected": len(saved.uuts),
        "uuts": saved.uuts[:MAX_ROWS],
        "window": saved.window.model_dump(),
        "issues": _issues(saved, "uuts"),
    }


class SearchArgs(Args):
    query: str = Field("", description="Words to look for in channel names, for example 'rpm' or 'coolant temp'. Separate alternatives with commas. Empty lists all.")
    limit: int = Field(25, ge=1, le=100)


@tool(
    "search_channels",
    "Search the channels recorded for the selected units under test and window. Returns each channel's identity, unit, how many recordings contain it, and its min and max. Only channels returned here can be added.",
    SearchArgs,
)
def search_channels(ctx: Context, args: SearchArgs) -> dict:
    spec = _spec(ctx)
    rows = _channels(ctx, spec)
    alternatives = [t.split() for t in args.query.lower().split(",") if t.strip()]
    matches = [r for r in rows if not alternatives or any(all(w in r["label"].lower() for w in words) for words in alternatives)]
    if not matches and alternatives:
        labels = [r["label"] for r in rows]
        close = difflib.get_close_matches(args.query, labels, n=8, cutoff=0.3)
        matches = [r for r in rows if r["label"] in close]
    recordings = max((r["containers"] for r in rows), default=0)
    return {
        "total_channels": len(rows),
        "recordings": recordings,
        "matches": [
            {"channel": r["channel"], "unit": r["unit"], "recordings": r["containers"], "min": r["min"], "max": r["max"]}
            for r in matches[: args.limit]
        ],
        "more": max(0, len(matches) - args.limit),
    }


class ChannelPick(Args):
    channel: str | dict[str, str] = Field(description="Channel name as returned by search_channels, or its full identity object")
    name: str = Field("", description="Signal name, lowercase with underscores; derived from the channel name when empty")
    unit: str = Field("", description="Unit; taken from the channel when empty")


class ChannelArgs(Args):
    channels: list[ChannelPick] = Field(min_length=1)


@tool("add_channel_signals", "Add channels returned by search_channels as signals.", ChannelArgs)
def add_channel_signals(ctx: Context, args: ChannelArgs) -> dict:
    spec = _spec(ctx)
    rows = _channels(ctx, spec)
    taken = {s.name for s in spec.signals}
    existing = {json.dumps(s.channel, sort_keys=True): s.name for s in spec.signals if s.kind == "channel"}
    added, skipped = [], []
    for pick in args.channels:
        if isinstance(pick.channel, dict):
            match = next((r for r in rows if r["channel"] == pick.channel), None)
        else:
            text = pick.channel.strip().lower()
            match = next((r for r in rows if r["label"].lower() == text), None)
        if match is None:
            label = pick.channel if isinstance(pick.channel, str) else " · ".join(pick.channel.values())
            close = difflib.get_close_matches(label, [r["label"] for r in rows], n=5, cutoff=0.3)
            raise ToolError(f"Channel {label} is not in the selected recordings. Close matches: {', '.join(close) or 'none'}. Use search_channels.")
        key = json.dumps(match["channel"], sort_keys=True)
        if key in existing:
            skipped.append({"channel": match["label"], "signal": existing[key]})
            continue
        name = pick.name or _slug(match["label"], taken)
        taken.add(name)
        existing[key] = name
        added.append(
            {
                "kind": "channel",
                "name": name,
                "channel": match["channel"],
                "unit": pick.unit or match["unit"],
                "desc": "",
                "min": match["min"],
                "max": match["max"],
            }
        )
    if added:
        _change(ctx, lambda d: d["signals"].extend(added))
    return {
        "added": [{"name": a["name"], "channel": a["channel"], "unit": a["unit"], "min": a["min"], "max": a["max"]} for a in added],
        "already_added": skipped,
    }


class DerivedArgs(Args):
    name: str
    expr: str = Field(description="TSAL expression over existing signal names that gives a signal, for example (amb_temp + intake_temp) / 2")
    unit: str = ""


@tool("add_derived_signal", "Add a signal computed from other signals with arithmetic or signal methods. Replaces a derived signal of the same name.", DerivedArgs)
def add_derived_signal(ctx: Context, args: DerivedArgs) -> dict:
    def mutate(d: dict) -> None:
        current = next((s for s in d["signals"] if s["name"] == args.name), None)
        if current and current["kind"] == "channel":
            raise ToolError(f"{args.name} is a channel signal; choose another name")
        entry = {"kind": "expression", "name": args.name, "expr": args.expr, "unit": args.unit, "desc": ""}
        if current:
            current.update(entry)
        else:
            d["signals"].append(entry)

    saved = _change(ctx, mutate)
    return {"signal": args.name, "issues": [m for m in _issues(saved, "signals") if args.name in m]}


class NamedArgs(Args):
    name: str


@tool("remove_signal", "Remove a signal. Aggregations and expressions that use it will show issues.", NamedArgs)
def remove_signal(ctx: Context, args: NamedArgs) -> dict:
    spec = _spec(ctx)
    if not any(s.name == args.name for s in spec.signals):
        raise ToolError(f"No signal {args.name}")
    saved = _change(ctx, lambda d: d.update(signals=[s for s in d["signals"] if s["name"] != args.name]))
    return {"removed": args.name, "issues": _issues(saved)}


class EventArgs(Args):
    name: str
    kind: Literal["condition", "points"] = Field(
        description="condition: time windows where an expression holds, for example eng_rpm > 3000. points: instants such as veh_spd.rising_edges()"
    )
    expr: str
    desc: str = ""


@tool("add_event", "Add or replace an event that scopes aggregations.", EventArgs)
def add_event(ctx: Context, args: EventArgs) -> dict:
    def mutate(d: dict) -> None:
        entry = {"kind": args.kind, "name": args.name, "expr": args.expr, "desc": args.desc}
        current = next((e for e in d["events"] if e["name"] == args.name), None)
        if current:
            current.update(entry)
        else:
            d["events"].append(entry)

    saved = _change(ctx, mutate)
    return {"event": args.name, "issues": [m for m in _issues(saved, "signals") if args.name in m]}


@tool("remove_event", "Remove an event. Histograms and statistics scoped to it switch to the whole series.", NamedArgs)
def remove_event(ctx: Context, args: NamedArgs) -> dict:
    spec = _spec(ctx)
    if not any(e.name == args.name for e in spec.events):
        raise ToolError(f"No event {args.name}")

    def mutate(d: dict) -> None:
        d["events"] = [e for e in d["events"] if e["name"] != args.name]
        for a in d["aggregations"]:
            if a["event"] == args.name and a["kind"] != "point_values":
                a["event"] = ""

    saved = _change(ctx, mutate)
    return {"removed": args.name, "issues": _issues(saved)}


def _resolve_bins(spec: ReportSpec, signal: str, bins: list[float] | None, start: float | None, stop: float | None, step: float | None) -> list[float]:
    if bins:
        return bins
    if start is not None and stop is not None and step is not None:
        return regular_bins(start, stop, step)
    s = next((x for x in spec.signals if x.name == signal), None)
    if s is not None and s.kind == "channel" and s.min is not None and s.max is not None:
        return nice_bins(s.min, s.max)
    raise ToolError(f"Give bins, or start, stop and step, for {signal}; its value range is unknown")


def _put_aggregation(ctx: Context, entry: dict) -> dict:
    def mutate(d: dict) -> None:
        current = next((i for i, a in enumerate(d["aggregations"]) if a["name"] == entry["name"]), None)
        if current is None:
            d["aggregations"].append(entry)
        else:
            d["aggregations"][current] = entry

    saved = _change(ctx, mutate)
    built = next(a for a in summary(saved)["aggregations"] if a["name"] == entry["name"])
    return {"aggregation": built, "issues": [m for m in _issues(saved, "aggregations") if entry["name"] in m]}


SCOPE = "Event name for a condition event; empty for the whole series"


class HistogramArgs(Args):
    name: str
    signal: str
    bins: list[float] | None = Field(None, description="Explicit bin edges; N edges give N-1 bins")
    start: float | None = None
    stop: float | None = None
    step: float | None = None
    event: str = Field("", description=SCOPE)
    weight: Literal["duration", "distance"] = Field("duration", description="duration: time spent per bin; distance: distance travelled per bin")
    distance_signal: str = Field("", description="Cumulative distance signal, required for weight distance")
    bins_unit: str = ""
    desc: str = ""


@tool(
    "add_histogram",
    "Add or replace a 1D histogram of a signal. Give explicit bins or start, stop and step; without either, bins are fitted to the channel's range.",
    HistogramArgs,
)
def add_histogram(ctx: Context, args: HistogramArgs) -> dict:
    spec = _spec(ctx)
    entry = {
        "kind": "histogram",
        "name": args.name,
        "signal": args.signal,
        "bins": _resolve_bins(spec, args.signal, args.bins, args.start, args.stop, args.step),
        "event": args.event,
        "weight": args.weight,
        "distance": args.distance_signal,
        "bins_unit": args.bins_unit,
        "values_unit": "",
        "desc": args.desc,
    }
    return _put_aggregation(ctx, entry)


class HeatmapArgs(Args):
    name: str
    x: str
    y: str
    x_bins: list[float] | None = None
    x_start: float | None = None
    x_stop: float | None = None
    x_step: float | None = None
    y_bins: list[float] | None = None
    y_start: float | None = None
    y_stop: float | None = None
    y_step: float | None = None
    event: str = Field("", description=SCOPE)
    desc: str = ""


@tool("add_heatmap", "Add or replace a 2D histogram of time spent in each combination of two signals' ranges.", HeatmapArgs)
def add_heatmap(ctx: Context, args: HeatmapArgs) -> dict:
    spec = _spec(ctx)
    entry = {
        "kind": "histogram_2d",
        "name": args.name,
        "x": args.x,
        "y": args.y,
        "x_bins": _resolve_bins(spec, args.x, args.x_bins, args.x_start, args.x_stop, args.x_step),
        "y_bins": _resolve_bins(spec, args.y, args.y_bins, args.y_start, args.y_stop, args.y_step),
        "event": args.event,
        "x_unit": "",
        "y_unit": "",
        "values_unit": "",
        "desc": args.desc,
    }
    return _put_aggregation(ctx, entry)


class StatisticsArgs(Args):
    name: str
    signals: list[str] = Field(min_length=1)
    statistics: list[Literal["min", "max", "mean", "median", "start", "end"]] = Field(
        default_factory=lambda: ["min", "max", "mean"], description=f"Any of {', '.join(STATISTICS)}; mean and median are duration-weighted"
    )
    event: str = Field("", description=SCOPE + "; statistics are computed per event instance")
    desc: str = ""


@tool("add_statistics", "Add or replace per-event statistics of one or more signals.", StatisticsArgs)
def add_statistics(ctx: Context, args: StatisticsArgs) -> dict:
    entry = {
        "kind": "statistics",
        "name": args.name,
        "signals": args.signals,
        "statistics": list(dict.fromkeys(args.statistics)),
        "event": args.event,
        "values_unit": "",
        "desc": args.desc,
    }
    return _put_aggregation(ctx, entry)


class PointValuesArgs(Args):
    name: str
    signals: list[str] = Field(min_length=1)
    event: str = Field(description="A points-in-time event")
    desc: str = ""


@tool("add_point_values", "Add or replace signal values sampled at the instants of a points-in-time event.", PointValuesArgs)
def add_point_values(ctx: Context, args: PointValuesArgs) -> dict:
    entry = {"kind": "point_values", "name": args.name, "signals": args.signals, "event": args.event, "values_unit": "", "desc": args.desc}
    return _put_aggregation(ctx, entry)


@tool("remove_aggregation", "Remove an aggregation.", NamedArgs)
def remove_aggregation(ctx: Context, args: NamedArgs) -> dict:
    spec = _spec(ctx)
    if not any(a.name == args.name for a in spec.aggregations):
        raise ToolError(f"No aggregation {args.name}")
    saved = _change(ctx, lambda d: d.update(aggregations=[a for a in d["aggregations"] if a["name"] != args.name]))
    return {"removed": args.name, "issues": _issues(saved, "aggregations")}


@tool("validate_report", "Check the whole report against the data and the framework rules. Returns the open issues per step; an empty list means it can run.")
def validate_report(ctx: Context, args: NoArgs) -> dict:
    spec = _spec(ctx)
    model = datamodel.load(ctx.user)
    found = datamodel.resolve(ctx.user, model, spec.source.catalog, spec.source.schema_) if spec.source.catalog else None
    issues = codegen.issues(spec, found, model)
    return {"ready": not issues, "issues": [{"step": i.step, "item": i.item, "message": i.message} for i in issues]}


@tool("preview_code", "Show the Impulse notebook generated from the report.")
def preview_code(ctx: Context, args: NoArgs) -> dict:
    spec = _spec(ctx)
    model, found = _source(ctx, spec)
    generated = codegen.generate(spec, found, model)
    return {"notebook": generated.notebook[:12000]}


@tool(
    "run_report",
    "Run the report as a serverless job. Checks first that the user and the app can read the source and write the output. Only call this when the user asks to run.",
)
def run_report(ctx: Context, args: NoArgs) -> dict:
    spec = _spec(ctx)
    model, found = _source(ctx, spec)
    generated = codegen.generate(spec, found, model)
    checked = runs.preflight(ctx.user, generated.config)
    if not checked["ok"]:
        return {"started": False, "preflight": checked}
    started = runs.submit(ctx.user, spec.name, generated.notebook)
    ctx.emit({"type": "run", "report": spec.name, "run_id": started["run_id"]})
    return {"started": True, "run_id": started["run_id"], "note": "Serverless runs take one to three minutes; check with get_run_status."}


class RunStatusArgs(Args):
    run_id: int | None = Field(None, description="Run id; the latest run when empty")


@tool("get_run_status", "Get the state of the latest or a given run, including the error message of a failed run.", RunStatusArgs)
def get_run_status(ctx: Context, args: RunStatusArgs) -> dict:
    run_id = args.run_id
    if run_id is None:
        history = runs.history(ctx.user, ctx.report, limit=1)
        if not history:
            return {"runs": 0}
        run_id = history[0]["run_id"]
    detail = runs.detail(ctx.user, ctx.report, run_id)
    detail["error_message"] = detail.pop("error")
    detail["trace"] = (detail.get("trace") or "")[-3000:] or None
    return detail


@tool("get_results", "Summarize the report's gold-layer results: events, histogram bins, heatmap cells and statistics.")
def get_results(ctx: Context, args: NoArgs) -> dict:
    found = results.results(ctx.user, _spec(ctx), datamodel.load(ctx.user))
    if not found["available"]:
        return {"available": False}
    compact = []
    for a in found["aggregations"]:
        if a["kind"] == "histogram":
            compact.append({"name": a["name"], "unit": a["unit"], "bins": [[b["lo"], b["hi"], round(b["value"], 3)] for b in a["bins"]][:60]})
        elif a["kind"] == "histogram_2d":
            top = sorted(a["cells"], key=lambda c: c["value"], reverse=True)[:12]
            compact.append(
                {
                    "name": a["name"],
                    "unit": a["unit"],
                    "total": round(sum(c["value"] for c in a["cells"]), 3),
                    "top_cells": [[c["x_lo"], c["x_hi"], c["y_lo"], c["y_hi"], round(c["value"], 3)] for c in top],
                }
            )
        else:
            compact.append(
                {
                    "name": a["name"],
                    "event": a["event"] if a["scoped"] else None,
                    "columns": a["columns"],
                    "rows": a["rows"][:60],
                    "more": a["more"] or len(a["rows"]) > 60,
                }
            )
    return {"recordings": found["recordings"], "events": found["events"], "aggregations": compact}


class SqlArgs(Args):
    statement: str = Field(description="One read-only statement: SELECT, WITH, SHOW or DESCRIBE")


READ_ONLY = re.compile(r"^\s*(SELECT|WITH|SHOW|DESCRIBE|DESC)\b", re.I)


@tool(
    "run_sql",
    "Run one read-only SQL statement with the user's permissions, for example to inspect silver or gold tables. Returns at most 50 rows.",
    SqlArgs,
    needs_report=False,
)
def run_sql(ctx: Context, args: SqlArgs) -> dict:
    statement = args.statement.strip().rstrip(";").strip()
    if ";" in statement:
        raise ToolError("Only one statement is allowed")
    match = READ_ONLY.match(statement)
    if not match:
        raise ToolError("Only SELECT, WITH, SHOW and DESCRIBE statements are allowed")
    if match.group(1).upper() in ("SELECT", "WITH"):
        statement = f"SELECT * FROM ({statement}) AS q LIMIT {MAX_ROWS + 1}"
    rows = sql.query(ctx.user, statement, timeout_s=60)
    return {"rows": rows[:MAX_ROWS], "truncated": len(rows) > MAX_ROWS}


class StepArgs(Args):
    step: Literal["data", "uuts", "signals", "aggregations", "run"]


@tool("goto_step", "Show a step of the open report in the app.", StepArgs)
def goto_step(ctx: Context, args: StepArgs) -> dict:
    ctx.emit({"type": "goto", "report": ctx.report, "step": args.step})
    return {"shown": args.step}


class SkillArgs(Args):
    name: str


@tool("load_skill", "Load a skill's instructions by name from the skills index.", SkillArgs, needs_report=False)
def load_skill(ctx: Context, args: SkillArgs) -> dict:
    try:
        return skills.load(args.name)
    except KeyError as e:
        raise ToolError(str(e.args[0])) from None


class ReferenceArgs(Args):
    skill: str
    file: str = Field(description="Reference file path as listed by load_skill")


@tool("read_skill_reference", "Read one reference file of a skill.", ReferenceArgs, needs_report=False)
def read_skill_reference(ctx: Context, args: ReferenceArgs) -> dict:
    try:
        return skills.reference(args.skill, args.file)
    except KeyError as e:
        raise ToolError(str(e.args[0])) from None
