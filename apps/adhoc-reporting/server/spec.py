import keyword
import math
import re
from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from server import tsal

NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
SPEC_VERSION = 1
STATISTICS = ("min", "max", "mean", "median", "start", "end")
RESERVED = {
    "spark",
    "dbutils",
    "display",
    "report",
    "db",
    "page",
    "config",
    "sink",
    "table",
    "tables",
}
MAX_BINS = 500


def _name(value: str) -> str:
    if not NAME_RE.match(value):
        raise ValueError("Use lowercase letters, digits and underscores, starting with a letter")
    return value


def _signal_name(value: str) -> str:
    _name(value)
    if keyword.iskeyword(value) or keyword.issoftkeyword(value) or value in RESERVED or value.startswith("ev_"):
        raise ValueError(f"{value} is reserved, choose another name")
    return value


Name = Annotated[str, AfterValidator(_name)]
SignalName = Annotated[str, AfterValidator(_signal_name)]
Statistic = Literal["min", "max", "mean", "median", "start", "end"]


class Location(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog: str = ""
    schema_: str = Field("", alias="schema")


class Sink(Location):
    table_prefix: str = ""


class Window(BaseModel):
    start: str | None = None
    stop: str | None = None


class ChannelSignal(BaseModel):
    kind: Literal["channel"] = "channel"
    name: SignalName
    channel: dict[str, str]
    unit: str = ""
    desc: str = ""
    min: float | None = None
    max: float | None = None


class ExpressionSignal(BaseModel):
    kind: Literal["expression"] = "expression"
    name: SignalName
    expr: str
    unit: str = ""
    desc: str = ""


Signal = Annotated[ChannelSignal | ExpressionSignal, Field(discriminator="kind")]


class Event(BaseModel):
    kind: Literal["condition", "points"]
    name: Name
    expr: str
    desc: str = ""


class Histogram(BaseModel):
    kind: Literal["histogram"] = "histogram"
    name: Name
    signal: str
    bins: list[float]
    event: str = ""
    weight: Literal["duration", "distance"] = "duration"
    distance: str = ""
    bins_unit: str = ""
    values_unit: str = ""
    desc: str = ""


class Histogram2D(BaseModel):
    kind: Literal["histogram_2d"] = "histogram_2d"
    name: Name
    x: str
    y: str
    x_bins: list[float]
    y_bins: list[float]
    event: str = ""
    x_unit: str = ""
    y_unit: str = ""
    values_unit: str = ""
    desc: str = ""


class Statistics(BaseModel):
    kind: Literal["statistics"] = "statistics"
    name: Name
    signals: list[str]
    statistics: list[Statistic]
    event: str = ""
    values_unit: str = ""
    desc: str = ""


class PointValues(BaseModel):
    kind: Literal["point_values"] = "point_values"
    name: Name
    signals: list[str]
    event: str
    values_unit: str = ""
    desc: str = ""


Aggregation = Annotated[Histogram | Histogram2D | Statistics | PointValues, Field(discriminator="kind")]


class ReportSpec(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    spec_version: int = SPEC_VERSION
    name: Name
    description: str = ""
    owner: str = ""
    source: Location = Field(default_factory=Location)
    sink: Sink = Field(default_factory=Sink)
    uuts: list[str] = Field(default_factory=list)
    window: Window = Field(default_factory=Window)
    signals: list[Signal] = Field(default_factory=list)
    events: list[Event] = Field(default_factory=list)
    aggregations: list[Aggregation] = Field(default_factory=list)
    framework_version: str = ""
    revision: int = 0
    updated_at: str = ""

    def dump(self) -> dict:
        return self.model_dump(by_alias=True)


class Issue(BaseModel):
    step: Literal["data", "uuts", "signals", "aggregations"]
    item: str = ""
    message: str


class Analysis(BaseModel):
    issues: list[Issue]
    signal_order: list[str]
    code: dict[str, str]


def _bins_issue(bins: list[float]) -> str | None:
    if len(bins) < 2:
        return "needs at least two bin edges"
    if len(bins) > MAX_BINS:
        return f"has more than {MAX_BINS} bin edges"
    if not all(math.isfinite(b) for b in bins):
        return "bin edges must be finite numbers"
    if any(b >= a for a, b in zip(bins[1:], bins)):
        return "bin edges must be strictly increasing"
    return None


def _timestamp_issue(value: str | None) -> bool:
    if not value:
        return False
    try:
        datetime.fromisoformat(value)
        return False
    except ValueError:
        return True


def _instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed


def analyze(spec: ReportSpec) -> Analysis:
    issues: list[Issue] = []

    def add(step: str, item: str, message: str) -> None:
        issues.append(Issue(step=step, item=item, message=message))

    if not (spec.source.catalog and spec.source.schema_):
        add("data", "", "Choose a source catalog and schema")
    if not (spec.sink.catalog and spec.sink.schema_):
        add("data", "", "Choose an output catalog and schema")
    if not NAME_RE.match(spec.sink.table_prefix):
        add("data", "", "The table prefix must use lowercase letters, digits and underscores")

    if not spec.uuts:
        add("uuts", "", "Select at least one unit under test")
    if len(set(spec.uuts)) != len(spec.uuts):
        add("uuts", "", "A unit under test is selected twice")
    for key in ("start", "stop"):
        if _timestamp_issue(getattr(spec.window, key)):
            add("uuts", "", f"The window {key} is not a valid date and time")
    if spec.window.start and spec.window.stop and not _timestamp_issue(spec.window.start) and not _timestamp_issue(spec.window.stop):
        if _instant(spec.window.start) >= _instant(spec.window.stop):
            add("uuts", "", "The window must start before it stops")

    seen: set[str] = set()
    for s in spec.signals:
        if s.name in seen:
            add("signals", s.name, f"Signal {s.name} is defined twice")
        seen.add(s.name)
    channels = {s.name for s in spec.signals if s.kind == "channel"}
    for s in spec.signals:
        if s.kind == "channel" and not any(v for v in s.channel.values()):
            add("signals", s.name, f"Signal {s.name} has no channel selected")

    code: dict[str, str] = {}
    order = [s.name for s in spec.signals if s.kind == "channel"]
    pending = {s.name: s for s in spec.signals if s.kind == "expression"}
    defined = set(channels)
    failed: set[str] = set()

    def fail(name: str, message: str) -> None:
        add("signals", name, message)
        failed.add(name)
        del pending[name]

    while pending:
        progressed = False
        for name, s in list(pending.items()):
            try:
                checked = tsal.check(s.expr, seen - {name})
            except tsal.ExprError as e:
                fail(name, f"{name}: {e}")
                progressed = True
                continue
            if checked.type != tsal.SIGNAL:
                fail(name, f"{name} must be a signal, but the expression gives {tsal.LABELS[checked.type]}")
                progressed = True
                continue
            broken = checked.names & failed
            if broken:
                fail(name, f"{name} uses {', '.join(sorted(broken))}, which has an error")
                progressed = True
                continue
            if checked.names <= defined:
                code[name] = checked.code
                order.append(name)
                defined.add(name)
                del pending[name]
                progressed = True
        if not progressed:
            for name in pending:
                add("signals", name, f"{name} depends on itself through other signals")
            break

    events: dict[str, str] = {}
    for e in spec.events:
        if e.name in events:
            add("signals", e.name, f"Event name {e.name} is already used")
            continue
        events[e.name] = ""
        try:
            checked = tsal.check(e.expr, seen)
        except tsal.ExprError as err:
            add("signals", e.name, f"Event {e.name}: {err}")
            continue
        expected = tsal.INTERVALS if e.kind == "condition" else tsal.POINTS
        if checked.type != expected:
            add("signals", e.name, f"Event {e.name} needs {tsal.LABELS[expected]}, but the expression gives {tsal.LABELS[checked.type]}")
            continue
        broken = checked.names - defined
        if broken:
            add("signals", e.name, f"Event {e.name} uses {', '.join(sorted(broken))}, which has an error")
            continue
        events[e.name] = e.kind
        code[f"ev_{e.name}"] = checked.code

    def need_signal(item: str, ref: str, role: str) -> None:
        if not ref:
            add("aggregations", item, f"{item}: choose {role}")
        elif ref not in seen:
            add("aggregations", item, f"{item}: signal {ref} does not exist")

    def need_interval_scope(item: str, ref: str) -> None:
        if ref and ref not in events:
            add("aggregations", item, f"{item}: event {ref} does not exist")
        elif ref and events[ref] == "points":
            add("aggregations", item, f"{item}: event {ref} marks points in time; use a condition event or the whole series")

    names: set[str] = set()
    for a in spec.aggregations:
        if a.name in names:
            add("aggregations", a.name, f"Aggregation name {a.name} is already used")
        names.add(a.name)
        if a.kind == "histogram":
            need_signal(a.name, a.signal, "a signal")
            if a.weight == "distance":
                need_signal(a.name, a.distance, "a distance signal")
            issue = _bins_issue(a.bins)
            if issue:
                add("aggregations", a.name, f"{a.name}: {issue}")
            need_interval_scope(a.name, a.event)
        elif a.kind == "histogram_2d":
            need_signal(a.name, a.x, "an x signal")
            need_signal(a.name, a.y, "a y signal")
            for axis, bins in (("x", a.x_bins), ("y", a.y_bins)):
                issue = _bins_issue(bins)
                if issue:
                    add("aggregations", a.name, f"{a.name}: {axis} axis {issue}")
            need_interval_scope(a.name, a.event)
        elif a.kind == "statistics":
            if not a.signals:
                add("aggregations", a.name, f"{a.name}: choose at least one signal")
            for ref in a.signals:
                need_signal(a.name, ref, "a signal")
            if not a.statistics:
                add("aggregations", a.name, f"{a.name}: choose at least one statistic")
            if len(set(a.signals)) != len(a.signals):
                add("aggregations", a.name, f"{a.name}: a signal is listed twice")
            need_interval_scope(a.name, a.event)
        else:
            if not a.signals:
                add("aggregations", a.name, f"{a.name}: choose at least one signal")
            for ref in a.signals:
                need_signal(a.name, ref, "a signal")
            if len(set(a.signals)) != len(a.signals):
                add("aggregations", a.name, f"{a.name}: a signal is listed twice")
            if not a.event:
                add("aggregations", a.name, f"{a.name}: choose a points-in-time event")
            elif a.event not in events:
                add("aggregations", a.name, f"{a.name}: event {a.event} does not exist")
            elif events[a.event] == "condition":
                add("aggregations", a.name, f"{a.name}: values are sampled at points in time; event {a.event} is a condition")
    if not spec.aggregations:
        add("aggregations", "", "Add at least one aggregation")

    return Analysis(issues=issues, signal_order=order, code=code)
