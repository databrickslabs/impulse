import keyword
from dataclasses import dataclass

from server.datamodel import DataModel, Source
from server.errors import ApiError
from server.spec import Issue, ReportSpec, analyze

CELL = "\n\n# COMMAND ----------\n\n"
MODULES = {
    "BasicEvent": "impulse_reporting.events.basic_event",
    "PointsInTimeEvent": "impulse_reporting.events.points_in_time_event",
    "HistogramDuration": "impulse_reporting.aggregations.histogram",
    "HistogramDistance": "impulse_reporting.aggregations.histogram",
    "Histogram2DDuration": "impulse_reporting.aggregations.histogram2d",
    "StatsAggregator": "impulse_reporting.aggregations.stats_aggregator",
    "PointValueAggregator": "impulse_reporting.aggregations.point_value_aggregator",
}


@dataclass(frozen=True)
class Generated:
    config: dict
    notebook: str


def issues(spec: ReportSpec, source: Source | None, model: DataModel) -> list[Issue]:
    found = list(analyze(spec).issues)
    if source is not None:
        found += [Issue(step="data", message=m) for m in source.issues]
    return found


def container_filters(spec: ReportSpec, source: Source, model: DataModel) -> dict:
    window = []
    if spec.window.start:
        window.append({"column_name": model.time_columns.start, "comparator": ">=", "value": spec.window.start, "value_type": "timestamp"})
    if spec.window.stop:
        window.append({"column_name": model.time_columns.stop, "comparator": "<=", "value": spec.window.stop, "value_type": "timestamp"})
    if source.uut_source == "column":
        return {"metric_filters": [[{"column_name": model.uut.key, "comparator": "==", "value": v}, *window] for v in spec.uuts]}
    filters: dict = {
        "tag_filters": [[{"tag_name": model.uut.key, "comparator": "==", "value": v, "cast_type": "string"}] for v in spec.uuts]
    }
    if window:
        filters["metric_filters"] = [window]
    return filters


def config(spec: ReportSpec, source: Source, model: DataModel, sinkless: bool = False) -> dict:
    tables = source.tables
    src = {
        "container_metrics_table": tables["container_metrics"],
        "channel_metrics_table": tables["channel_metrics"],
        "channels_uri": tables["channels"],
    }
    for key, table in (("container_tags_table", "container_tags"), ("channel_tags_table", "channel_tags")):
        if tables.get(table):
            src[key] = tables[table]
    result: dict = {"source": src}
    if not sinkless:
        result["unity_sink"] = {
            "catalog": spec.sink.catalog,
            "schema": spec.sink.schema_,
            "table_prefix": spec.sink.table_prefix,
            "cleanup_temp_tables": True,
        }
    engine: dict = {"solver": "DefaultSolver", "data_type": source.data_type}
    if model.solver_config:
        engine["solver_config"] = model.solver_config
    result["query_engine"] = engine
    result["container_filters"] = container_filters(spec, source, model)
    if model.measurement_dimensions is not None:
        result["measurement_dimensions"] = list(model.measurement_dimensions)
    return result


def _literal(value: object, indent: int = 0) -> str:
    pad = " " * indent
    if isinstance(value, dict):
        if not value:
            return "{}"
        if not any(isinstance(v, (dict, list)) for v in value.values()):
            flat = "{" + ", ".join(f"{k!r}: {v!r}" for k, v in value.items()) + "}"
            if indent + len(flat) <= 100:
                return flat
        items = [f"{pad}    {k!r}: {_literal(v, indent + 4)}," for k, v in value.items()]
        return "{\n" + "\n".join(items) + "\n" + pad + "}"
    if isinstance(value, list):
        parts = [_literal(v, indent + 4) for v in value]
        flat = "[" + ", ".join(parts) + "]"
        if indent + len(flat) <= 100 and "\n" not in flat:
            return flat
        lines: list[str] = []
        for part in parts:
            if lines and "\n" not in part and "\n" not in lines[-1] and len(lines[-1]) + len(part) + 2 <= 96 - indent:
                lines[-1] += f", {part}"
            else:
                lines.append(part)
        return "[\n" + "\n".join(f"{pad}    {line}," for line in lines) + "\n" + pad + "]"
    return repr(value)


def _call(name: str, args: list[tuple[str, str]], indent: int = 0) -> str:
    pad = " " * (indent + 4)
    lines = [f"{pad}{key}={value}," for key, value in args]
    return f"{name}(\n" + "\n".join(lines) + "\n" + " " * indent + ")"


def _channel_call(channel: dict[str, str]) -> str:
    if all(k.isidentifier() and not keyword.iskeyword(k) for k in channel):
        return "db.query.channel(" + ", ".join(f"{k}={v!r}" for k, v in channel.items()) + ")"
    return f"db.query.channel(**{channel!r})"


def generate(spec: ReportSpec, source: Source, model: DataModel, sinkless: bool = False) -> Generated:
    found = issues(spec, source, model)
    if found:
        raise ApiError(422, "invalid_report", "; ".join(i.message for i in found))
    analysis = analyze(spec)
    signals = {s.name: s for s in spec.signals}
    events = {e.name: e for e in spec.events}

    def label(name: str) -> str:
        s = signals[name]
        return s.channel.get("channel_name", name) if s.kind == "channel" else name

    def unit(name: str, override: str) -> list[tuple[str, str]]:
        value = override or signals[name].unit
        return [("bins_unit", repr(value))] if value else []

    def scope(event: str) -> list[tuple[str, str]]:
        return [("event", f"ev_{event}")] if event else []

    def described(args: list[tuple[str, str]], desc: str) -> list[tuple[str, str]]:
        return [*args, ("desc", repr(desc))] if desc else args

    used: set[str] = {"Page", "Report"}
    signal_lines = []
    for name in analysis.signal_order:
        s = signals[name]
        signal_lines.append(f"{name} = {_channel_call(s.channel) if s.kind == 'channel' else analysis.code[name]}")

    event_lines = []
    for e in spec.events:
        cls = "BasicEvent" if e.kind == "condition" else "PointsInTimeEvent"
        used.add(cls)
        args = described([("name", repr(e.name)), ("expr", analysis.code[f"ev_{e.name}"])], e.desc)
        event_lines += [f"ev_{e.name} = {_call(cls, args)}", f"report.add_event(ev_{e.name})"]

    aggregation_lines = ["page = Page(page_number=1)", "report.add_page(page)"]
    for a in spec.aggregations:
        if a.kind == "histogram":
            cls = "HistogramDuration" if a.weight == "duration" else "HistogramDistance"
            args = [("name", repr(a.name)), ("base_expr", a.signal)]
            if a.weight == "distance":
                args += [("weights_expr", a.distance)]
            args += [("bins", _literal(a.bins, 8)), *scope(a.event), ("channel_name", repr(label(a.signal)))]
            if a.weight == "distance":
                args += [("weights_channel_name", repr(label(a.distance)))]
            args += unit(a.signal, a.bins_unit)
        elif a.kind == "histogram_2d":
            cls = "Histogram2DDuration"
            args = [
                ("name", repr(a.name)),
                ("x_expr", a.x),
                ("y_expr", a.y),
                ("x_bins", _literal(a.x_bins, 8)),
                ("y_bins", _literal(a.y_bins, 8)),
                *scope(a.event),
                ("x_channel_name", repr(label(a.x))),
                ("y_channel_name", repr(label(a.y))),
            ]
            for key, ref, override in (("x_bins_unit", a.x, a.x_unit), ("y_bins_unit", a.y, a.y_unit)):
                value = override or signals[ref].unit
                if value:
                    args.append((key, repr(value)))
        elif a.kind == "statistics":
            cls = "StatsAggregator"
            args = [
                ("name", repr(a.name)),
                ("input_expressions", "[" + ", ".join(a.signals) + "]"),
                ("channel_names", repr([label(r) for r in a.signals])),
                ("statistics", repr(list(a.statistics))),
                *scope(a.event),
            ]
        else:
            cls = "PointValueAggregator"
            args = [
                ("name", repr(a.name)),
                ("input_expressions", "[" + ", ".join(a.signals) + "]"),
                ("channel_names", repr([label(r) for r in a.signals])),
                ("event", f"ev_{a.event}"),
            ]
        if a.values_unit:
            args.append(("values_unit", repr(a.values_unit)))
        used.add(cls)
        aggregation_lines.append(f"page.add_aggregation(\n    {_call(cls, described(args, a.desc), 4)}\n)")

    cfg = config(spec, source, model, sinkless)
    imports = ["from databricks.sdk import WorkspaceClient"]
    by_module: dict[str, list[str]] = {}
    for cls in sorted(used - {"Page", "Report"}):
        by_module.setdefault(MODULES[cls], []).append(cls)
    by_module["impulse_reporting.core.page"] = ["Page"]
    by_module["impulse_reporting.core.report"] = ["Report"]
    imports += [f"from {module} import {', '.join(names)}" for module, names in sorted(by_module.items())]
    if sinkless:
        imports.insert(0, "import json")

    cells = ["# Databricks notebook source", "\n".join(imports) + "\n\nconfig = " + _literal(cfg)]
    cells.append(
        "\n".join(
            [
                f"report = Report(name={spec.name!r}, spark=spark, workspace_client=WorkspaceClient(), config=config)",
                "db = report.get_db()",
                "",
                *signal_lines,
                "",
                *event_lines,
                "",
                *aggregation_lines,
            ]
        )
    )
    if sinkless:
        cells.append(
            "\n".join(
                [
                    "report.run(persist_results=False)",
                    "counts = {}",
                    "for group in (report.event_dfs, report.aggregation_dfs):",
                    "    for kind, frames in group.items():",
                    "        counts[kind] = sum(df.count() for df in frames.values() if df is not None)",
                    "dbutils.notebook.exit(json.dumps(counts))",
                ]
            )
        )
    else:
        cells.append("report.run()")
    return Generated(config=cfg, notebook=CELL.join(cells) + "\n")
