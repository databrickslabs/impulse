from concurrent.futures import ThreadPoolExecutor

from server import sql
from server.datamodel import DataModel
from server.errors import ApiError
from server.spec import ReportSpec
from server.user import User

SECONDS = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0}
MAX_FACTS = 3000
MAX_ROWS = 500
MAX_INSTANCES = 5000


def _number(value: str | None) -> float | None:
    return float(value) if value is not None else None


def _container(value: str) -> int | str:
    return int(value) if value.isdigit() else value


def stats_table(aggregation, facts: list[dict], instances: list[dict]) -> dict:
    points = aggregation.kind == "point_values"
    ordinals = {(r["event_id"], r["event_instance_id"]): int(r["ordinal"]) for r in instances}
    event_names = {r["event_id"]: r["event_name"] for r in instances}
    event_id = next((r["event_id"] for r in facts if r["event_id"] is not None), None)
    columns: list[str] = []
    grouped: dict[tuple, dict[str, float | None]] = {}
    for r in facts:
        column = r["channel"] if points else r["label"]
        if column not in columns:
            columns.append(column)
        instance = ordinals.get((r["event_id"], r["event_instance_id"])) if r["event_id"] is not None else None
        key = (_container(r["container_id"]), instance or 0, r["event_instance_id"], "" if points else r["channel"])
        grouped.setdefault(key, {})[column] = _number(r["value"])
    if not points:
        order = list(aggregation.statistics)
        columns.sort(key=lambda c: order.index(c) if c in order else len(order))
    rows = [
        {"recording": recording, "instance": instance or None, "signal": signal or None, "values": values}
        for (recording, instance, _, signal), values in sorted(grouped.items())
    ]
    return {
        "name": aggregation.name,
        "kind": aggregation.kind,
        "unit": aggregation.values_unit,
        "scoped": event_id is not None,
        "event": event_names.get(event_id),
        "columns": columns,
        "rows": rows[:MAX_ROWS],
        "more": (int(facts[0]["total"]) > len(facts) if facts else False) or len(rows) > MAX_ROWS,
    }


def _names(names: list[str], params: dict[str, str]) -> str:
    for i, n in enumerate(names):
        params[f"n{i}"] = n
    return ", ".join(f":n{i}" for i in range(len(names)))


def _query(user: User, statement: str, params: dict[str, str]) -> list[dict] | None:
    try:
        return sql.query(user, statement, params)
    except ApiError as e:
        if e.status == 404:
            return None
        raise


def results(user: User, spec: ReportSpec, model: DataModel) -> dict:
    prefix = (spec.sink.catalog, spec.sink.schema_)

    def table(suffix: str) -> str:
        return sql.ident(*prefix, f"{spec.sink.table_prefix}_{suffix}")

    by_kind: dict[str, list[str]] = {}
    for a in spec.aggregations:
        by_kind.setdefault("histogram_2d" if a.kind == "histogram_2d" else "histogram" if a.kind == "histogram" else "stats", []).append(a.name)

    jobs: dict[str, tuple[str, dict[str, str]]] = {"recordings": (f"SELECT COUNT(*) AS n FROM {table('measurement_dimension')}", {})}
    params: dict[str, str] = {}
    jobs["events"] = (
        f"SELECT d.event_name AS name, COUNT(*) AS instances, COUNT(DISTINCT f.container_id) AS recordings, "
        f"SUM(f.end_ts - f.start_ts) AS duration FROM {table('event_instance_fact')} f JOIN {table('event_dimension')} d ON f.event_id = d.event_id "
        f"GROUP BY 1",
        params,
    )
    if by_kind.get("histogram"):
        p: dict[str, str] = {}
        jobs["histogram"] = (
            f"SELECT d.name, f.bin_id, MIN(f.lower_bound) AS lo, MIN(f.upper_bound) AS hi, SUM(f.hist_value) AS value, "
            f"COUNT(DISTINCT f.container_id) AS recordings FROM {table('histogram_fact')} f JOIN {table('histogram_dimension')} d "
            f"ON f.visual_id = d.visual_id WHERE d.name IN ({_names(by_kind['histogram'], p)}) GROUP BY 1, 2 ORDER BY 1, 2",
            p,
        )
    if by_kind.get("histogram_2d"):
        p = {}
        jobs["histogram_2d"] = (
            f"SELECT d.name, f.x_bin_id, f.y_bin_id, MIN(f.x_lower_bound) AS x_lo, MIN(f.x_upper_bound) AS x_hi, MIN(f.y_lower_bound) AS y_lo, "
            f"MIN(f.y_upper_bound) AS y_hi, SUM(f.hist_value) AS value FROM {table('histogram2d_fact')} f JOIN {table('histogram2d_dimension')} d "
            f"ON f.visual_id = d.visual_id WHERE d.name IN ({_names(by_kind['histogram_2d'], p)}) GROUP BY 1, 2, 3 ORDER BY 1, 2, 3",
            p,
        )
    if by_kind.get("stats"):
        p = {}
        jobs["stats"] = (
            f"SELECT name, container_id, event_id, event_instance_id, channel, label, value, total FROM ("
            f"SELECT d.name, f.container_id, f.event_id, f.event_instance_id, f.channel_name AS channel, f.aggregation_label AS label, "
            f"f.statistic_value AS value, COUNT(*) OVER (PARTITION BY d.name) AS total, ROW_NUMBER() OVER (PARTITION BY d.name "
            f"ORDER BY f.container_id, f.event_instance_id, f.channel_name, f.aggregation_label) AS n FROM {table('stats_aggregator_fact')} f "
            f"JOIN {table('stats_aggregator_dimension')} d ON f.visual_id = d.visual_id WHERE d.name IN ({_names(by_kind['stats'], p)})) AS s "
            f"WHERE n <= {MAX_FACTS}",
            p,
        )
        scoped = sorted({a.event for a in spec.aggregations if a.name in by_kind["stats"] and a.event})
        if scoped:
            p = {}
            jobs["instances"] = (
                f"SELECT event_id, event_instance_id, event_name, ordinal FROM (SELECT i.event_id, i.event_instance_id, i.container_id, "
                f"e.event_name, ROW_NUMBER() OVER (PARTITION BY i.event_id, i.container_id ORDER BY i.start_ts, i.event_instance_id) AS ordinal "
                f"FROM {table('event_instance_fact')} i JOIN {table('event_dimension')} e ON i.event_id = e.event_id "
                f"WHERE e.event_name IN ({_names(scoped, p)})) AS i ORDER BY container_id, ordinal LIMIT {MAX_INSTANCES}",
                p,
            )

    with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        futures = {k: pool.submit(_query, user, s, p) for k, (s, p) in jobs.items()}
        rows = {k: f.result() for k, f in futures.items()}

    if rows["recordings"] is None:
        return {"available": False, "recordings": 0, "events": [], "aggregations": []}

    seconds = SECONDS[model.time_unit]
    defined = {e.name for e in spec.events}
    events = [
        {
            "name": r["name"],
            "instances": int(r["instances"]),
            "recordings": int(r["recordings"]),
            "seconds": _number(r["duration"]) * seconds if r["duration"] is not None else None,
        }
        for r in rows["events"] or []
        if r["name"] in defined
    ]

    signals = {s.name: s for s in spec.signals}
    out = []
    for a in spec.aggregations:
        if a.kind == "histogram":
            bins = [r for r in rows.get("histogram") or [] if r["name"] == a.name]
            duration = a.weight == "duration"
            unit = "s" if duration else (a.values_unit or signals[a.distance].unit if a.distance in signals else a.values_unit)
            out.append(
                {
                    "name": a.name,
                    "kind": a.kind,
                    "unit": unit,
                    "axis": a.bins_unit or (signals[a.signal].unit if a.signal in signals else ""),
                    "bins": [
                        {
                            "lo": _number(r["lo"]),
                            "hi": _number(r["hi"]),
                            "value": _number(r["value"]) * (seconds if duration else 1.0),
                            "recordings": int(r["recordings"]),
                        }
                        for r in bins
                    ],
                }
            )
        elif a.kind == "histogram_2d":
            cells = [r for r in rows.get("histogram_2d") or [] if r["name"] == a.name]
            out.append(
                {
                    "name": a.name,
                    "kind": a.kind,
                    "unit": "s",
                    "x_axis": a.x_unit or (signals[a.x].unit if a.x in signals else ""),
                    "y_axis": a.y_unit or (signals[a.y].unit if a.y in signals else ""),
                    "cells": [
                        {
                            "x": int(r["x_bin_id"]),
                            "y": int(r["y_bin_id"]),
                            "x_lo": _number(r["x_lo"]),
                            "x_hi": _number(r["x_hi"]),
                            "y_lo": _number(r["y_lo"]),
                            "y_hi": _number(r["y_hi"]),
                            "value": _number(r["value"]) * seconds,
                        }
                        for r in cells
                    ],
                }
            )
        else:
            out.append(stats_table(a, [r for r in rows.get("stats") or [] if r["name"] == a.name], rows.get("instances") or []))
    return {"available": True, "recordings": int(rows["recordings"][0]["n"]), "events": events, "aggregations": out}
