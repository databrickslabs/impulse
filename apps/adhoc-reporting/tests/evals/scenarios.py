import os
from dataclasses import dataclass
from typing import Callable

from server.spec import ReportSpec

DATA = f"{os.environ['IMPULSE_DEMO_CATALOG']}.{os.environ['IMPULSE_DEMO_SCHEMA']}"


@dataclass(frozen=True)
class Scenario:
    name: str
    turns: list[str]
    check: Callable[[ReportSpec, str, list[dict]], list[str]]
    prepared: bool = False


def channel_signal(spec: ReportSpec, channel: str) -> str | None:
    return next((s.name for s in spec.signals if s.kind == "channel" and channel in s.channel.values()), None)


def tools_used(events: list[dict]) -> list[str]:
    return [e["name"] for e in events if e.get("type") == "tool" and e.get("status") != "running"]


def ready(spec: ReportSpec, problems: list[str]) -> list[str]:
    from server.spec import analyze

    issues = [i.message for i in analyze(spec).issues]
    return problems + ([f"not ready: {issues}"] if issues else [])


def rpm_band(spec, text, events):
    problems = []
    rpm = channel_signal(spec, "Engine RPM")
    if not rpm:
        problems.append("no Engine RPM signal")
    band = next((e for e in spec.events if e.kind == "condition" and rpm and rpm in e.expr), None)
    if not band:
        problems.append("no condition event on rpm")
    hist = next((a for a in spec.aggregations if a.kind == "histogram"), None)
    if not hist or (band and hist.event != band.name):
        problems.append("histogram missing or not scoped to the rpm event")
    elif len(hist.bins) > 2 and abs((hist.bins[1] - hist.bins[0]) - 250) > 1e-6:
        problems.append(f"bin step is {hist.bins[1] - hist.bins[0]}, expected 250")
    return ready(spec, problems)


def german(spec, text, events):
    problems = []
    rpm = channel_signal(spec, "Engine RPM")
    hist = next((a for a in spec.aggregations if a.kind == "histogram" and a.signal == rpm), None)
    if not rpm or not hist:
        problems.append("no histogram of Engine RPM")
    elif hist.event:
        problems.append("histogram should cover the whole recording")
    return ready(spec, problems)


def distance(spec, text, events):
    problems = []
    derived = [s for s in spec.signals if s.kind == "expression" and "cumtrapz" in s.expr]
    hist = next((a for a in spec.aggregations if a.kind == "histogram" and a.weight == "distance"), None)
    if not derived:
        problems.append("no cumulative distance signal")
    if not hist:
        problems.append("no distance-weighted histogram")
    elif hist.signal != channel_signal(spec, "Vehicle Speed Sensor"):
        problems.append("distance histogram is not over vehicle speed")
    return ready(spec, problems)


def heatmap(spec, text, events):
    problems = []
    rpm, speed = channel_signal(spec, "Engine RPM"), channel_signal(spec, "Vehicle Speed Sensor")
    if not any(a.kind == "histogram_2d" and {a.x, a.y} == {rpm, speed} for a in spec.aggregations):
        problems.append("no heatmap of rpm against speed")
    return ready(spec, problems)


def temperatures(spec, text, events):
    problems = []
    amb, intake = channel_signal(spec, "Ambient Air Temperature"), channel_signal(spec, "Intake Air Temperature")
    average = next((s.name for s in spec.signals if s.kind == "expression" and amb and intake and amb in s.expr and intake in s.expr), None)
    stats = next((a for a in spec.aggregations if a.kind == "statistics"), None)
    if not (amb and intake and average):
        problems.append("missing temperature signals or their average")
    if not stats or not {amb, intake, average} <= set(stats.signals) or not {"min", "max", "mean"} <= set(stats.statistics):
        problems.append("statistics do not cover min, max, mean of the three signals")
    return ready(spec, problems)


def milestones(spec, text, events):
    problems = []
    points = next((e for e in spec.events if e.kind == "points" and "falling_edges" in e.expr and "% 10" in e.expr.replace("%10", "% 10")), None)
    values = next((a for a in spec.aggregations if a.kind == "point_values"), None)
    if not points:
        problems.append("no 10 km points event")
    if not values or (points and values.event != points.name):
        problems.append("no values at the milestone event")
    return ready(spec, problems)


def unsupported(spec, text, events):
    problems = []
    lowered = text.lower()
    if not any(word in lowered for word in ("not supported", "isn't supported", "is not available", "can't", "cannot", "unsupported")):
        problems.append("did not decline the calculated channel")
    if "run_report" in tools_used(events):
        problems.append("started a run")
    return problems


def missing_channel(spec, text, events):
    problems = []
    if any("coolant" in (str(s.channel) if s.kind == "channel" else s.expr).lower() for s in spec.signals):
        problems.append("added a coolant signal that does not exist")
    if "search_channels" not in tools_used(events):
        problems.append("did not search channels")
    if "coolant" not in text.lower():
        problems.append("reply does not address coolant")
    return problems


def window(spec, text, events):
    problems = []
    if not (spec.window.start or "").startswith("2026-01-01") or not (spec.window.stop or "").startswith(("2026-01-31", "2026-02-01")):
        problems.append(f"window is {spec.window.start} to {spec.window.stop}")
    if not spec.uuts:
        problems.append("no units under test selected")
    return problems


def no_run(spec, text, events):
    problems = []
    if "run_report" in tools_used(events):
        problems.append("ran without being asked")
    return ready(spec, problems)


SCENARIOS = [
    Scenario(
        "rpm_band",
        [f"Use {DATA} with all vehicles. Make a histogram of engine speed while the engine runs between 2000 and 5000 rpm, with 250 rpm bins."],
        rpm_band,
    ),
    Scenario("german", [f"Erstelle eine Drehzahlverteilung der Motordrehzahl über die ganze Aufzeichnung, alle Fahrzeuge, Daten in {DATA}."], german),
    Scenario("distance", [f"Data is {DATA}, all vehicles. I want the vehicle speed distribution weighted by distance driven."], distance),
    Scenario("heatmap", [f"On {DATA} for every vehicle: an operating point map of engine rpm against vehicle speed."], heatmap),
    Scenario(
        "temperatures",
        [f"Use {DATA}, all vehicles. Per recording, give min, max and mean of ambient and intake air temperature and of their average."],
        temperatures,
    ),
    Scenario("milestones", [f"Use {DATA}, all vehicles. Record vehicle speed and engine rpm at every 10 km driven."], milestones),
    Scenario("unsupported", [f"Use {DATA}. Add a calculated channel that writes the average of ambient and intake air temperature back as a new channel."], unsupported),
    Scenario("missing_channel", [f"Use {DATA}, all vehicles. Add a histogram of the coolant temperature."], missing_channel),
    Scenario("window", [f"Use {DATA}. Select all vehicles but only recordings from January 2026."], window),
    Scenario(
        "no_run",
        [f"Use {DATA}, all vehicles, add engine rpm with a histogram over the whole recording. Don't run anything yet."],
        no_run,
    ),
]
