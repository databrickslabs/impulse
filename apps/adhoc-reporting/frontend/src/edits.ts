import type { Aggregation, ReportSpec } from "./types";

const word = (name: string) => new RegExp(`\\b${name}\\b`, "g");

export function renameSignal(spec: ReportSpec, from: string, to: string): ReportSpec {
  const swap = (ref: string) => (ref === from ? to : ref);
  const rewrite = (expr: string) => expr.replace(word(from), to);
  const aggregations = spec.aggregations.map((a): Aggregation => {
    switch (a.kind) {
      case "histogram":
        return { ...a, signal: swap(a.signal), distance: swap(a.distance) };
      case "histogram_2d":
        return { ...a, x: swap(a.x), y: swap(a.y) };
      default:
        return { ...a, signals: a.signals.map(swap) };
    }
  });
  return {
    ...spec,
    signals: spec.signals.map((s) => ({ ...s, name: swap(s.name), ...(s.kind === "expression" ? { expr: rewrite(s.expr) } : {}) })),
    events: spec.events.map((e) => ({ ...e, expr: rewrite(e.expr) })),
    aggregations,
  };
}

export function renameEvent(spec: ReportSpec, from: string, to: string): ReportSpec {
  return {
    ...spec,
    events: spec.events.map((e) => (e.name === from ? { ...e, name: to } : e)),
    aggregations: spec.aggregations.map((a) => (a.event === from ? { ...a, event: to } : a)),
  };
}

export function signalUses(spec: ReportSpec, name: string): number {
  const pattern = word(name);
  const inExpressions = [...spec.signals.flatMap((s) => (s.kind === "expression" && s.name !== name ? [s.expr] : [])), ...spec.events.map((e) => e.expr)].filter(
    (expr) => expr.match(pattern),
  ).length;
  const inAggregations = spec.aggregations.filter((a) =>
    a.kind === "histogram" ? a.signal === name || a.distance === name : a.kind === "histogram_2d" ? a.x === name || a.y === name : a.signals.includes(name),
  ).length;
  return inExpressions + inAggregations;
}

export function channelLabel(channel: Record<string, string>): string {
  return Object.values(channel).filter(Boolean).join(" · ");
}
