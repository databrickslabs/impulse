import { BarChart3, Crosshair, Grid3x3, Sigma, Sparkles, Trash2 } from "lucide-react";
import { useEffect, useState, type ReactNode } from "react";
import { formatNumber, nameProblem, niceBins, range, slug } from "../../names";
import { useIssues, useReport } from "../../report";
import { STATISTICS, type Aggregation, type ReportSpec, type Signal } from "../../types";
import { Empty, Field, NameInput, Problems, StepHead, TextInput, Toggles } from "../ui";

type Kind = Aggregation["kind"];

const KINDS: { kind: Kind; label: string; icon: ReactNode; text: string }[] = [
  { kind: "histogram", label: "Histogram", icon: <BarChart3 size={15} />, text: "HistogramDuration, HistogramDistance" },
  { kind: "histogram_2d", label: "Heatmap", icon: <Grid3x3 size={15} />, text: "Histogram2DDuration" },
  { kind: "statistics", label: "Statistics", icon: <Sigma size={15} />, text: "StatsAggregator" },
  { kind: "point_values", label: "Values at points", icon: <Crosshair size={15} />, text: "PointValueAggregator" },
];

function regular(bins: number[]): { from: string; to: string; step: string } {
  if (bins.length < 2) return { from: "", to: "", step: "" };
  const step = bins[1] - bins[0];
  const even = bins.every((b, i) => i === 0 || Math.abs(b - bins[i - 1] - step) < Math.abs(step) * 1e-9);
  return even ? { from: String(bins[0]), to: String(bins[bins.length - 1]), step: String(Number(step.toPrecision(12))) } : { from: "", to: "", step: "" };
}

function BinsEditor({ label, value, onChange, signal }: { label: string; value: number[]; onChange: (bins: number[]) => void; signal?: Signal }) {
  const [form, setForm] = useState(() => regular(value));
  const [raw, setRaw] = useState<string | null>(null);
  const key = value.join(",");
  useEffect(() => setForm(regular(value)), [key]);

  const apply = (next: { from: string; to: string; step: string }) => {
    setForm(next);
    const bins = range(Number(next.from), Number(next.to), Number(next.step));
    if (next.from !== "" && next.to !== "" && next.step !== "" && bins.length >= 2) onChange(bins);
  };
  const known = signal?.kind === "channel" && signal.min !== null && signal.max !== null ? signal : undefined;
  const summary =
    value.length >= 2
      ? `${value.length - 1} bins: ${value.slice(0, 4).map(formatNumber).join(", ")}${value.length > 5 ? ", …" : ""}${value.length > 4 ? `, ${formatNumber(value[value.length - 1])}` : ""}`
      : "No bins yet";

  return (
    <div className="field bins">
      <label>{label}</label>
      {raw === null ? (
        <div className="bins-row">
          {(["from", "to", "step"] as const).map((key) => (
            <span className="affix" key={key}>
              <span>{key}</span>
              <input
                className="input"
                inputMode="decimal"
                aria-label={`${label} ${key}`}
                value={form[key]}
                onChange={(e) => apply({ ...form, [key]: e.target.value })}
              />
            </span>
          ))}
          {known && (
            <button className="btn" type="button" title={`Fit to ${formatNumber(known.min)} to ${formatNumber(known.max)}`} onClick={() => onChange(niceBins(known.min!, known.max!))}>
              <Sparkles size={14} />
              Suggest
            </button>
          )}
        </div>
      ) : (
        <textarea
          className="input mono bins-raw"
          value={raw}
          aria-label={`${label} edges`}
          onChange={(e) => setRaw(e.target.value)}
          onBlur={() => {
            const parsed = raw
              .split(/[\s,;]+/)
              .filter(Boolean)
              .map(Number);
            if (parsed.every(Number.isFinite)) onChange(parsed);
            setRaw(null);
          }}
          autoFocus
        />
      )}
      <span className="hint">
        {summary}
        {raw === null && (
          <button className="link" type="button" onClick={() => setRaw(value.join(", "))}>
            Edit edges
          </button>
        )}
      </span>
    </div>
  );
}

function SignalSelect({ value, signals, onChange, label, placeholder = "Select signal" }: { value: string; signals: Signal[]; onChange: (v: string) => void; label: string; placeholder?: string }) {
  return (
    <select className="select" value={value} onChange={(e) => onChange(e.target.value)} aria-label={label}>
      <option value="">{placeholder}</option>
      {signals.map((s) => (
        <option key={s.name} value={s.name}>
          {s.name}
        </option>
      ))}
    </select>
  );
}

function ScopeSelect({ value, spec, points, onChange }: { value: string; spec: ReportSpec; points?: boolean; onChange: (v: string) => void }) {
  const events = spec.events.filter((e) => (points ? e.kind === "points" : e.kind === "condition"));
  return (
    <select className="select" value={value} onChange={(e) => onChange(e.target.value)} aria-label="Scope">
      {points ? <option value="">Select points-in-time event</option> : <option value="">Whole series</option>}
      {events.map((e) => (
        <option key={e.name} value={e.name}>
          {e.name}
        </option>
      ))}
    </select>
  );
}

function Card({ index }: { index: number }) {
  const { spec, update } = useReport();
  const issues = useIssues("aggregations");
  const a = spec.aggregations[index];
  const meta = KINDS.find((k) => k.kind === a.kind)!;
  const signal = (name: string) => spec.signals.find((s) => s.name === name);
  const set = (change: Partial<Aggregation>) =>
    update((s) => ({ ...s, aggregations: s.aggregations.map((x, i) => (i === index ? ({ ...x, ...change } as Aggregation) : x)) }));
  const names = spec.aggregations.map((x) => x.name).filter((n) => n !== a.name);

  let body: ReactNode;
  if (a.kind === "histogram") {
    body = (
      <>
        <div className="grid">
          <Field label="Signal">
            <SignalSelect value={a.signal} signals={spec.signals} label="Signal" onChange={(v) => set({ signal: v })} />
          </Field>
          <Field label="Scope">
            <ScopeSelect value={a.event} spec={spec} onChange={(v) => set({ event: v })} />
          </Field>
          <Field label="Weight">
            <select className="select" value={a.weight} onChange={(e) => set({ weight: e.target.value as "duration" | "distance" })} aria-label="Weight">
              <option value="duration">Time spent</option>
              <option value="distance">Distance travelled</option>
            </select>
          </Field>
          {a.weight === "distance" && (
            <Field label="Cumulative distance signal">
              <SignalSelect value={a.distance} signals={spec.signals} label="Cumulative distance signal" onChange={(v) => set({ distance: v })} />
            </Field>
          )}
        </div>
        <BinsEditor label="Bins" value={a.bins} signal={signal(a.signal)} onChange={(bins) => set({ bins })} />
      </>
    );
  } else if (a.kind === "histogram_2d") {
    body = (
      <>
        <div className="grid">
          <Field label="X signal">
            <SignalSelect value={a.x} signals={spec.signals} label="X signal" onChange={(v) => set({ x: v })} />
          </Field>
          <Field label="Y signal">
            <SignalSelect value={a.y} signals={spec.signals} label="Y signal" onChange={(v) => set({ y: v })} />
          </Field>
          <Field label="Scope">
            <ScopeSelect value={a.event} spec={spec} onChange={(v) => set({ event: v })} />
          </Field>
        </div>
        <div className="grid">
          <BinsEditor label="X bins" value={a.x_bins} signal={signal(a.x)} onChange={(x_bins) => set({ x_bins })} />
          <BinsEditor label="Y bins" value={a.y_bins} signal={signal(a.y)} onChange={(y_bins) => set({ y_bins })} />
        </div>
      </>
    );
  } else if (a.kind === "statistics") {
    body = (
      <div className="grid">
        <Field label="Signals">
          <Toggles options={spec.signals.map((s) => s.name)} value={a.signals} label="Signals" onChange={(signals) => set({ signals })} />
        </Field>
        <Field label="Statistics">
          <Toggles options={STATISTICS} value={a.statistics} label="Statistics" onChange={(statistics) => set({ statistics })} />
        </Field>
        <Field label="Scope">
          <ScopeSelect value={a.event} spec={spec} onChange={(v) => set({ event: v })} />
        </Field>
      </div>
    );
  } else {
    body = (
      <div className="grid">
        <Field label="Signals">
          <Toggles options={spec.signals.map((s) => s.name)} value={a.signals} label="Signals" onChange={(signals) => set({ signals })} />
        </Field>
        <Field label="Points in time">
          <ScopeSelect value={a.event} spec={spec} points onChange={(v) => set({ event: v })} />
        </Field>
      </div>
    );
  }

  return (
    <div className="card agg">
      <div className="card-head">
        <div className="row" style={{ minWidth: 0 }}>
          <span className="agg-icon">{meta.icon}</span>
          <span className="agg-kind">{meta.label}</span>
          <NameInput
            value={a.name}
            taken={names}
            check={nameProblem}
            label="Aggregation name"
            onCommit={(to) => update((s) => ({ ...s, aggregations: s.aggregations.map((x, i) => (i === index ? { ...x, name: to } : x)) }))}
          />
        </div>
        <button
          className="btn ghost icon danger"
          aria-label={`Remove ${a.name}`}
          onClick={() => update((s) => ({ ...s, aggregations: s.aggregations.filter((_, i) => i !== index) }))}
        >
          <Trash2 size={15} />
        </button>
      </div>
      <div className="card-body stack">
        {body}
        <div className="grid">
          <Field label="Description">
            <TextInput value={a.desc} placeholder="Optional" label="Description" onChange={(desc) => set({ desc })} />
          </Field>
          <Field label="Values unit">
            <TextInput value={a.values_unit} placeholder="Optional" label="Values unit" onChange={(values_unit) => set({ values_unit })} />
          </Field>
        </div>
        <Problems messages={issues.of(a.name)} />
      </div>
    </div>
  );
}

function create(spec: ReportSpec, kind: Kind): Aggregation {
  const [first, second] = spec.signals;
  const bins = (s?: Signal) => (s?.kind === "channel" && s.min !== null && s.max !== null ? niceBins(s.min, s.max) : []);
  const taken = spec.aggregations.map((a) => a.name);
  const name = (suffix: string, base = first?.name ?? "report") => slug(`${base}_${suffix}`, taken, "agg");
  switch (kind) {
    case "histogram":
      return { kind, name: name("hist"), signal: first?.name ?? "", bins: bins(first), event: "", weight: "duration", distance: "", bins_unit: "", values_unit: "", desc: "" };
    case "histogram_2d":
      return {
        kind,
        name: name(`vs_${second?.name ?? "y"}`),
        x: first?.name ?? "",
        y: second?.name ?? "",
        x_bins: bins(first),
        y_bins: bins(second),
        event: "",
        x_unit: "",
        y_unit: "",
        values_unit: "",
        desc: "",
      };
    case "statistics":
      return { kind, name: name("stats", "signal"), signals: spec.signals.slice(0, 3).map((s) => s.name), statistics: ["min", "max", "mean"], event: "", values_unit: "", desc: "" };
    default:
      return {
        kind,
        name: name("at_points"),
        signals: first ? [first.name] : [],
        event: spec.events.find((e) => e.kind === "points")?.name ?? "",
        values_unit: "",
        desc: "",
      };
  }
}

export function AggregationsStep() {
  const { spec, update } = useReport();
  const issues = useIssues("aggregations");
  return (
    <>
      <StepHead
        title="Aggregations"
        sub="Choose what the report computes per recording."
        report={spec.name}
        next={{ step: "run", label: "Review" }}
        ready={spec.aggregations.length > 0 && issues.all.length === 0}
      />
      <div className="kinds">
        {KINDS.map((k) => (
          <button
            key={k.kind}
            className="kind"
            disabled={!spec.signals.length}
            onClick={() => update((s) => ({ ...s, aggregations: [...s.aggregations, create(s, k.kind)] }))}
          >
            <span className="agg-icon">{k.icon}</span>
            <span>
              <strong>{k.label}</strong>
              <span>{k.text}</span>
            </span>
          </button>
        ))}
      </div>
      <Problems messages={issues.general.filter((m) => !(spec.aggregations.length === 0 && m.startsWith("Add at least")))} />
      {spec.aggregations.length === 0 ? (
        <div className="card">
          <Empty icon={<BarChart3 size={22} />} title="No aggregations yet">
            {spec.signals.length ? "Add a histogram, heatmap, statistics or values at points above." : "Define signals first, then add aggregations over them."}
          </Empty>
        </div>
      ) : (
        spec.aggregations.map((_, i) => <Card key={i} index={i} />)
      )}
    </>
  );
}
