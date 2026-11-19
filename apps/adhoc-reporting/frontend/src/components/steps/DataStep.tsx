import { CheckCircle2, CircleDashed, XCircle } from "lucide-react";
import { api } from "../../api";
import { useAsync } from "../../hooks";
import { useIssues, useReport } from "../../report";
import type { Location, Source } from "../../types";
import { Banner, Field, Select, StepHead } from "../ui";

const TABLES: { key: string; label: string; required: boolean }[] = [
  { key: "container_metrics", label: "Container metrics", required: true },
  { key: "channel_metrics", label: "Channel metrics", required: true },
  { key: "channels", label: "Channels", required: true },
  { key: "container_tags", label: "Container tags", required: false },
  { key: "channel_tags", label: "Channel tags", required: false },
];

const PREFIX_RE = /^[a-z][a-z0-9_]{0,62}$/;

const short = (table: string | null | undefined) => table?.split(".").pop() ?? "";

export function Detection({ source }: { source: Source }) {
  const uut =
    source.uut_source === "column"
      ? `${short(source.tables.container_metrics)}.${source.uut_key}`
      : source.uut_source === "tag"
        ? `${short(source.tables.container_tags) || "container_tags"} key ${source.uut_key}`
        : null;
  const detected = [
    { label: "Data type", value: source.data_type },
    { label: "Unit under test id", value: uut },
  ];
  return (
    <>
      <div className="row" style={{ marginBottom: 12 }}>
        <span className={source.issues.length ? "badge bad" : "badge ok"}>{source.issues.length ? "Needs attention" : "Impulse-ready"}</span>
      </div>
      {source.issues.length > 0 && (
        <Banner>
          {source.issues.map((issue) => (
            <div key={issue}>{issue}</div>
          ))}
        </Banner>
      )}
      <div className="checklist">
        {TABLES.map((t) => {
          const path = source.tables[t.key];
          const icon = path ? (
            <CheckCircle2 size={16} color="var(--success)" />
          ) : t.required ? (
            <XCircle size={16} color="var(--error)" />
          ) : (
            <CircleDashed size={16} color="var(--muted)" />
          );
          return (
            <div key={t.key} className="check-item">
              {icon}
              <span className="name">{t.label}</span>
              <span className="path mono" title={path ?? undefined}>
                {path ? short(path) : t.required ? "missing" : "not used"}
              </span>
            </div>
          );
        })}
        {detected.map((d) => (
          <div key={d.label} className="check-item">
            {d.value ? <CheckCircle2 size={16} color="var(--success)" /> : <XCircle size={16} color="var(--error)" />}
            <span className="name">{d.label}</span>
            <span className="path mono" title={d.value ?? undefined}>
              {d.value ?? "not detected"}
            </span>
          </div>
        ))}
      </div>
    </>
  );
}

export function DataStep() {
  const { spec, update } = useReport();
  const { catalog, schema } = spec.source;
  const catalogs = useAsync(() => api.catalogs(), []);
  const schemas = useAsync(() => (catalog ? api.schemas(catalog) : null), [catalog]);
  const sinkSchemas = useAsync(() => (spec.sink.catalog ? api.schemas(spec.sink.catalog) : null), [spec.sink.catalog]);
  const source = useAsync(() => (catalog && schema ? api.source(catalog, schema) : null), [catalog, schema]);

  const setSource = (change: Partial<Location>) =>
    update((s) => {
      const next = { ...s.source, ...change };
      if (change.catalog !== undefined && change.catalog !== s.source.catalog) next.schema = "";
      const sinkFollows = (!s.sink.catalog && !s.sink.schema) || (s.sink.catalog === s.source.catalog && s.sink.schema === s.source.schema);
      return {
        ...s,
        source: next,
        sink: sinkFollows ? { ...s.sink, catalog: next.catalog, schema: next.schema } : s.sink,
        uuts: [],
        window: { start: null, stop: null },
      };
    });

  const setSink = (change: Partial<typeof spec.sink>) =>
    update((s) => {
      const next = { ...s.sink, ...change };
      if (change.catalog !== undefined && change.catalog !== s.sink.catalog) next.schema = "";
      return { ...s, sink: next };
    });

  const prefixValid = PREFIX_RE.test(spec.sink.table_prefix);
  const issues = useIssues("data");
  const ready = !!source.data && source.data.issues.length === 0 && issues.all.length === 0;

  return (
    <>
      <StepHead
        title="Data"
        sub="Pick the silver schema Impulse reads and where the report writes its gold tables."
        report={spec.name}
        next={{ step: "uuts", label: "Units under test" }}
        ready={ready}
      />
      {catalogs.error && <Banner>{catalogs.error.message}</Banner>}
      <div className="card">
        <div className="card-head">
          <h2>Source</h2>
          <span className="sub" style={{ margin: 0 }}>
            Read with your permissions
          </span>
        </div>
        <div className="card-body">
          <div className="grid" style={{ marginBottom: 18 }}>
            <Field label="Catalog">
              <Select value={catalog} options={catalogs.data} placeholder="Select catalog" onChange={(v) => setSource({ catalog: v })} />
            </Field>
            <Field label="Schema">
              <Select value={schema} options={catalog ? schemas.data : []} placeholder="Select schema" disabled={!catalog} onChange={(v) => setSource({ schema: v })} />
            </Field>
          </div>
          {source.loading && catalog && schema && (
            <div className="row" style={{ color: "var(--text-2)" }}>
              <span className="spinner" />
              Checking tables
            </div>
          )}
          {source.error && <Banner>{source.error.message}</Banner>}
          {source.data && <Detection source={source.data} />}
        </div>
      </div>
      <div className="card">
        <div className="card-head">
          <h2>Output</h2>
          <span className="sub" style={{ margin: 0 }}>
            Gold tables are named {spec.sink.table_prefix || "<prefix>"}_*
          </span>
        </div>
        <div className="card-body">
          <div className="grid">
            <Field label="Catalog">
              <Select value={spec.sink.catalog} options={catalogs.data} placeholder="Select catalog" onChange={(v) => setSink({ catalog: v })} />
            </Field>
            <Field label="Schema">
              <Select
                value={spec.sink.schema}
                options={spec.sink.catalog ? sinkSchemas.data : []}
                placeholder="Select schema"
                disabled={!spec.sink.catalog}
                onChange={(v) => setSink({ schema: v })}
              />
            </Field>
            <Field label="Table prefix" hint={prefixValid ? undefined : "Lowercase letters, digits and underscores"} bad={!prefixValid}>
              <input className="input mono" value={spec.sink.table_prefix} onChange={(e) => setSink({ table_prefix: e.target.value.trim() })} />
            </Field>
          </div>
        </div>
      </div>
    </>
  );
}
