import { CheckCircle2, Save, Stethoscope } from "lucide-react";
import { useEffect, useState } from "react";
import { api } from "../api";
import { useAsync } from "../hooks";
import type { Me, ModelCheck } from "../types";
import { Detection } from "./steps/DataStep";
import { TopBar } from "./TopBar";
import { Banner, Field, Select } from "./ui";

const EXAMPLE = `silver_catalog: main
silver_schema: measurements
tables:
  channels: channel_data
uut:
  key: uut_id
solver_config:
  container_metrics:
    column_name_mapping:
      measurement_id: container_id`;

export function SettingsPage({ me }: { me?: Me }) {
  const model = useAsync(() => api.model(), []);
  const catalogs = useAsync(() => api.catalogs(), []);
  const [text, setText] = useState<string>();
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const schemas = useAsync(() => (catalog ? api.schemas(catalog) : null), [catalog]);
  const [check, setCheck] = useState<ModelCheck>();
  const [busy, setBusy] = useState<"check" | "save">();
  const [error, setError] = useState<string>();
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    if (model.data && text === undefined) setText(model.data.yaml);
  }, [model.data, text]);

  const run = async (kind: "check" | "save") => {
    setBusy(kind);
    setError(undefined);
    setSaved(false);
    if (kind === "check") setCheck(undefined);
    try {
      if (kind === "check") setCheck(await api.checkModel(text ?? "", catalog, schema));
      else {
        await api.saveModel(text ?? "");
        setSaved(true);
      }
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(undefined);
    }
  };

  return (
    <>
      <TopBar me={me} crumb="Data model" />
      <main className="main">
        <div className="page-head">
          <div>
            <h1>Data model</h1>
            <p className="sub">
              How Impulse finds your silver tables. Leave this empty for the standard Impulse layout; list only the settings that differ. Shared by all users
              {model.data ? ` in ${model.data.path}` : ""}.
            </p>
          </div>
          <div className="row">
            <button className="btn" disabled={!!busy || text === undefined} onClick={() => void run("check")}>
              {busy === "check" ? <span className="spinner" /> : <Stethoscope size={15} />}
              Test
            </button>
            <button className="btn primary" disabled={!!busy || text === undefined} onClick={() => void run("save")}>
              {busy === "save" ? <span className="spinner" /> : <Save size={15} />}
              Save
            </button>
          </div>
        </div>
        {model.error && <Banner>{model.error.message}</Banner>}
        {model.data?.error && <Banner>The saved file is invalid: {model.data.error}</Banner>}
        {error && <Banner>{error}</Banner>}
        {saved && (
          <div className="banner ok" role="status">
            <CheckCircle2 size={16} />
            Saved. New and reopened reports use this data model.
          </div>
        )}
        <div className="split wide">
          <div className="card">
            <div className="card-head">
              <h2>config.yaml</h2>
              <span className="sub" style={{ margin: 0 }}>
                {model.data ? (model.data.exists ? "Customized" : "Using defaults") : ""}
              </span>
            </div>
            <textarea
              className="editor"
              value={text ?? ""}
              placeholder="Empty: the standard Impulse layout is used."
              spellCheck={false}
              aria-label="Data model YAML"
              onChange={(e) => {
                setText(e.target.value);
                setSaved(false);
              }}
            />
          </div>
          <div>
            <div className="card">
              <div className="card-head">
                <h2>Test against a schema</h2>
              </div>
              <div className="card-body stack">
                <div className="grid">
                  <Field label="Catalog">
                    <Select value={catalog} options={catalogs.data} placeholder="silver_catalog" onChange={(v) => (setCatalog(v), setSchema(""))} />
                  </Field>
                  <Field label="Schema">
                    <Select value={schema} options={catalog ? schemas.data : []} placeholder="silver_schema" disabled={!catalog} onChange={setSchema} />
                  </Field>
                </div>
                {check?.source ? (
                  <>
                    <Detection source={check.source} />
                    {check.uuts !== null && (
                      <p className="sub">
                        {check.uuts} unit{check.uuts === 1 ? "" : "s"} under test found with your permissions.
                      </p>
                    )}
                  </>
                ) : (
                  check && <Banner warn>{check.issues.join(" ")}</Banner>
                )}
              </div>
            </div>
            <details className="card reference">
              <summary className="card-head">
                <h2>Example</h2>
              </summary>
              <pre className="code">{EXAMPLE}</pre>
            </details>
          </div>
        </div>
        <details className="card reference">
          <summary className="card-head">
            <h2>All settings and defaults</h2>
          </summary>
          <table className="table settings-table">
            <thead>
              <tr>
                <th>Setting</th>
                <th>Default</th>
                <th>Description</th>
              </tr>
            </thead>
            <tbody>
              {model.data?.reference.map((s) => (
                <tr key={s.key}>
                  <td className="mono">{s.key}</td>
                  <td className="mono">{s.default}</td>
                  <td>
                    {s.description}
                    {s.example && <pre className="code">{s.example}</pre>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </details>
      </main>
    </>
  );
}
