import { FileBarChart2, Plus, Trash2 } from "lucide-react";
import { useState } from "react";
import { api } from "../api";
import { relativeTime, reportHref, useAsync } from "../hooks";
import { Banner, Empty, Field, SkeletonRows } from "./ui";

const NAME_RE = /^[a-z][a-z0-9_]{0,62}$/;

function NewReport({ onCancel }: { onCancel: () => void }) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const valid = NAME_RE.test(name);

  const create = async () => {
    setBusy(true);
    setError(undefined);
    try {
      const spec = await api.createReport(name, description);
      window.location.hash = reportHref(spec.name);
    } catch (e) {
      setError((e as Error).message);
      setBusy(false);
    }
  };

  return (
    <div className="card" style={{ marginBottom: 16 }}>
      <div className="card-head">
        <h2>New report</h2>
      </div>
      <div className="card-body">
        {error && <Banner>{error}</Banner>}
        <div className="grid">
          <Field label="Name" hint={name && !valid ? "Lowercase letters, digits and underscores" : "Also used as the gold table prefix"} bad={!!name && !valid}>
            <input className="input" autoFocus value={name} placeholder="engine_load_profile" onChange={(e) => setName(e.target.value.trim())} />
          </Field>
          <Field label="Description">
            <input className="input" value={description} placeholder="Optional" onChange={(e) => setDescription(e.target.value)} />
          </Field>
        </div>
        <div className="row" style={{ marginTop: 16, justifyContent: "flex-end" }}>
          <button className="btn ghost" onClick={onCancel}>
            Cancel
          </button>
          <button className="btn primary" disabled={!valid || busy} onClick={create}>
            {busy ? <span className="spinner" /> : <Plus size={15} />}
            Create report
          </button>
        </div>
      </div>
    </div>
  );
}

export function Home() {
  const reports = useAsync(() => api.reports(), []);
  const [creating, setCreating] = useState(false);

  const remove = async (name: string) => {
    if (!window.confirm(`Delete report ${name} with its job and run history? A running job is cancelled. Gold tables are not touched.`)) return;
    await api.deleteReport(name);
    reports.reload();
  };

  const list = reports.data ?? [];
  return (
    <main className="main">
      <div className="page-head">
        <div>
          <h1>Reports</h1>
          <p className="sub">Build Impulse reports on your measurement data, guided step by step or through the assistant.</p>
        </div>
        {!creating && (
          <button className="btn primary" onClick={() => setCreating(true)}>
            <Plus size={15} />
            New report
          </button>
        )}
      </div>
      {creating && <NewReport onCancel={() => setCreating(false)} />}
      {reports.error && <Banner>{reports.error.message}</Banner>}
      <div className="card">
        {!reports.loading && list.length === 0 ? (
          <Empty icon={<FileBarChart2 size={22} />} title="No reports yet">
            Create a report to select data, define signals and aggregations, and run it on your workspace.
          </Empty>
        ) : (
          <table className="table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Description</th>
                <th>Units under test</th>
                <th>Aggregations</th>
                <th>Updated</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {reports.loading && !reports.data ? (
                <SkeletonRows rows={3} columns={6} />
              ) : (
                list.map((r) => (
                  <tr key={r.name} className="clickable" onClick={() => (window.location.hash = reportHref(r.name))}>
                    <td style={{ fontWeight: 550 }}>{r.name}</td>
                    <td style={{ color: "var(--text-2)" }}>{r.description}</td>
                    <td className="num">{r.uuts}</td>
                    <td className="num">{r.aggregations}</td>
                    <td className="num">{relativeTime(r.updated_at)}</td>
                    <td style={{ textAlign: "right" }}>
                      <button
                        className="btn ghost icon danger"
                        aria-label={`Delete ${r.name}`}
                        onClick={(e) => {
                          e.stopPropagation();
                          void remove(r.name);
                        }}
                      >
                        <Trash2 size={15} />
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        )}
      </div>
    </main>
  );
}
