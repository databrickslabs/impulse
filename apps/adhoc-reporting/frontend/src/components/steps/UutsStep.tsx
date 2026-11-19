import { Box } from "lucide-react";
import { useEffect } from "react";
import { api } from "../../api";
import { reportHref, useAsync } from "../../hooks";
import { useIssues, useReport } from "../../report";
import type { Window } from "../../types";
import { Banner, Empty, Field, Problems, SkeletonRows, StepHead, TextInput, useSlow } from "../ui";

export function UutsStep() {
  const { spec, update } = useReport();
  const issues = useIssues("uuts");
  const { catalog, schema } = spec.source;
  const ready = !!(catalog && schema);
  const uuts = useAsync(() => (ready ? api.uuts(catalog, schema) : null), [catalog, schema]);
  const slow = useSlow(uuts.loading);

  const selected = new Set(spec.uuts);
  const all = uuts.data?.uuts ?? [];
  const allSelected = all.length > 0 && all.every((v) => selected.has(v.id));
  const first = all.map((v) => v.first).filter(Boolean).sort()[0] ?? null;
  const last = all.map((v) => v.last).filter(Boolean).sort().reverse()[0] ?? null;
  const empty = !spec.window.start && !spec.window.stop;
  const full = spec.window.start === first && spec.window.stop === last;

  useEffect(() => {
    if (empty && !uuts.loading && first && last) update((s) => ({ ...s, window: { start: first, stop: last } }));
  }, [empty, uuts.loading, first, last, update]);

  const toggle = (id: string) => update((s) => ({ ...s, uuts: s.uuts.includes(id) ? s.uuts.filter((v) => v !== id) : [...s.uuts, id] }));
  const toggleAll = () => update((s) => ({ ...s, uuts: allSelected ? [] : all.map((v) => v.id) }));
  const setWindow = (key: keyof Window, value: string) => update((s) => ({ ...s, window: { ...s.window, [key]: value || null } }));

  return (
    <>
      <StepHead
        title="Units under test"
        sub="Choose the units under test and the time window of their recordings."
        report={spec.name}
        next={{ step: "signals", label: "Signals" }}
        ready={spec.uuts.length > 0 && issues.all.length === 0}
      />
      {!ready && (
        <Banner warn>
          Choose a source catalog and schema in <a href={reportHref(spec.name, "data")}>Data</a> first.
        </Banner>
      )}
      {uuts.error && <Banner>{uuts.error.message}</Banner>}
      {uuts.data && uuts.data.issues.length > 0 && (
        <Banner>
          {uuts.data.issues.map((issue) => (
            <div key={issue}>{issue}</div>
          ))}
        </Banner>
      )}
      {ready && (
        <>
          <div className="card">
            <div className="card-head">
              <h2>
                {spec.uuts.length} of {all.length} selected
              </h2>
              {slow && (
                <span className="row sub" style={{ margin: 0 }}>
                  <span className="spinner" />
                  Starting SQL warehouse
                </span>
              )}
            </div>
            {!uuts.loading && all.length === 0 && !uuts.error ? (
              <Empty icon={<Box size={22} />} title="No units under test found">
                {!uuts.data?.issues.length &&
                  `The container tables in ${catalog}.${schema} contain no unit under test ids. Set uut.key on the Data model page to the column or tag that holds them.`}
              </Empty>
            ) : (
              <div className="table-scroll">
                <table className="table">
                  <thead>
                    <tr>
                      <th style={{ width: 36 }}>
                        <input className="checkbox" type="checkbox" checked={allSelected} onChange={toggleAll} aria-label="Select all units under test" />
                      </th>
                      <th>Unit under test</th>
                      <th>Recordings</th>
                      <th>First</th>
                      <th>Last</th>
                    </tr>
                  </thead>
                  <tbody>
                    {uuts.loading && !uuts.data ? (
                      <SkeletonRows rows={4} columns={5} />
                    ) : (
                      all.map((v) => (
                        <tr key={v.id} className="clickable" onClick={() => toggle(v.id)}>
                          <td>
                            <input
                              className="checkbox"
                              type="checkbox"
                              checked={selected.has(v.id)}
                              onChange={() => toggle(v.id)}
                              onClick={(e) => e.stopPropagation()}
                              aria-label={`Select ${v.id}`}
                            />
                          </td>
                          <td style={{ fontWeight: 550 }}>{v.id}</td>
                          <td className="num">{v.containers}</td>
                          <td className="num">{v.first}</td>
                          <td className="num">{v.last}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>
          <div className="card">
            <div className="card-head">
              <div className="row">
                <h2>Time window</h2>
                <span className="sub" style={{ margin: 0 }}>
                  Includes recordings that start and stop inside the window
                </span>
              </div>
              {first && last && !full && (
                <button className="btn ghost" onClick={() => update((s) => ({ ...s, window: { start: first, stop: last } }))}>
                  Full range
                </button>
              )}
            </div>
            <div className="card-body">
              <div className="grid" style={{ maxWidth: 560 }}>
                <Field label="From" hint={first ? `Earliest recording ${first}` : undefined}>
                  <TextInput mono value={spec.window.start ?? ""} label="Window start" onChange={(v) => setWindow("start", v)} />
                </Field>
                <Field label="To" hint={last ? `Latest recording ${last}` : undefined}>
                  <TextInput mono value={spec.window.stop ?? ""} label="Window stop" onChange={(v) => setWindow("stop", v)} />
                </Field>
              </div>
              <Problems messages={issues.general.filter((m) => m !== "Select at least one unit under test")} />
            </div>
          </div>
        </>
      )}
    </>
  );
}
