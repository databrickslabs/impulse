import { CheckCircle2, CircleX, Copy, ExternalLink, Play, Square } from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { api } from "../../api";
import { reportHref, useAsync } from "../../hooks";
import { useReport } from "../../report";
import { STEPS, type Preflight, type RunSummary } from "../../types";
import { duration, ResultsView } from "../Results";
import { Banner, StepHead } from "../ui";

function CopyButton({ text, label = "Copy" }: { text: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      className="btn ghost"
      onClick={() => {
        void navigator.clipboard.writeText(text).then(() => {
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1500);
        });
      }}
    >
      {copied ? <CheckCircle2 size={14} /> : <Copy size={14} />}
      {copied ? "Copied" : label}
    </button>
  );
}

function useNow(active: boolean): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (!active) return;
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [active]);
  return now;
}

const STATE_LABEL: Record<string, string> = {
  QUEUED: "Queued",
  PENDING: "Starting",
  BLOCKED: "Waiting",
  RUNNING: "Running",
  TERMINATING: "Finishing",
  SUCCESS: "Succeeded",
  FAILED: "Failed",
  TIMEDOUT: "Timed out",
  CANCELED: "Cancelled",
  INTERNAL_ERROR: "Failed",
};

function label(run: RunSummary): string {
  return STATE_LABEL[run.active ? (run.state ?? "") : (run.result ?? run.state ?? "")] ?? run.result ?? run.state ?? "Unknown";
}

function badge(run: RunSummary): string {
  if (run.active) return "badge";
  return run.result === "SUCCESS" ? "badge ok" : "badge bad";
}

function PreflightProblems({ preflight }: { preflight: Preflight }) {
  return (
    <div className="card-body stack preflight">
      {preflight.user.length > 0 && (
        <div>
          <h3>Your permissions</h3>
          <p className="sub">The report reads and writes only data you can access yourself. Ask a data owner for these privileges.</p>
          <ul className="todo">
            {preflight.user.map((p) => (
              <li key={`${p.privilege}:${p.object}`}>
                <span className="mono priv">{p.privilege}</span>
                <span>
                  <span className="mono">{p.object}</span>
                  <span className="hint"> {p.message}</span>
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}
      {preflight.app.length > 0 && (
        <div>
          <h3>The app's service principal</h3>
          <p className="sub">
            Report jobs run as {preflight.app_principal.name}. A workspace or catalog admin grants it access once per catalog and schema:
          </p>
          <ul className="todo">
            {preflight.app.map((p) => (
              <li key={`${p.privilege}:${p.object}`}>
                <span className="mono priv">{p.privilege}</span>
                <span className="mono">{p.object}</span>
              </li>
            ))}
          </ul>
          {preflight.grants && (
            <div className="grants">
              <pre className="code">{preflight.grants}</pre>
              <CopyButton text={preflight.grants} label="Copy SQL" />
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function RunPanel({ onSuccess }: { onSuccess: () => void }) {
  const { spec, flushNow } = useReport();
  const [history, setHistory] = useState<RunSummary[]>();
  const [current, setCurrent] = useState<RunSummary>();
  const [preflight, setPreflight] = useState<Preflight>();
  const [busy, setBusy] = useState<"run" | "cancel">();
  const [error, setError] = useState<string>();
  const now = useNow(!!current?.active);

  const refresh = useCallback(async () => {
    const runs = await api.runs(spec.name);
    setHistory(runs);
    return runs;
  }, [spec.name]);

  useEffect(() => {
    refresh().then(
      (runs) => runs[0] && api.runDetail(spec.name, runs[0].run_id).then(setCurrent),
      (e: Error) => setError(e.message),
    );
  }, [refresh, spec.name]);

  useEffect(() => {
    const onRun = (e: Event) => {
      const detail = (e as CustomEvent<{ report: string; run_id: number }>).detail;
      if (detail.report !== spec.name) return;
      setPreflight(undefined);
      void api.runDetail(spec.name, detail.run_id).then(setCurrent);
      void refresh();
    };
    window.addEventListener("impulse:run", onRun);
    return () => window.removeEventListener("impulse:run", onRun);
  }, [refresh, spec.name]);

  useEffect(() => {
    if (!current?.active) return;
    const timer = window.setInterval(() => {
      api.runDetail(spec.name, current.run_id).then(
        (run) => {
          setCurrent(run);
          if (!run.active) {
            void refresh();
            if (run.result === "SUCCESS") onSuccess();
          }
        },
        () => undefined,
      );
    }, 5000);
    return () => window.clearInterval(timer);
  }, [current?.active, current?.run_id, spec.name, refresh, onSuccess]);

  const start = async () => {
    setBusy("run");
    setError(undefined);
    setPreflight(undefined);
    try {
      if (!(await flushNow())) throw new Error("The report could not be saved. Fix the save error first.");
      const response = await api.run(spec.name);
      if (!response.run) {
        setPreflight(response.preflight);
        return;
      }
      setCurrent(await api.runDetail(spec.name, response.run.run_id));
      void refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(undefined);
    }
  };

  const cancel = async () => {
    if (!current) return;
    setBusy("cancel");
    try {
      await api.cancelRun(spec.name, current.run_id);
      setCurrent(await api.runDetail(spec.name, current.run_id));
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(undefined);
    }
  };

  const elapsed = current?.start_time ? ((current.end_time ?? now) - current.start_time) / 1000 : null;

  return (
    <div className="card">
      <div className="card-head">
        <div className="row">
          <Play size={16} />
          <h2>Run</h2>
        </div>
        {current?.active ? (
          <button className="btn" onClick={() => void cancel()} disabled={!!busy}>
            {busy === "cancel" ? <span className="spinner" /> : <Square size={14} />}
            Cancel
          </button>
        ) : (
          <button className="btn primary" onClick={() => void start()} disabled={!!busy}>
            {busy === "run" ? <span className="spinner" /> : <Play size={15} />}
            {busy === "run" ? "Checking access" : "Run report"}
          </button>
        )}
      </div>
      {error && (
        <div className="card-body">
          <Banner>{error}</Banner>
        </div>
      )}
      {preflight && !preflight.ok && <PreflightProblems preflight={preflight} />}
      {current && (
        <div className="card-body stack">
          <div className="row">
            <span className={badge(current)}>
              {current.active && <span className="spinner" />}
              {label(current)}
            </span>
            {elapsed !== null && <span className="sub" style={{ margin: 0 }}>{duration(elapsed)}</span>}
            <span className="spacer" />
            {current.url && (
              <a className="btn ghost" href={current.url} target="_blank" rel="noreferrer">
                <ExternalLink size={14} />
                Job run
              </a>
            )}
          </div>
          {!current.active && current.result !== "SUCCESS" && (current.error || current.message) && (
            <div className="run-error">
              <div className="row">
                <CircleX size={15} color="var(--error)" />
                <strong>{current.error || current.message}</strong>
              </div>
              {current.trace && (
                <details>
                  <summary>Error trace</summary>
                  <pre className="code">{current.trace}</pre>
                </details>
              )}
            </div>
          )}
        </div>
      )}
      {history && history.length > 1 && (
        <details className="history">
          <summary>Earlier runs</summary>
          <table className="table compact">
            <tbody>
              {history.slice(1).map((run) => (
                <tr key={run.run_id}>
                  <td>
                    <span className={badge(run)}>{label(run)}</span>
                  </td>
                  <td className="num">{run.start_time ? new Date(run.start_time).toLocaleString() : ""}</td>
                  <td className="num">{run.start_time && run.end_time ? duration((run.end_time - run.start_time) / 1000) : ""}</td>
                  <td style={{ textAlign: "right" }}>
                    {run.url && (
                      <a className="btn ghost icon" href={run.url} target="_blank" rel="noreferrer" aria-label="Open job run">
                        <ExternalLink size={14} />
                      </a>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </details>
      )}
    </div>
  );
}

export function ReviewStep() {
  const { spec, issues } = useReport();
  const [tick, setTick] = useState(0);
  const results = useAsync(() => api.results(spec.name), [spec.name, tick]);
  const onSuccess = useCallback(() => setTick((t) => t + 1), []);
  const open = issues ?? [];

  return (
    <>
      <StepHead title="Review & run" sub="Run the report on serverless compute and inspect its results." report={spec.name} />
      {open.length > 0 ? (
        <div className="card">
          <div className="card-head">
            <h2>
              {open.length} thing{open.length > 1 ? "s" : ""} to fix before running
            </h2>
          </div>
          <div className="card-body">
            <ul className="todo">
              {open.map((issue) => (
                <li key={`${issue.step}:${issue.message}`}>
                  <a href={reportHref(spec.name, issue.step)}>{STEPS.find((s) => s.key === issue.step)?.label}</a>
                  <span>{issue.message}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      ) : (
        issues && <RunPanel onSuccess={onSuccess} />
      )}
      <div className="section-head">
        <h2>Results</h2>
      </div>
      {results.error && <Banner>{results.error.message}</Banner>}
      {results.data ? (
        <ResultsView results={results.data} />
      ) : (
        !results.error && (
          <div className="card">
            <div className="card-body">
              <div className="skeleton" style={{ width: "30%", marginBottom: 12 }} />
              <div className="skeleton" style={{ width: "70%" }} />
            </div>
          </div>
        )
      )}
    </>
  );
}
