import { AlertTriangle, Check } from "lucide-react";
import { Fragment, useEffect } from "react";
import { api } from "../api";
import { reportHref, useAsync } from "../hooks";
import { ReportProvider, useReport, type SaveState } from "../report";
import { STEPS, type Issue, type Me, type ReportSpec, type StepKey } from "../types";
import { AggregationsStep } from "./steps/AggregationsStep";
import { DataStep } from "./steps/DataStep";
import { ReviewStep } from "./steps/ReviewStep";
import { SignalsStep } from "./steps/SignalsStep";
import { UutsStep } from "./steps/UutsStep";
import { TopBar } from "./TopBar";
import { Banner } from "./ui";

function progress(spec: ReportSpec, issues: Issue[] | undefined): Record<StepKey, "done" | "todo" | "bad"> {
  const state = (step: StepKey, filled: boolean) => {
    const open = (issues ?? []).some((i) => i.step === step);
    return filled && !open ? "done" : filled && open ? "bad" : "todo";
  };
  return {
    data: state("data", !!(spec.source.catalog && spec.source.schema)),
    uuts: state("uuts", spec.uuts.length > 0),
    signals: state("signals", spec.signals.length > 0),
    aggregations: state("aggregations", spec.aggregations.length > 0),
    run: "todo",
  };
}

function Stepper({ name, step, states }: { name: string; step: StepKey; states: Record<StepKey, "done" | "todo" | "bad"> }) {
  return (
    <nav className="stepper" aria-label="Report steps">
      {STEPS.map((s, i) => {
        const active = s.key === step;
        const state = states[s.key];
        return (
          <Fragment key={s.key}>
            {i > 0 && <span className="step-sep" />}
            <a
              href={reportHref(name, s.key)}
              className={`step${active ? " active" : ""}${state === "done" && !active ? " done" : ""}${state === "bad" ? " bad" : ""}`}
              aria-current={active ? "step" : undefined}
            >
              <span className="step-index">{state === "done" && !active ? <Check size={12} strokeWidth={3} /> : i + 1}</span>
              {s.label}
            </a>
          </Fragment>
        );
      })}
    </nav>
  );
}

function SaveStatus({ state }: { state: SaveState }) {
  if (state === "saving")
    return (
      <span className="save-state">
        <span className="spinner" />
        Saving
      </span>
    );
  if (state === "saved")
    return (
      <span className="save-state">
        <Check size={13} />
        Saved
      </span>
    );
  if (state === "error" || state === "conflict")
    return (
      <span className="save-state bad">
        <AlertTriangle size={13} />
        Not saved
      </span>
    );
  return null;
}

function Editor({ me, step, onReload }: { me?: Me; step: StepKey; onReload: () => void }) {
  const { spec, save, saveError, issues } = useReport();
  return (
    <>
      <TopBar
        me={me}
        crumb={spec.name}
        center={<Stepper name={spec.name} step={step} states={progress(spec, issues)} />}
        status={<SaveStatus state={save} />}
      />
      <main className="main">
        {save === "conflict" && (
          <Banner warn>
            This report was changed in another tab or by another user.{" "}
            <button className="btn ghost" style={{ height: 24 }} onClick={onReload}>
              Reload
            </button>
          </Banner>
        )}
        {save === "error" && <Banner>Saving failed: {saveError}</Banner>}
        {step === "data" && <DataStep />}
        {step === "uuts" && <UutsStep />}
        {step === "signals" && <SignalsStep />}
        {step === "aggregations" && <AggregationsStep />}
        {step === "run" && <ReviewStep />}
      </main>
    </>
  );
}

export function ReportPage({ me, name, step }: { me?: Me; name: string; step: StepKey }) {
  const report = useAsync(() => api.report(name), [name]);
  const { reload } = report;
  useEffect(() => {
    const onSpec = (e: Event) => {
      if ((e as CustomEvent<{ report: string }>).detail.report === name) reload();
    };
    window.addEventListener("impulse:spec", onSpec);
    return () => window.removeEventListener("impulse:spec", onSpec);
  }, [name, reload]);
  if (!report.data) {
    return (
      <>
        <TopBar me={me} crumb={name} />
        <main className="main">
          {report.error ? (
            <Banner>{report.error.message}</Banner>
          ) : (
            <div className="card">
              <div className="card-body">
                <div className="skeleton" style={{ width: "35%", marginBottom: 12 }} />
                <div className="skeleton" style={{ width: "60%" }} />
              </div>
            </div>
          )}
        </main>
      </>
    );
  }
  return (
    <ReportProvider key={`${report.data.name}:${report.data.revision}`} initial={report.data}>
      <Editor me={me} step={step} onReload={report.reload} />
    </ReportProvider>
  );
}
