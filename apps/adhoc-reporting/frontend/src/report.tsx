import { createContext, useCallback, useContext, useEffect, useRef, useState, type ReactNode } from "react";
import { ApiError, api } from "./api";
import type { Issue, ReportSpec, StepKey } from "./types";

export type SaveState = "idle" | "saving" | "saved" | "error" | "conflict";

type ReportContextValue = {
  spec: ReportSpec;
  update: (change: (spec: ReportSpec) => ReportSpec) => void;
  save: SaveState;
  saveError?: string;
  issues?: Issue[];
  flushNow: () => Promise<boolean>;
};

const ReportContext = createContext<ReportContextValue | null>(null);

export const bridge: { report?: string; flush?: () => Promise<boolean> } = {};

export function useReport(): ReportContextValue {
  const value = useContext(ReportContext);
  if (!value) throw new Error("useReport must be used inside ReportProvider");
  return value;
}

export function useIssues(step: Exclude<StepKey, "run">): { all: Issue[]; general: string[]; of: (item: string) => string[] } {
  const { issues } = useReport();
  const all = (issues ?? []).filter((i) => i.step === step);
  return {
    all,
    general: all.filter((i) => !i.item).map((i) => i.message),
    of: (item: string) => all.filter((i) => i.item === item).map((i) => i.message),
  };
}

export function ReportProvider({ initial, children }: { initial: ReportSpec; children: ReactNode }) {
  const [spec, setSpec] = useState(initial);
  const [save, setSave] = useState<SaveState>("idle");
  const [saveError, setSaveError] = useState<string>();
  const [issues, setIssues] = useState<Issue[]>();
  const latest = useRef(initial);
  const revision = useRef(initial.revision);
  const timer = useRef<number | undefined>(undefined);
  const busy = useRef(false);
  const again = useRef(false);
  const ok = useRef(true);
  const inflight = useRef<Promise<void>>(Promise.resolve());

  const flush = useCallback((): Promise<void> => {
    window.clearTimeout(timer.current);
    timer.current = undefined;
    if (busy.current) {
      again.current = true;
      return inflight.current;
    }
    busy.current = true;
    setSave("saving");
    inflight.current = (async () => {
      try {
        const saved = await api.saveReport({ ...latest.current, revision: revision.current });
        revision.current = saved.revision;
        ok.current = true;
        setSave("saved");
      } catch (error) {
        ok.current = false;
        again.current = false;
        if (error instanceof ApiError && error.status === 409) {
          setSave("conflict");
        } else {
          setSave("error");
          setSaveError((error as Error).message);
        }
      } finally {
        busy.current = false;
        if (again.current) {
          again.current = false;
          void flush();
        }
      }
    })();
    return inflight.current;
  }, []);

  const flushNow = useCallback(async () => {
    while (timer.current !== undefined || busy.current) {
      await (timer.current !== undefined ? flush() : inflight.current);
    }
    return ok.current;
  }, [flush]);

  const update = useCallback(
    (change: (spec: ReportSpec) => ReportSpec) => {
      const next = change(latest.current);
      latest.current = next;
      setSpec(next);
      window.clearTimeout(timer.current);
      timer.current = window.setTimeout(() => void flush(), 1000);
    },
    [flush],
  );

  useEffect(() => {
    const controller = new AbortController();
    const wait = window.setTimeout(() => {
      api.check(spec, controller.signal).then(setIssues, (error: Error) => {
        if (error.name !== "AbortError") setIssues([{ step: "data", item: "", message: error.message }]);
      });
    }, 400);
    return () => {
      window.clearTimeout(wait);
      controller.abort();
    };
  }, [spec]);

  useEffect(() => {
    bridge.report = initial.name;
    bridge.flush = flushNow;
    return () => {
      if (bridge.flush === flushNow) {
        bridge.report = undefined;
        bridge.flush = undefined;
      }
      if (timer.current !== undefined) void flush();
    };
  }, [flush, flushNow, initial.name]);

  return <ReportContext.Provider value={{ spec, update, save, saveError, issues, flushNow }}>{children}</ReportContext.Provider>;
}
