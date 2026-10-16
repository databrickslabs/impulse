import { useCallback, useEffect, useState, type DependencyList } from "react";
import { STEPS, type StepKey } from "./types";

export type AsyncState<T> = { data?: T; error?: Error; loading: boolean; reload: () => void };

export function useAsync<T>(load: () => Promise<T> | null, deps: DependencyList): AsyncState<T> {
  const [state, setState] = useState<{ data?: T; error?: Error; loading: boolean }>({ loading: true });
  const [tick, setTick] = useState(0);
  useEffect(() => {
    const promise = load();
    if (!promise) {
      setState({ loading: false });
      return;
    }
    let live = true;
    setState((s) => ({ data: s.data, loading: true }));
    promise.then(
      (data) => live && setState({ data, loading: false }),
      (error: Error) => live && setState({ error, loading: false }),
    );
    return () => {
      live = false;
    };
  }, [...deps, tick]);
  const reload = useCallback(() => setTick((t) => t + 1), []);
  return { ...state, reload };
}

export type Route = { page: "home" } | { page: "data-model" } | { page: "report"; name: string; step: StepKey };

function parse(hash: string): Route {
  const [, kind, name, step] = hash.replace(/^#/, "").split("/");
  if (kind === "r" && name) {
    const known = STEPS.some((s) => s.key === step);
    return { page: "report", name: decodeURIComponent(name), step: known ? (step as StepKey) : "data" };
  }
  if (kind === "data-model") return { page: "data-model" };
  return { page: "home" };
}

export function useRoute(): Route {
  const [route, setRoute] = useState(() => parse(window.location.hash));
  useEffect(() => {
    const onChange = () => setRoute(parse(window.location.hash));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return route;
}

export const reportHref = (name: string, step: StepKey = "data") => `#/r/${encodeURIComponent(name)}/${step}`;

export function relativeTime(iso: string): string {
  if (!iso) return "";
  const seconds = (new Date(iso).getTime() - Date.now()) / 1000;
  const format = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  const units: [Intl.RelativeTimeFormatUnit, number][] = [
    ["day", 86400],
    ["hour", 3600],
    ["minute", 60],
  ];
  for (const [unit, size] of units) {
    if (Math.abs(seconds) >= size) return format.format(Math.round(seconds / size), unit);
  }
  return "just now";
}
