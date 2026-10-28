import type { Channel, ChatEvent, ChatMessage, Choices, DataModelText, Issue, Me, ModelCheck, Preflight, ReportSpec, ReportSummary, Results, RunSummary, SettingsOptions, Source, UserSettings, Uut, Window } from "./types";

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, {
    method,
    headers: body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal,
  });
  const data = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = Array.isArray(data?.detail)
      ? data.detail.map((d: { loc?: unknown[]; msg: string }) => `${(d.loc ?? []).slice(1).join(".")}: ${d.msg}`).join("; ")
      : data?.detail;
    throw new ApiError(response.status, data?.error?.code ?? "http_error", data?.error?.message ?? detail ?? response.statusText);
  }
  return data as T;
}

const query = (params: Record<string, string>) => new URLSearchParams(params).toString();
const reportPath = (name: string) => `/api/reports/${encodeURIComponent(name)}`;

export const api = {
  me: () => request<Me>("GET", "/api/meta/me"),
  warmup: () => request<{ started: boolean }>("POST", "/api/meta/warmup"),
  reports: () => request<{ reports: ReportSummary[] }>("GET", "/api/reports").then((r) => r.reports),
  createReport: (name: string, description: string) => request<ReportSpec>("POST", "/api/reports", { name, description }),
  report: (name: string) => request<ReportSpec>("GET", reportPath(name)),
  saveReport: (spec: ReportSpec) => request<ReportSpec>("PUT", reportPath(spec.name), spec),
  deleteReport: (name: string) => request<{ deleted: string }>("DELETE", reportPath(name)),
  check: (spec: ReportSpec, signal?: AbortSignal) =>
    request<{ issues: Issue[] }>("POST", `${reportPath(spec.name)}/check`, spec, signal).then((r) => r.issues),
  catalogs: () => request<{ catalogs: string[] }>("GET", "/api/data/catalogs").then((r) => r.catalogs),
  schemas: (catalog: string) => request<{ schemas: string[] }>("GET", `/api/data/schemas?${query({ catalog })}`).then((r) => r.schemas),
  source: (catalog: string, schema: string) => request<Source>("GET", `/api/data/source?${query({ catalog, schema })}`),
  uuts: (catalog: string, schema: string) =>
    request<{ uuts: Uut[]; issues: string[] }>("GET", `/api/data/uuts?${query({ catalog, schema })}`),
  channels: (catalog: string, schema: string, uuts: string[], window: Window) =>
    request<{ channels: Channel[]; issues: string[] }>("POST", "/api/data/channels", { catalog, schema, uuts, window }),
  preflight: (name: string) => request<Preflight>("POST", `${reportPath(name)}/preflight`),
  run: (name: string) => request<{ preflight: Preflight; run: { job_id: number; run_id: number } | null }>("POST", `${reportPath(name)}/runs`),
  runs: (name: string) => request<{ runs: RunSummary[] }>("GET", `${reportPath(name)}/runs`).then((r) => r.runs),
  runDetail: (name: string, runId: number) => request<RunSummary>("GET", `${reportPath(name)}/runs/${runId}`),
  cancelRun: (name: string, runId: number) => request<{ cancelled: number }>("POST", `${reportPath(name)}/runs/${runId}/cancel`),
  results: (name: string) => request<Results>("GET", `${reportPath(name)}/results`),
  chatStart: (message: string, report: string | null) => request<{ turn_id: string }>("POST", "/api/chat/turns", { message, report }),
  chatEvents: (turnId: string, after: number) =>
    request<{ events: ChatEvent[]; done: boolean }>("GET", `/api/chat/turns/${turnId}?${query({ after: String(after) })}`),
  chatHistory: (report: string | null) =>
    request<{ messages: ChatMessage[] }>("GET", `/api/chat/history${report ? `?${query({ report })}` : ""}`).then((r) => r.messages),
  chatClear: (report: string | null) => request<{ cleared: boolean }>("DELETE", `/api/chat/history${report ? `?${query({ report })}` : ""}`),
  model: () => request<DataModelText>("GET", "/api/data/model"),
  checkModel: (yaml: string, catalog: string, schema: string) => request<ModelCheck>("POST", "/api/data/model/check", { yaml, catalog, schema }),
  saveModel: (yaml: string) => request<{ saved: boolean; path: string }>("PUT", "/api/data/model", { yaml }),
  settings: () => request<UserSettings>("GET", "/api/settings"),
  saveSettings: (choices: Choices) => request<UserSettings>("PUT", "/api/settings", choices),
  settingsOptions: () => request<SettingsOptions>("GET", "/api/settings/options"),
};
