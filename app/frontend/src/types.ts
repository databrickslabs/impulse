export type Location = { catalog: string; schema: string };

export type Sink = Location & { table_prefix: string };

export type Window = { start: string | null; stop: string | null };

export type ChannelSignal = {
  kind: "channel";
  name: string;
  channel: Record<string, string>;
  unit: string;
  desc: string;
  min: number | null;
  max: number | null;
};

export type ExpressionSignal = { kind: "expression"; name: string; expr: string; unit: string; desc: string };

export type Signal = ChannelSignal | ExpressionSignal;

export type ReportEvent = { kind: "condition" | "points"; name: string; expr: string; desc: string };

export type Histogram = {
  kind: "histogram";
  name: string;
  signal: string;
  bins: number[];
  event: string;
  weight: "duration" | "distance";
  distance: string;
  bins_unit: string;
  values_unit: string;
  desc: string;
};

export type Histogram2D = {
  kind: "histogram_2d";
  name: string;
  x: string;
  y: string;
  x_bins: number[];
  y_bins: number[];
  event: string;
  x_unit: string;
  y_unit: string;
  values_unit: string;
  desc: string;
};

export const STATISTICS = ["min", "max", "mean", "median", "start", "end"] as const;

export type Statistic = (typeof STATISTICS)[number];

export type Statistics = {
  kind: "statistics";
  name: string;
  signals: string[];
  statistics: Statistic[];
  event: string;
  values_unit: string;
  desc: string;
};

export type PointValues = { kind: "point_values"; name: string; signals: string[]; event: string; values_unit: string; desc: string };

export type Aggregation = Histogram | Histogram2D | Statistics | PointValues;

export type ReportSpec = {
  spec_version: number;
  name: string;
  description: string;
  owner: string;
  source: Location;
  sink: Sink;
  uuts: string[];
  window: Window;
  signals: Signal[];
  events: ReportEvent[];
  aggregations: Aggregation[];
  framework_version: string;
  revision: number;
  updated_at: string;
};

export type ReportSummary = {
  name: string;
  description: string;
  updated_at: string;
  uuts: number;
  aggregations: number;
};

export type Source = {
  catalog: string;
  schema: string;
  tables: Record<string, string | null>;
  columns: Record<string, string[]>;
  data_type: "RAW" | "RLE" | null;
  uut_source: "tag" | "column" | null;
  uut_key: string;
  issues: string[];
};

export type Uut = { id: string; containers: number; first: string | null; last: string | null };

export type Channel = { channel: Record<string, string>; unit: string; containers: number; min: number | null; max: number | null };

export type Me = { email: string; app: string; impulse_version: string; warehouse_configured: boolean };

export type StepKey = "data" | "uuts" | "signals" | "aggregations" | "run";

export type Issue = { step: Exclude<StepKey, "run">; item: string; message: string };

export type DataModelSetting = { key: string; default: string; description: string; example: string | null };

export type DataModelText = { yaml: string; reference: DataModelSetting[]; path: string; exists: boolean; error: string | null };

export type ModelCheck = { source: Source | null; uuts: number | null; issues: string[] };

export const STEPS: { key: StepKey; label: string }[] = [
  { key: "data", label: "Data" },
  { key: "uuts", label: "Units under test" },
  { key: "signals", label: "Signals" },
  { key: "aggregations", label: "Aggregations" },
  { key: "run", label: "Review & run" },
];

export type RunSummary = {
  run_id: number;
  active: boolean;
  state: string | null;
  result: string | null;
  message: string;
  start_time: number | null;
  end_time: number | null;
  url: string | null;
  error?: string | null;
  trace?: string | null;
};

export type Problem = { object: string; privilege: string; message: string };

export type Preflight = {
  ok: boolean;
  user: Problem[];
  app: Problem[];
  app_principal: { id: string; name: string };
  grants: string | null;
};

export type HistogramResult = { name: string; kind: "histogram"; unit: string; axis: string; bins: { lo: number; hi: number; value: number; recordings: number }[] };

export type Histogram2DResult = {
  name: string;
  kind: "histogram_2d";
  unit: string;
  x_axis: string;
  y_axis: string;
  cells: { x: number; y: number; x_lo: number; x_hi: number; y_lo: number; y_hi: number; value: number }[];
};

export type StatsResult = {
  name: string;
  kind: "statistics" | "point_values";
  unit: string;
  scoped: boolean;
  event: string | null;
  columns: string[];
  rows: { recording: number | string; instance: number | null; signal: string | null; values: Record<string, number | null> }[];
  more: boolean;
};

export type Results = {
  available: boolean;
  aggregations: (HistogramResult | Histogram2DResult | StatsResult)[];
};

export type ChatEvent =
  | { seq: number; type: "text"; text: string }
  | { seq: number; type: "tool"; id: string; name: string; label: string; status: "running" | "done" | "error"; detail?: string }
  | { seq: number; type: "spec"; report: string; revision: number }
  | { seq: number; type: "goto"; report: string; step: StepKey }
  | { seq: number; type: "run"; report: string; run_id: number }
  | { seq: number; type: "error"; message: string }
  | { seq: number; type: "done" };

export type ChatMessage = { role: "user" | "assistant"; text: string; tools: string[] };

export type Choices = { warehouse_id: string; serving_endpoint: string };

export type UserSettings = Choices & { default_warehouse_id: string; default_model: string | null };

export type SettingsOptions = {
  warehouses: { id: string; name: string; state: string; size: string }[];
  models: { name: string; ready: boolean }[];
  warehouses_error: string | null;
  models_error: string | null;
};
