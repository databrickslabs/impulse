export const NAME_RE = /^[a-z][a-z0-9_]{0,62}$/;

const RESERVED = new Set(
  (
    "and as assert async await break class continue def del elif else except finally for from global if import in is lambda " +
    "nonlocal not or pass raise return try while with yield match case type spark dbutils display report db page config sink table tables"
  ).split(" "),
);

export function signalNameProblem(name: string): string | null {
  if (!NAME_RE.test(name)) return "Lowercase letters, digits and underscores, starting with a letter";
  if (RESERVED.has(name) || name.startsWith("ev_")) return `${name} is reserved`;
  return null;
}

export function nameProblem(name: string): string | null {
  return NAME_RE.test(name) ? null : "Lowercase letters, digits and underscores, starting with a letter";
}

export function slug(text: string, taken: Iterable<string>, fallback = "signal"): string {
  let base = text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 40);
  if (!base || !/^[a-z]/.test(base)) base = `${fallback}_${base}`.replace(/_+$/, "");
  if (signalNameProblem(base)) base = `${base}_1`;
  const used = new Set(taken);
  let name = base;
  for (let i = 2; used.has(name); i++) name = `${base}_${i}`;
  return name;
}

export function niceBins(min: number, max: number, target = 20): number[] {
  if (!Number.isFinite(min) || !Number.isFinite(max) || min === max) return [min, min + 1];
  const rough = (max - min) / target;
  const magnitude = 10 ** Math.floor(Math.log10(rough));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * magnitude).find((s) => s >= rough) ?? rough;
  return range(Math.floor(min / step) * step, Math.ceil(max / step) * step, step);
}

export function range(start: number, stop: number, step: number): number[] {
  if (!(step > 0) || !(stop > start)) return [];
  const count = Math.min(Math.round((stop - start) / step), 499);
  const digits = Math.max(0, -Math.floor(Math.log10(step)) + 2);
  return Array.from({ length: count + 1 }, (_, i) => Number((start + i * step).toFixed(digits)));
}

export function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined) return "";
  if (Math.abs(value) >= 1e5 || (Math.abs(value) < 1e-3 && value !== 0)) return value.toExponential(2);
  return String(Number(value.toFixed(3)));
}
