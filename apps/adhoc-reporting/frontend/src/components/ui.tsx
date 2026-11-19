import { AlertTriangle, ArrowRight } from "lucide-react";
import { useEffect, useState, type ReactNode } from "react";
import { reportHref } from "../hooks";
import type { StepKey } from "../types";

export function Field({ label, hint, bad, children }: { label: string; hint?: string; bad?: boolean; children: ReactNode }) {
  return (
    <div className="field">
      <label>{label}</label>
      {children}
      {hint && <span className={bad ? "hint bad" : "hint"}>{hint}</span>}
    </div>
  );
}

export function Select({
  value,
  options,
  placeholder,
  disabled,
  onChange,
}: {
  value: string;
  options: string[] | undefined;
  placeholder: string;
  disabled?: boolean;
  onChange: (value: string) => void;
}) {
  const list = options ?? [];
  const known = value === "" || list.includes(value);
  return (
    <select className="select" value={value} disabled={disabled || !options} onChange={(e) => onChange(e.target.value)}>
      <option value="">{options ? placeholder : "Loading…"}</option>
      {!known && <option value={value}>{value}</option>}
      {list.map((option) => (
        <option key={option} value={option}>
          {option}
        </option>
      ))}
    </select>
  );
}

export function Banner({ children, warn }: { children: ReactNode; warn?: boolean }) {
  return (
    <div className={warn ? "banner warn" : "banner"} role="alert">
      <AlertTriangle size={16} style={{ flexShrink: 0, marginTop: 2 }} />
      <div>{children}</div>
    </div>
  );
}

export function Problems({ messages }: { messages: string[] }) {
  if (!messages.length) return null;
  return (
    <div className="problems" role="alert">
      {messages.map((m) => (
        <div key={m}>{m}</div>
      ))}
    </div>
  );
}

export function SkeletonRows({ rows, columns }: { rows: number; columns: number }) {
  return (
    <>
      {Array.from({ length: rows }, (_, r) => (
        <tr key={r}>
          {Array.from({ length: columns }, (_, c) => (
            <td key={c}>
              <div className="skeleton" style={{ width: c === 0 ? 18 : `${50 + ((r * 7 + c * 13) % 40)}%` }} />
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

export function Empty({ icon, title, children }: { icon: ReactNode; title: string; children?: ReactNode }) {
  return (
    <div className="empty">
      <div className="glyph">{icon}</div>
      <div style={{ color: "var(--text)", fontWeight: 600 }}>{title}</div>
      {children}
    </div>
  );
}

export function StepHead({ title, sub, report, next, ready }: { title: string; sub: string; report: string; next?: { step: StepKey; label: string }; ready?: boolean }) {
  return (
    <div className="page-head">
      <div>
        <h1>{title}</h1>
        <p className="sub">{sub}</p>
      </div>
      {next && (
        <a className={`btn primary${ready ? "" : " disabled"}`} href={ready ? reportHref(report, next.step) : undefined} aria-disabled={!ready}>
          {next.label}
          <ArrowRight size={15} />
        </a>
      )}
    </div>
  );
}

export function NameInput({
  value,
  taken,
  check,
  onCommit,
  label,
}: {
  value: string;
  taken: string[];
  check: (name: string) => string | null;
  onCommit: (name: string) => void;
  label: string;
}) {
  const [text, setText] = useState(value);
  useEffect(() => setText(value), [value]);
  const problem = text === value ? null : (check(text) ?? (taken.includes(text) ? `${text} is already used` : null));
  const commit = () => {
    if (text === value) return;
    if (problem) setText(value);
    else onCommit(text);
  };
  return (
    <div className="name-input">
      <input
        className={`input mono${problem ? " invalid" : ""}`}
        value={text}
        aria-label={label}
        spellCheck={false}
        onChange={(e) => setText(e.target.value.trim())}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === "Enter") (e.target as HTMLInputElement).blur();
          if (e.key === "Escape") setText(value);
        }}
      />
      {problem && <span className="hint bad">{problem}</span>}
    </div>
  );
}

export function TextInput({
  value,
  onChange,
  placeholder,
  mono,
  invalid,
  label,
}: {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  mono?: boolean;
  invalid?: boolean;
  label: string;
}) {
  return (
    <input
      className={`input${mono ? " mono" : ""}${invalid ? " invalid" : ""}`}
      value={value}
      placeholder={placeholder}
      aria-label={label}
      spellCheck={false}
      onChange={(e) => onChange(e.target.value)}
    />
  );
}

export function Toggles<T extends string>({ options, value, onChange, label }: { options: readonly T[]; value: T[]; onChange: (value: T[]) => void; label: string }) {
  return (
    <div className="toggles" role="group" aria-label={label}>
      {options.map((option) => {
        const on = value.includes(option);
        return (
          <button
            key={option}
            type="button"
            className={`toggle${on ? " on" : ""}`}
            aria-pressed={on}
            onClick={() => onChange(on ? value.filter((v) => v !== option) : options.filter((o) => o === option || value.includes(o)))}
          >
            {option}
          </button>
        );
      })}
    </div>
  );
}

export function useSlow(loading: boolean, ms = 3000): boolean {
  const [slow, setSlow] = useState(false);
  useEffect(() => {
    if (!loading) {
      setSlow(false);
      return;
    }
    const timer = window.setTimeout(() => setSlow(true), ms);
    return () => window.clearTimeout(timer);
  }, [loading, ms]);
  return slow;
}
