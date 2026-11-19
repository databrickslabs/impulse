import { Check, Database, Settings } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { api } from "../api";
import type { Choices, SettingsOptions, UserSettings } from "../types";
import { Problems } from "./ui";

export function SettingsMenu() {
  const [anchor, setAnchor] = useState<{ top: number; right: number }>();
  const [current, setCurrent] = useState<UserSettings>();
  const [options, setOptions] = useState<SettingsOptions>();
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string>();
  const button = useRef<HTMLButtonElement>(null);
  const panel = useRef<HTMLDivElement>(null);
  const open = !!anchor;

  useEffect(() => {
    if (!open) return;
    setError(undefined);
    setSaved(false);
    api.settings().then(setCurrent, (e: Error) => setError(e.message));
    api.settingsOptions().then(setOptions, (e: Error) => setError(e.message));
    const close = () => setAnchor(undefined);
    const onPointer = (e: PointerEvent) => {
      const target = e.target as Node;
      if (!panel.current?.contains(target) && !button.current?.contains(target)) close();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") close();
    };
    document.addEventListener("pointerdown", onPointer);
    document.addEventListener("keydown", onKey);
    window.addEventListener("resize", close);
    return () => {
      document.removeEventListener("pointerdown", onPointer);
      document.removeEventListener("keydown", onKey);
      window.removeEventListener("resize", close);
    };
  }, [open]);

  useEffect(() => {
    if (!saved) return;
    const timer = window.setTimeout(() => setSaved(false), 1500);
    return () => window.clearTimeout(timer);
  }, [saved]);

  const toggle = () => {
    const rect = button.current?.getBoundingClientRect();
    setAnchor(open || !rect ? undefined : { top: rect.bottom + 6, right: window.innerWidth - rect.right });
  };

  const save = async (change: Partial<Choices>) => {
    if (!current) return;
    const previous = current;
    setCurrent({ ...current, ...change });
    setSaved(false);
    setError(undefined);
    try {
      setCurrent(await api.saveSettings({ warehouse_id: previous.warehouse_id, serving_endpoint: previous.serving_endpoint, ...change }));
      setSaved(true);
    } catch (e) {
      setCurrent(previous);
      setError((e as Error).message);
    }
  };

  const warehouses = options?.warehouses ?? [];
  const models = options?.models ?? [];
  const loading = !current || !options;
  const fallback = current && (warehouses.find((w) => w.id === current.default_warehouse_id)?.name ?? current.default_warehouse_id);

  return (
    <>
      <button ref={button} className={`btn ghost${open ? " active" : ""}`} onClick={toggle} aria-expanded={open} aria-haspopup="dialog">
        <Settings size={15} />
        Settings
      </button>
      {anchor &&
        createPortal(
          <div ref={panel} className="settings-pop" style={anchor} role="dialog" aria-label="Settings">
            <div className="field">
              <label htmlFor="settings-warehouse">SQL warehouse</label>
              <select
                id="settings-warehouse"
                className="select"
                value={current?.warehouse_id ?? ""}
                disabled={loading}
                onChange={(e) => void save({ warehouse_id: e.target.value })}
              >
                <option value="">{loading ? "Loading…" : fallback ? `App default · ${fallback}` : "App default"}</option>
                {current?.warehouse_id && !warehouses.some((w) => w.id === current.warehouse_id) && <option value={current.warehouse_id}>{current.warehouse_id}</option>}
                {warehouses.map((w) => (
                  <option key={w.id} value={w.id}>{`${w.name} · ${w.state.toLowerCase()}`}</option>
                ))}
              </select>
              <span className={options?.warehouses_error ? "hint bad" : "hint"}>
                {options?.warehouses_error ?? "Used for the app's queries. Report jobs run on serverless compute."}
              </span>
            </div>
            <div className="field">
              <label htmlFor="settings-model">Chat model</label>
              <select
                id="settings-model"
                className="select"
                value={current?.serving_endpoint ?? ""}
                disabled={loading}
                onChange={(e) => void save({ serving_endpoint: e.target.value })}
              >
                <option value="">{loading ? "Loading…" : current?.default_model ? `Automatic · ${current.default_model}` : "Automatic"}</option>
                {current?.serving_endpoint && !models.some((m) => m.name === current.serving_endpoint) && (
                  <option value={current.serving_endpoint}>{current.serving_endpoint}</option>
                )}
                {models.map((m) => (
                  <option key={m.name} value={m.name}>
                    {m.ready ? m.name : `${m.name} · not ready`}
                  </option>
                ))}
              </select>
              {options?.models_error && <span className="hint bad">{options.models_error}</span>}
            </div>
            <Problems messages={error ? [error] : []} />
            <div className="settings-foot">
              <a className="btn ghost" href="#/data-model" onClick={() => setAnchor(undefined)}>
                <Database size={15} />
                Data model
              </a>
              {saved && (
                <span className="settings-saved" role="status">
                  <Check size={14} />
                  Saved
                </span>
              )}
            </div>
          </div>,
          document.body,
        )}
    </>
  );
}
