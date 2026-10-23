import { ArrowUp, Check, Maximize2, MessageSquare, Minimize2, PanelRight, PanelRightClose, Trash2, X } from "lucide-react";
import { useCallback, useEffect, useRef, useState, type PointerEvent as ReactPointerEvent } from "react";
import { api } from "../api";
import { reportHref } from "../hooks";
import { bridge } from "../report";
import type { ChatEvent, ChatMessage } from "../types";
import { Markdown } from "./Markdown";

type Mode = "closed" | "open" | "docked" | "expanded";
type Geometry = { x: number; y: number; w: number; h: number };
type Saved = { mode: Mode; last: Mode; geometry: Geometry; dockWidth: number };
type Chip = { id: string; label: string; status: "running" | "done" | "error"; detail?: string };
type Bubble = { role: "user" | "assistant" | "error"; text: string; tools: Chip[] };

const KEY = "impulse.chat";
const MIN_W = 320;
const MIN_H = 360;
const EDGES = ["n", "s", "e", "w", "ne", "nw", "se", "sw"] as const;

const sleep = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));

function defaults(): Saved {
  const w = 420;
  const h = Math.min(600, window.innerHeight - 120);
  return { mode: "closed", last: "open", geometry: { x: window.innerWidth - w - 24, y: window.innerHeight - h - 24, w, h }, dockWidth: 420 };
}

function clamp(g: Geometry): Geometry {
  const w = Math.min(Math.max(g.w, MIN_W), window.innerWidth - 16);
  const h = Math.min(Math.max(g.h, MIN_H), window.innerHeight - 16);
  return {
    w,
    h,
    x: Math.min(Math.max(g.x, 8), window.innerWidth - w - 8),
    y: Math.min(Math.max(g.y, 8), window.innerHeight - h - 8),
  };
}

function restore(): Saved {
  try {
    const saved = { ...defaults(), ...JSON.parse(localStorage.getItem(KEY) ?? "{}") } as Saved;
    return { ...saved, geometry: clamp(saved.geometry) };
  } catch {
    return defaults();
  }
}

function resized(start: Geometry, edge: string, dx: number, dy: number): Geometry {
  let { x, y, w, h } = start;
  if (edge.includes("e")) w = Math.max(MIN_W, start.w + dx);
  if (edge.includes("s")) h = Math.max(MIN_H, start.h + dy);
  if (edge.includes("w")) {
    w = Math.max(MIN_W, start.w - dx);
    x = start.x + start.w - w;
  }
  if (edge.includes("n")) {
    h = Math.max(MIN_H, start.h - dy);
    y = start.y + start.h - h;
  }
  return clamp({ x, y, w, h });
}

function fromHistory(messages: ChatMessage[]): Bubble[] {
  return messages.map((m) => ({ role: m.role, text: m.text, tools: m.tools.map((label, i) => ({ id: `${i}`, label, status: "done" })) }));
}

function withAssistant(bubbles: Bubble[], change: (b: Bubble) => Bubble): Bubble[] {
  const last = bubbles[bubbles.length - 1];
  if (last?.role === "assistant") return [...bubbles.slice(0, -1), change(last)];
  return [...bubbles, change({ role: "assistant", text: "", tools: [] })];
}

export function ChatWindow({ report }: { report: string | null }) {
  const [state, setState] = useState<Saved>(restore);
  const drag = useRef<{ kind: string; startX: number; startY: number; geometry: Geometry; dockWidth: number } | null>(null);
  const [bubbles, setBubbles] = useState<Bubble[]>([]);
  const [loaded, setLoaded] = useState<string | null | undefined>(undefined);
  const [busy, setBusy] = useState(false);
  const [draft, setDraft] = useState("");
  const body = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    localStorage.setItem(KEY, JSON.stringify(state));
    document.documentElement.style.setProperty("--chat-dock", state.mode === "docked" ? `${state.dockWidth}px` : "0px");
  }, [state]);

  const setMode = useCallback((mode: Mode) => setState((s) => ({ ...s, mode, last: mode === "closed" ? s.last : mode })), []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setState((s) => (s.mode === "closed" ? { ...s, mode: s.last } : { ...s, mode: "closed" }));
      }
    };
    const onResize = () => setState((s) => ({ ...s, geometry: clamp(s.geometry) }));
    window.addEventListener("keydown", onKey);
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("resize", onResize);
    };
  }, []);

  useEffect(() => {
    if (state.mode === "closed" || busy || loaded === report) return;
    setLoaded(report);
    setBubbles([]);
    api.chatHistory(report).then(
      (messages) => setBubbles(fromHistory(messages)),
      () => setBubbles([]),
    );
  }, [state.mode, report, loaded, busy]);

  useEffect(() => {
    body.current?.scrollTo({ top: body.current.scrollHeight, behavior: "smooth" });
  }, [bubbles]);

  useEffect(() => {
    if (state.mode !== "closed") input.current?.focus();
  }, [state.mode]);

  const apply = (event: ChatEvent) => {
    if (event.type === "text") setBubbles((b) => withAssistant(b, (m) => ({ ...m, text: m.text ? `${m.text}\n\n${event.text}` : event.text })));
    else if (event.type === "tool")
      setBubbles((b) =>
        withAssistant(b, (m) => {
          const chip = { id: event.id, label: event.label, status: event.status, detail: event.detail };
          const known = m.tools.some((t) => t.id === event.id);
          return { ...m, tools: known ? m.tools.map((t) => (t.id === event.id ? chip : t)) : [...m.tools, chip] };
        }),
      );
    else if (event.type === "spec" || event.type === "run") window.dispatchEvent(new CustomEvent(`impulse:${event.type}`, { detail: event }));
    else if (event.type === "goto") {
      setLoaded(event.report);
      window.location.hash = reportHref(event.report, event.step);
    } else if (event.type === "error") setBubbles((b) => [...b, { role: "error", text: event.message, tools: [] }]);
  };

  const send = async (text: string) => {
    const message = text.trim();
    if (!message || busy) return;
    setDraft("");
    setBusy(true);
    setBubbles((b) => [...b, { role: "user", text: message, tools: [] }]);
    try {
      if (bridge.flush && bridge.report === report) await bridge.flush();
      const { turn_id } = await api.chatStart(message, report);
      let after = 0;
      for (let done = false; !done; ) {
        await sleep(after === 0 ? 300 : 700);
        const response = await api.chatEvents(turn_id, after);
        response.events.forEach(apply);
        after += response.events.length;
        done = response.done;
      }
    } catch (e) {
      setBubbles((b) => [...b, { role: "error", text: (e as Error).message, tools: [] }]);
    } finally {
      setBusy(false);
    }
  };

  const clear = async () => {
    await api.chatClear(report).catch(() => undefined);
    setBubbles([]);
  };

  const begin = (kind: string) => (e: ReactPointerEvent) => {
    if ((e.target as HTMLElement).closest("button")) return;
    if (kind === "move" && state.mode !== "open") return;
    e.preventDefault();
    (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
    drag.current = { kind, startX: e.clientX, startY: e.clientY, geometry: state.geometry, dockWidth: state.dockWidth };
  };

  const move = (e: ReactPointerEvent) => {
    const d = drag.current;
    if (!d) return;
    const dx = e.clientX - d.startX;
    const dy = e.clientY - d.startY;
    if (d.kind === "move") setState((s) => ({ ...s, geometry: clamp({ ...d.geometry, x: d.geometry.x + dx, y: d.geometry.y + dy }) }));
    else if (d.kind === "dock") setState((s) => ({ ...s, dockWidth: Math.min(Math.max(MIN_W, d.dockWidth - dx), window.innerWidth * 0.6) }));
    else setState((s) => ({ ...s, geometry: resized(d.geometry, d.kind, dx, dy) }));
  };

  const end = () => {
    drag.current = null;
  };

  if (state.mode === "closed") {
    return (
      <button className="chat-launcher" onClick={() => setMode(state.last)} aria-label="Open assistant" title="Assistant (Ctrl K)">
        <MessageSquare size={20} />
      </button>
    );
  }

  const style =
    state.mode === "docked"
      ? { top: "var(--topbar)", right: 0, bottom: 0, width: state.dockWidth }
      : state.mode === "expanded"
        ? { top: "6vh", left: "8vw", width: "84vw", height: "88vh" }
        : { left: state.geometry.x, top: state.geometry.y, width: state.geometry.w, height: state.geometry.h };

  return (
    <section className={`chat${state.mode === "docked" ? " docked" : ""}`} style={style} aria-label="Assistant" onPointerMove={move} onPointerUp={end}>
      {state.mode === "open" && EDGES.map((edge) => <div key={edge} className={`resize ${edge}`} onPointerDown={begin(edge)} />)}
      {state.mode === "docked" && <div className="resize w" onPointerDown={begin("dock")} />}
      <header className="chat-head" onPointerDown={begin("move")}>
        <span className="chat-title">
          <span className={`chat-dot${busy ? " busy" : ""}`} />
          Assistant
        </span>
        <span className="chip chat-context">{report ?? "No report open"}</span>
        <span className="spacer" />
        <button className="btn ghost icon" onClick={() => void clear()} disabled={busy || !bubbles.length} aria-label="Clear conversation" title="Clear conversation">
          <Trash2 size={15} />
        </button>
        <button
          className="btn ghost icon"
          onClick={() => setMode(state.mode === "docked" ? "open" : "docked")}
          aria-label={state.mode === "docked" ? "Undock" : "Dock right"}
          title={state.mode === "docked" ? "Undock" : "Dock right"}
        >
          {state.mode === "docked" ? <PanelRightClose size={15} /> : <PanelRight size={15} />}
        </button>
        <button
          className="btn ghost icon"
          onClick={() => setMode(state.mode === "expanded" ? "open" : "expanded")}
          aria-label={state.mode === "expanded" ? "Restore size" : "Expand"}
          title={state.mode === "expanded" ? "Restore size" : "Expand"}
        >
          {state.mode === "expanded" ? <Minimize2 size={15} /> : <Maximize2 size={15} />}
        </button>
        <button className="btn ghost icon" onClick={() => setMode("closed")} aria-label="Close" title="Close (Ctrl K)">
          <X size={15} />
        </button>
      </header>
      <div className="chat-body" ref={body}>
        {bubbles.map((b, i) => (
          <div key={i} className={`msg ${b.role}`}>
            {b.tools.length > 0 && (
              <div className="tool-chips">
                {b.tools.map((t) => (
                  <span key={t.id} className={`tool-chip ${t.status}`} title={t.detail || undefined}>
                    {t.status === "running" ? <span className="spinner" /> : t.status === "done" ? <Check size={11} /> : <X size={11} />}
                    {t.label}
                  </span>
                ))}
              </div>
            )}
            {b.role === "assistant" ? b.text && <Markdown text={b.text} /> : b.text}
          </div>
        ))}
        {busy && bubbles[bubbles.length - 1]?.role === "user" && (
          <div className="msg assistant thinking">
            <span className="spinner" />
            Thinking
          </div>
        )}
      </div>
      <form
        className="chat-compose"
        onSubmit={(e) => {
          e.preventDefault();
          void send(draft);
        }}
      >
        <textarea
          ref={input}
          rows={1}
          value={draft}
          placeholder={busy ? "Working…" : "Ask the assistant"}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              void send(draft);
            }
          }}
          aria-label="Message"
        />
        <button className="btn primary icon" style={{ height: 38, width: 38 }} disabled={busy || !draft.trim()} aria-label="Send" type="submit">
          <ArrowUp size={16} />
        </button>
      </form>
    </section>
  );
}
