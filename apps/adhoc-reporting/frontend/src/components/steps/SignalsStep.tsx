import { Check, Flag, Plus, Search, Trash2, Waves } from "lucide-react";
import { useState } from "react";
import { api } from "../../api";
import { channelLabel, renameEvent, renameSignal, signalUses } from "../../edits";
import { useAsync } from "../../hooks";
import { formatNumber, nameProblem, signalNameProblem, slug } from "../../names";
import { useIssues, useReport } from "../../report";
import type { Channel, ChannelSignal, ReportEvent, Signal } from "../../types";
import { Banner, Empty, NameInput, Problems, SkeletonRows, StepHead, TextInput, useSlow } from "../ui";

const LIMIT = 200;

function ChannelPicker() {
  const { spec, update } = useReport();
  const { catalog, schema } = spec.source;
  const key = JSON.stringify([catalog, schema, spec.uuts, spec.window]);
  const channels = useAsync(() => (catalog && schema ? api.channels(catalog, schema, spec.uuts, spec.window) : null), [key]);
  const slow = useSlow(channels.loading);
  const [query, setQuery] = useState("");
  const added = new Set(spec.signals.flatMap((s) => (s.kind === "channel" ? [channelLabel(s.channel)] : [])));
  const all = channels.data?.channels ?? [];
  const needle = query.trim().toLowerCase();
  const matches = needle ? all.filter((c) => channelLabel(c.channel).toLowerCase().includes(needle)) : all;
  const recordings = Math.max(0, ...all.map((c) => c.containers));

  const add = (c: Channel) =>
    update((s) => {
      const signal: ChannelSignal = {
        kind: "channel",
        name: slug(channelLabel(c.channel), s.signals.map((x) => x.name)),
        channel: c.channel,
        unit: c.unit,
        desc: "",
        min: c.min,
        max: c.max,
      };
      return { ...s, signals: [...s.signals, signal] };
    });

  return (
    <div className="card picker">
      <div className="card-head">
        <h2>Channels</h2>
        <span className="sub row" style={{ margin: 0 }}>
          {channels.loading && slow && <span className="spinner" />}
          {channels.data ? `${all.length} channels` : slow ? "Starting SQL warehouse" : ""}
        </span>
      </div>
      <div className="search">
        <Search size={14} />
        <input className="input" placeholder="Search channels" value={query} onChange={(e) => setQuery(e.target.value)} aria-label="Search channels" />
      </div>
      {channels.error && (
        <div className="card-body">
          <Banner>{channels.error.message}</Banner>
        </div>
      )}
      <div className="picker-list">
        <table className="table compact">
          <tbody>
            {channels.loading && !channels.data ? (
              <SkeletonRows rows={7} columns={3} />
            ) : (
              matches.slice(0, LIMIT).map((c) => {
                const label = channelLabel(c.channel);
                return (
                  <tr key={label}>
                    <td>
                      <div className="channel-name">{label}</div>
                      <div className="channel-meta">
                        {c.unit && <span>{c.unit}</span>}
                        {c.min !== null && c.max !== null && (
                          <span>
                            {formatNumber(c.min)} to {formatNumber(c.max)}
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="num" title="Recordings that contain this channel">
                      {c.containers}/{recordings}
                    </td>
                    <td style={{ textAlign: "right", width: 44 }}>
                      {added.has(label) ? (
                        <span className="added" title="Added">
                          <Check size={15} />
                        </span>
                      ) : (
                        <button className="btn icon" onClick={() => add(c)} aria-label={`Add ${label}`} title="Add as signal">
                          <Plus size={15} />
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
        {channels.data && matches.length === 0 && <div className="more">No channels match</div>}
        {matches.length > LIMIT && (
          <div className="more">
            Showing {LIMIT} of {matches.length}; refine the search
          </div>
        )}
      </div>
    </div>
  );
}

function SignalList() {
  const { spec, update } = useReport();
  const issues = useIssues("signals");
  const names = spec.signals.map((s) => s.name);

  const set = (index: number, change: Partial<Signal>) =>
    update((s) => ({ ...s, signals: s.signals.map((x, i) => (i === index ? ({ ...x, ...change } as Signal) : x)) }));

  const remove = (name: string) => {
    const uses = signalUses(spec, name);
    if (uses && !window.confirm(`${name} is used in ${uses} place${uses > 1 ? "s" : ""}. Remove it anyway?`)) return;
    update((s) => ({ ...s, signals: s.signals.filter((x) => x.name !== name) }));
  };

  const derive = () =>
    update((s) => ({ ...s, signals: [...s.signals, { kind: "expression", name: slug("derived", s.signals.map((x) => x.name)), expr: "", unit: "", desc: "" }] }));

  return (
    <div className="card">
      <div className="card-head">
        <h2>Signals</h2>
        <button className="btn" onClick={derive} disabled={!spec.signals.length}>
          <Plus size={15} />
          Derived signal
        </button>
      </div>
      {spec.signals.length === 0 ? (
        <Empty icon={<Waves size={22} />} title="No signals yet">
          Add channels from the list.
        </Empty>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th style={{ width: 190 }}>Name</th>
              <th>Definition</th>
              <th style={{ width: 96 }}>Unit</th>
              <th style={{ width: 44 }} />
            </tr>
          </thead>
          <tbody>
            {spec.signals.map((s, i) => {
              const problems = issues.of(s.name);
              return (
                <tr key={i}>
                  <td>
                    <NameInput
                      value={s.name}
                      taken={names.filter((n) => n !== s.name)}
                      check={signalNameProblem}
                      label="Signal name"
                      onCommit={(to) => update((x) => renameSignal(x, s.name, to))}
                    />
                  </td>
                  <td>
                    {s.kind === "channel" ? (
                      <span className="channel-name">{channelLabel(s.channel)}</span>
                    ) : (
                      <TextInput
                        mono
                        value={s.expr}
                        placeholder="(amb_temp + intake_temp) / 2"
                        invalid={problems.length > 0}
                        label={`Expression for ${s.name}`}
                        onChange={(expr) => set(i, { expr })}
                      />
                    )}
                    <Problems messages={problems} />
                  </td>
                  <td>
                    <TextInput value={s.unit} label={`Unit of ${s.name}`} onChange={(unit) => set(i, { unit })} />
                  </td>
                  <td>
                    <button className="btn ghost icon danger" onClick={() => remove(s.name)} aria-label={`Remove ${s.name}`}>
                      <Trash2 size={15} />
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

function EventList() {
  const { spec, update } = useReport();
  const issues = useIssues("signals");
  const names = spec.events.map((e) => e.name);

  const set = (index: number, change: Partial<ReportEvent>) =>
    update((s) => ({ ...s, events: s.events.map((x, i) => (i === index ? { ...x, ...change } : x)) }));

  const remove = (name: string) => {
    const uses = spec.aggregations.filter((a) => a.event === name).length;
    if (uses && !window.confirm(`${name} scopes ${uses} aggregation${uses > 1 ? "s" : ""}. Remove it anyway?`)) return;
    update((s) => ({ ...s, events: s.events.filter((x) => x.name !== name), aggregations: s.aggregations.map((a) => (a.event === name ? { ...a, event: "" } : a)) }));
  };

  const add = () =>
    update((s) => ({ ...s, events: [...s.events, { kind: "condition", name: slug("event", s.events.map((x) => x.name), "event"), expr: "", desc: "" }] }));

  return (
    <div className="card">
      <div className="card-head">
        <h2>Events</h2>
        <button className="btn" onClick={add} disabled={!spec.signals.length}>
          <Plus size={15} />
          Event
        </button>
      </div>
      {spec.events.length === 0 ? (
        <Empty icon={<Flag size={22} />} title="No events" />
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th style={{ width: 190 }}>Name</th>
              <th style={{ width: 150 }}>Kind</th>
              <th>Expression</th>
              <th style={{ width: 220 }}>Description</th>
              <th style={{ width: 44 }} />
            </tr>
          </thead>
          <tbody>
            {spec.events.map((e, i) => {
              const problems = issues.of(e.name);
              return (
                <tr key={i}>
                  <td>
                    <NameInput
                      value={e.name}
                      taken={names.filter((n) => n !== e.name)}
                      check={nameProblem}
                      label="Event name"
                      onCommit={(to) => update((x) => renameEvent(x, e.name, to))}
                    />
                  </td>
                  <td>
                    <select className="select" value={e.kind} onChange={(ev) => set(i, { kind: ev.target.value as ReportEvent["kind"] })} aria-label="Event kind">
                      <option value="condition">Condition</option>
                      <option value="points">Points in time</option>
                    </select>
                  </td>
                  <td>
                    <TextInput
                      mono
                      value={e.expr}
                      placeholder={e.kind === "condition" ? "(veh_spd > 0) & (eng_rpm > 3000)" : "veh_spd.rising_edges()"}
                      invalid={problems.length > 0}
                      label={`Expression for ${e.name}`}
                      onChange={(expr) => set(i, { expr })}
                    />
                    <Problems messages={problems} />
                  </td>
                  <td>
                    <TextInput value={e.desc} placeholder="Optional" label={`Description of ${e.name}`} onChange={(desc) => set(i, { desc })} />
                  </td>
                  <td>
                    <button className="btn ghost icon danger" onClick={() => remove(e.name)} aria-label={`Remove ${e.name}`}>
                      <Trash2 size={15} />
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

export function SignalsStep() {
  const { spec } = useReport();
  const issues = useIssues("signals");
  return (
    <>
      <StepHead
        title="Signals"
        sub="Pick channels, derive signals from them and define the events that scope aggregations."
        report={spec.name}
        next={{ step: "aggregations", label: "Aggregations" }}
        ready={spec.signals.length > 0 && issues.all.length === 0}
      />
      {!spec.uuts.length && <Banner warn>No units under test are selected, so channels from all recordings are listed.</Banner>}
      <div className="split">
        <ChannelPicker />
        <SignalList />
      </div>
      <EventList />
    </>
  );
}
