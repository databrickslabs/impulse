import { BarChart3, Crosshair, Grid3x3, Sigma } from "lucide-react";
import { useMemo } from "react";
import { formatNumber } from "../names";
import type { Histogram2DResult, HistogramResult, Results as ResultsData, StatsResult } from "../types";
import { Chart, HEAT } from "./Chart";
import { Empty } from "./ui";

const range = (lo: number, hi: number) => `${formatNumber(lo)} to ${formatNumber(hi)}`;

export function duration(seconds: number | null): string {
  if (seconds === null) return "";
  if (seconds < 120) return `${formatNumber(seconds)} s`;
  if (seconds < 7200) return `${formatNumber(seconds / 60)} min`;
  return `${formatNumber(seconds / 3600)} h`;
}

function HistogramChart({ result }: { result: HistogramResult }) {
  const data = useMemo(
    () => [
      {
        type: "bar",
        x: result.bins.map((b) => range(b.lo, b.hi)),
        y: result.bins.map((b) => b.value),
        marker: { color: "#ff3621" },
        hovertemplate: `%{x} ${result.axis}<br>%{y:.4g} ${result.unit}<extra></extra>`,
      },
    ],
    [result],
  );
  const layout = useMemo(
    () => ({ xaxis: { title: { text: result.axis }, type: "category" }, yaxis: { title: { text: result.unit === "s" ? "Time (s)" : result.unit } } }),
    [result],
  );
  return <Chart data={data} layout={layout} />;
}

function HeatmapChart({ result }: { result: Histogram2DResult }) {
  const data = useMemo(() => {
    const xs = [...new Map(result.cells.map((c) => [c.x, range(c.x_lo, c.x_hi)])).entries()].sort((a, b) => a[0] - b[0]);
    const ys = [...new Map(result.cells.map((c) => [c.y, range(c.y_lo, c.y_hi)])).entries()].sort((a, b) => a[0] - b[0]);
    const z = ys.map(([y]) => xs.map(([x]) => result.cells.find((c) => c.x === x && c.y === y)?.value ?? 0));
    return [
      {
        type: "heatmap",
        x: xs.map(([, label]) => label),
        y: ys.map(([, label]) => label),
        z,
        colorscale: HEAT,
        hovertemplate: `x %{x}<br>y %{y}<br>%{z:.4g} s<extra></extra>`,
        colorbar: { outlinewidth: 0, tickfont: { color: "#a1a1aa" }, title: { text: "s", font: { color: "#a1a1aa" } } },
      },
    ];
  }, [result]);
  const layout = useMemo(
    () => ({ xaxis: { title: { text: result.x_axis }, type: "category" }, yaxis: { title: { text: result.y_axis }, type: "category" } }),
    [result],
  );
  return <Chart data={data} layout={layout} height={380} />;
}

function StatsTable({ result }: { result: StatsResult }) {
  const signals = result.kind === "statistics";
  const unit = result.unit ? ` (${result.unit})` : "";
  return (
    <>
      <div className="table-scroll">
        <table className="table">
          <thead>
            <tr>
              <th>Recording</th>
              {result.scoped && <th>{result.event ?? "Instance"} #</th>}
              {signals && <th>Signal</th>}
              {result.columns.map((c) => (
                <th key={c} className="right">
                  {c}
                  {unit}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((r, i) => (
              <tr key={i}>
                <td className="num">{r.recording}</td>
                {result.scoped && <td className="num">{r.instance}</td>}
                {signals && <td style={{ fontWeight: 550 }}>{r.signal}</td>}
                {result.columns.map((c) => {
                  const value = r.values[c];
                  return (
                    <td key={c} className="right num stat">
                      {value == null ? "" : formatNumber(value)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {result.more && <div className="more">Showing the first {result.rows.length} rows</div>}
    </>
  );
}

const ICONS = {
  histogram: <BarChart3 size={15} />,
  histogram_2d: <Grid3x3 size={15} />,
  statistics: <Sigma size={15} />,
  point_values: <Crosshair size={15} />,
};

export function ResultsView({ results }: { results: ResultsData }) {
  if (!results.available) {
    return (
      <div className="card">
        <Empty icon={<BarChart3 size={22} />} title="No results yet">
          Run the report to compute its gold tables.
        </Empty>
      </div>
    );
  }
  return (
    <>
      {results.aggregations.map((a) => {
        const empty = a.kind === "histogram" ? !a.bins.length : a.kind === "histogram_2d" ? !a.cells.length : !a.rows.length;
        return (
          <div className="card" key={a.name}>
            <div className="card-head">
              <div className="row">
                <span className="agg-icon">{ICONS[a.kind]}</span>
                <h2>{a.name}</h2>
              </div>
            </div>
            {empty ? (
              <div className="more">No rows for this aggregation. Run the report again after changing it.</div>
            ) : a.kind === "histogram" ? (
              <div className="card-body">
                <HistogramChart result={a} />
              </div>
            ) : a.kind === "histogram_2d" ? (
              <div className="card-body">
                <HeatmapChart result={a} />
              </div>
            ) : (
              <StatsTable result={a} />
            )}
          </div>
        );
      })}
    </>
  );
}
