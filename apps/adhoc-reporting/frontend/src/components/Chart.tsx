import { useEffect, useRef } from "react";

const AXIS = { gridcolor: "#2a2a2e", zerolinecolor: "#3a3a40", linecolor: "#3a3a40", tickcolor: "#3a3a40", automargin: true };

const BASE = {
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  font: { family: "-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif", color: "#a1a1aa", size: 12 },
  margin: { l: 64, r: 16, t: 12, b: 64 },
  hoverlabel: { bgcolor: "#1c1c1f", bordercolor: "#3a3a40", font: { color: "#f4f4f5" } },
  bargap: 0.08,
};

export const HEAT = [
  [0, "#141416"],
  [0.35, "#7a1a10"],
  [0.7, "#ff3621"],
  [1, "#ffffff"],
];

type Axis = Record<string, unknown>;

export function Chart({ data, layout, height = 320 }: { data: object[]; layout: { xaxis?: Axis; yaxis?: Axis } & Record<string, unknown>; height?: number }) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let live = true;
    const element = ref.current;
    void import("plotly.js-cartesian-dist-min").then(({ default: Plotly }) => {
      if (!live || !element) return;
      void Plotly.react(
        element,
        data,
        { ...BASE, ...layout, height, xaxis: { ...AXIS, ...layout.xaxis }, yaxis: { ...AXIS, ...layout.yaxis } },
        { displaylogo: false, responsive: true, modeBarButtonsToRemove: ["select2d", "lasso2d", "autoScale2d"] },
      );
    });
    return () => {
      live = false;
    };
  }, [data, layout, height]);

  useEffect(() => {
    const element = ref.current;
    return () => {
      if (element) void import("plotly.js-cartesian-dist-min").then(({ default: Plotly }) => Plotly.purge(element));
    };
  }, []);

  return <div ref={ref} style={{ height }} />;
}
