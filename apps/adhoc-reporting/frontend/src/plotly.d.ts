declare module "plotly.js-cartesian-dist-min" {
  const Plotly: {
    react: (element: HTMLElement, data: object[], layout: object, config: object) => Promise<void>;
    purge: (element: HTMLElement) => void;
    Plots: { resize: (element: HTMLElement) => void };
  };
  export default Plotly;
}
