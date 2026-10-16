import { Component, type ReactNode } from "react";
import { Banner } from "./ui";

export class ErrorBoundary extends Component<{ children: ReactNode; resetKey: string }, { error: Error | null }> {
  state = { error: null as Error | null };

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  componentDidUpdate(previous: { resetKey: string }) {
    if (previous.resetKey !== this.props.resetKey && this.state.error) this.setState({ error: null });
  }

  render() {
    if (!this.state.error) return this.props.children;
    return (
      <main className="main">
        <Banner>
          Something went wrong on this page: {this.state.error.message}{" "}
          <button className="btn ghost" style={{ height: 24 }} onClick={() => window.location.reload()}>
            Reload
          </button>
        </Banner>
      </main>
    );
  }
}
