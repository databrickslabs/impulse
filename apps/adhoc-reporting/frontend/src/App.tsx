import { useEffect } from "react";
import { api } from "./api";
import { ChatWindow } from "./components/ChatWindow";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { Home } from "./components/Home";
import { ReportPage } from "./components/ReportPage";
import { SettingsPage } from "./components/SettingsPage";
import { TopBar } from "./components/TopBar";
import { useAsync, useRoute } from "./hooks";

export default function App() {
  const route = useRoute();
  const me = useAsync(() => api.me(), []);

  useEffect(() => {
    api.warmup().catch(() => undefined);
  }, []);

  return (
    <>
      <ErrorBoundary resetKey={window.location.hash}>
        {route.page === "home" && (
          <>
            <TopBar me={me.data} />
            <Home />
          </>
        )}
        {route.page === "data-model" && <SettingsPage me={me.data} />}
        {route.page === "report" && <ReportPage key={route.name} me={me.data} name={route.name} step={route.step} />}
      </ErrorBoundary>
      <ChatWindow report={route.page === "report" ? route.name : null} />
    </>
  );
}
