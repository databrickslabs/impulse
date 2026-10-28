import { Activity, ChevronRight } from "lucide-react";
import type { ReactNode } from "react";
import type { Me } from "../types";
import { SettingsMenu } from "./SettingsMenu";

function initials(email: string): string {
  const name = email.split("@")[0] ?? "";
  const parts = name.split(/[._-]/).filter(Boolean);
  return ((parts[0]?.[0] ?? "") + (parts[1]?.[0] ?? "")).toUpperCase() || "?";
}

export function TopBar({ me, crumb, center, status }: { me?: Me; crumb?: string; center?: ReactNode; status?: ReactNode }) {
  return (
    <header className="topbar">
      <div className="topbar-left">
        <a className="logo" href="#/">
          <span className="logo-mark">
            <Activity size={16} strokeWidth={2.6} />
          </span>
          Impulse
        </a>
        <nav className="crumbs" aria-label="Breadcrumb">
          <ChevronRight size={14} />
          <a href="#/">Reports</a>
          {crumb && (
            <>
              <ChevronRight size={14} />
              <span className="current">{crumb}</span>
            </>
          )}
        </nav>
      </div>
      <div>{center}</div>
      <div className="topbar-right">
        {status}
        <SettingsMenu />
        {me && <span className="chip">Impulse {me.impulse_version}</span>}
        {me && (
          <span className="avatar" title={me.email}>
            {initials(me.email)}
          </span>
        )}
      </div>
    </header>
  );
}
