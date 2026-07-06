import { useEffect, useState } from "react";
import { GitCompareArrows, History, Map, Route, Sparkles } from "lucide-react";
import { AuthProvider, useAuth } from "@/components/AuthProvider";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { PrivacyDialog } from "@/components/PrivacyDialog";
import { UserMenu } from "@/components/UserMenu";
import { ComparePage } from "@/pages/ComparePage";
import { HistoryPage } from "@/pages/HistoryPage";
import { InsightsPage } from "@/pages/InsightsPage";
import { PlotPage } from "@/pages/PlotPage";
import { cn } from "@/lib/utils";

type Tab = "insights" | "plot" | "compare" | "history";

interface TabDef {
  id: Tab;
  label: string;
  icon: React.ReactNode;
}

const TABS: TabDef[] = [
  { id: "insights", label: "Insights", icon: <Sparkles className="h-4 w-4" /> },
  { id: "plot", label: "Plot", icon: <Map className="h-4 w-4" /> },
  { id: "compare", label: "Compare", icon: <GitCompareArrows className="h-4 w-4" /> },
  { id: "history", label: "History", icon: <History className="h-4 w-4" /> },
];

function Tabs({
  tab,
  tabs,
  onChange,
}: {
  tab: Tab;
  tabs: TabDef[];
  onChange: (t: Tab) => void;
}) {
  return (
    <div
      role="tablist"
      className="inline-flex items-center rounded-lg border border-white/10 bg-white/10 p-1 text-sm shadow-inner shadow-black/10 backdrop-blur"
    >
      {tabs.map((t) => (
        <button
          key={t.id}
          role="tab"
          aria-selected={tab === t.id}
          aria-label={t.label}
          title={t.label}
          className={cn(
            // Tighter horizontal padding on phones so all four tabs +
            // the user menu fit on one line; full padding from sm.
            "inline-flex items-center gap-1.5 rounded-md px-2 py-1.5 sm:px-3",
            tab === t.id
              ? "bg-white text-cyan-950 shadow-sm"
              : "text-slate-200 hover:bg-white/10 hover:text-white",
          )}
          onClick={() => onChange(t.id)}
        >
          {t.icon}
          {/* Label is hidden on phones (icon-only tabs) but kept in
              the accessibility tree via ``sr-only`` + ``aria-label``. */}
          <span className="sr-only sm:not-sr-only">{t.label}</span>
        </button>
      ))}
    </div>
  );
}

function AppShell() {
  const [tab, setTab] = useState<Tab>("insights");
  const [privacyOpen, setPrivacyOpen] = useState(false);
  const { user, capabilities } = useAuth();
  const tabs = capabilities.history_enabled
    ? TABS
    : TABS.filter((t) => t.id !== "history");

  useEffect(() => {
    if (tab === "history" && !capabilities.history_enabled) setTab("insights");
  }, [tab, capabilities.history_enabled]);

  // Replay from the History tab lands on the Insights tab. HistoryPage
  // writes the stored request to localStorage and dispatches the event;
  // InsightsPage listens for the same event to rehydrate its state.
  useEffect(() => {
    const handler = () => setTab("insights");
    window.addEventListener("bhulan:replay", handler);
    return () => window.removeEventListener("bhulan:replay", handler);
  }, []);

  return (
    <div className="min-h-full">
      <header className="sticky top-0 z-30 border-b border-cyan-900/40 bg-slate-950/95 text-white shadow-lg shadow-slate-900/10 backdrop-blur">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-3 px-4 py-3">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-cyan-400/15 text-cyan-200 ring-1 ring-cyan-300/20">
              <Route className="h-5 w-5" />
            </div>
            <div>
              <div className="text-lg font-semibold">Bhulan</div>
              <div className="text-xs text-slate-300">
                Find stops, trips, and frequent locations in any GPS track
              </div>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <Tabs tab={tab} onChange={setTab} tabs={tabs} />
            <UserMenu />
          </div>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-4 py-7 sm:py-8">
        {tab === "insights" && (
          <ErrorBoundary label="Insights">
            <InsightsPage />
          </ErrorBoundary>
        )}
        {tab === "plot" && (
          <ErrorBoundary label="Plot">
            <PlotPage />
          </ErrorBoundary>
        )}
        {tab === "compare" && (
          <ErrorBoundary label="Compare">
            <ComparePage />
          </ErrorBoundary>
        )}
        {tab === "history" && (
          <ErrorBoundary label="History">
            <HistoryPage />
          </ErrorBoundary>
        )}
      </main>
      <footer className="mx-auto flex max-w-6xl flex-col gap-2 px-4 py-8 text-xs text-slate-600 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <span>
            {capabilities.history_enabled
              ? user
                ? "Signed in — your insights runs are saved to your history."
                : "Anonymous runs aren\u2019t stored. Sign in to save your history."
              : "Public demo mode — runs are processed in memory and aren\u2019t stored."}
          </span>
          <span aria-hidden> · </span>
          <button
            type="button"
            className="underline-offset-2 hover:underline"
            onClick={() => setPrivacyOpen(true)}
          >
            Privacy
          </button>
        </div>
        <div>
          Created by Ali Kamil. For questions, reach out at{" "}
          <a
            className="font-medium text-cyan-900 underline-offset-2 hover:underline"
            href="mailto:alikamil@gmail.com"
          >
            alikamil@gmail.com
          </a>
          .
        </div>
      </footer>
      {privacyOpen && <PrivacyDialog onClose={() => setPrivacyOpen(false)} />}
    </div>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <AppShell />
    </AuthProvider>
  );
}
