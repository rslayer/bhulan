import { useEffect, useState } from "react";
import { GitCompareArrows, History, Map, Sparkles } from "lucide-react";
import { AuthProvider, useAuth } from "@/components/AuthProvider";
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

function Tabs({ tab, onChange }: { tab: Tab; onChange: (t: Tab) => void }) {
  return (
    <div
      role="tablist"
      className="inline-flex items-center rounded-lg border border-slate-200 bg-white p-1 text-sm"
    >
      {TABS.map((t) => (
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
              ? "bg-slate-900 text-white"
              : "text-slate-700 hover:bg-slate-100",
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
  const { user } = useAuth();

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
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-3 px-4 py-3">
          <div>
            <div className="text-lg font-semibold">Bhulan</div>
            <div className="text-xs text-slate-500">GPS mobility insights &amp; plotting</div>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <Tabs tab={tab} onChange={setTab} />
            <UserMenu />
          </div>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-4 py-6">
        {tab === "insights" && <InsightsPage />}
        {tab === "plot" && <PlotPage />}
        {tab === "compare" && <ComparePage />}
        {tab === "history" && <HistoryPage />}
      </main>
      <footer className="mx-auto max-w-6xl px-4 py-8 text-xs text-slate-500">
        {user
          ? "Signed in — your insights runs are saved to your history."
          : "All analysis runs against your bhulan backend — sign in to save your history."}
      </footer>
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
