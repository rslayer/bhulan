import { useState } from "react";
import { GitCompareArrows, Map, Sparkles } from "lucide-react";
import { ComparePage } from "@/pages/ComparePage";
import { InsightsPage } from "@/pages/InsightsPage";
import { PlotPage } from "@/pages/PlotPage";
import { cn } from "@/lib/utils";

type Tab = "insights" | "plot" | "compare";

interface TabDef {
  id: Tab;
  label: string;
  icon: React.ReactNode;
}

const TABS: TabDef[] = [
  { id: "insights", label: "Insights", icon: <Sparkles className="h-4 w-4" /> },
  { id: "plot", label: "Plot", icon: <Map className="h-4 w-4" /> },
  { id: "compare", label: "Compare", icon: <GitCompareArrows className="h-4 w-4" /> },
];

function Tabs({ tab, onChange }: { tab: Tab; onChange: (t: Tab) => void }) {
  return (
    <div className="inline-flex items-center rounded-lg border border-slate-200 bg-white p-1 text-sm">
      {TABS.map((t) => (
        <button
          key={t.id}
          className={cn(
            "inline-flex items-center gap-1.5 rounded-md px-3 py-1.5",
            tab === t.id
              ? "bg-slate-900 text-white"
              : "text-slate-700 hover:bg-slate-100",
          )}
          onClick={() => onChange(t.id)}
        >
          {t.icon}
          {t.label}
        </button>
      ))}
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState<Tab>("insights");

  return (
    <div className="min-h-full">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-3 px-4 py-3">
          <div>
            <div className="text-lg font-semibold">Bhulan</div>
            <div className="text-xs text-slate-500">GPS mobility insights &amp; plotting</div>
          </div>
          <Tabs tab={tab} onChange={setTab} />
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-4 py-6">
        {tab === "insights" && <InsightsPage />}
        {tab === "plot" && <PlotPage />}
        {tab === "compare" && <ComparePage />}
      </main>
      <footer className="mx-auto max-w-6xl px-4 py-8 text-xs text-slate-500">
        All analysis runs against your bhulan backend — your data is never stored.
      </footer>
    </div>
  );
}
