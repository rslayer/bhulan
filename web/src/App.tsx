import { useState } from "react";
import { Map, Sparkles } from "lucide-react";
import { InsightsPage } from "@/pages/InsightsPage";
import { PlotPage } from "@/pages/PlotPage";
import { cn } from "@/lib/utils";

type Tab = "insights" | "plot";

function Tabs({ tab, onChange }: { tab: Tab; onChange: (t: Tab) => void }) {
  return (
    <div className="inline-flex items-center rounded-lg border border-slate-200 bg-white p-1 text-sm">
      <button
        className={cn(
          "inline-flex items-center gap-1.5 rounded-md px-3 py-1.5",
          tab === "insights" ? "bg-slate-900 text-white" : "text-slate-700 hover:bg-slate-100",
        )}
        onClick={() => onChange("insights")}
      >
        <Sparkles className="h-4 w-4" />
        Insights
      </button>
      <button
        className={cn(
          "inline-flex items-center gap-1.5 rounded-md px-3 py-1.5",
          tab === "plot" ? "bg-slate-900 text-white" : "text-slate-700 hover:bg-slate-100",
        )}
        onClick={() => onChange("plot")}
      >
        <Map className="h-4 w-4" />
        Plot
      </button>
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
        {tab === "insights" ? <InsightsPage /> : <PlotPage />}
      </main>
      <footer className="mx-auto max-w-6xl px-4 py-8 text-xs text-slate-500">
        All analysis runs against your bhulan backend — your data is never stored.
      </footer>
    </div>
  );
}
