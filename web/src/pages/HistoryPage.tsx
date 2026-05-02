import { useCallback, useEffect, useState } from "react";
import { History as HistoryIcon, RotateCw, Trash2 } from "lucide-react";
import { LoadingSkeleton } from "@/components/LoadingSkeleton";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { useAuth } from "@/components/AuthProvider";
import {
  deleteHistoryEntry,
  getHistoryEntry,
  listHistory,
  type HistorySummaryEntry,
} from "@/lib/api";

function formatWhen(created_at: number): string {
  return new Date(created_at * 1000).toLocaleString();
}

export function HistoryPage() {
  const { user, loading: authLoading } = useAuth();
  const [entries, setEntries] = useState<HistorySummaryEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<number | null>(null);

  const refresh = useCallback(async () => {
    if (!user) return;
    setLoading(true);
    setError(null);
    try {
      const resp = await listHistory(50);
      setEntries(resp.entries);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [user]);

  useEffect(() => {
    if (!user) {
      setEntries([]);
      return;
    }
    void refresh();
  }, [user, refresh]);

  if (authLoading) {
    return (
      <Card>
        <CardContent className="py-6">
          <LoadingSkeleton lines={4} />
        </CardContent>
      </Card>
    );
  }

  if (!user) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <HistoryIcon className="h-4 w-4" />
            Your history
          </CardTitle>
          <CardDescription>
            Sign in to save your insights runs across visits. History is
            stored server-side and is only ever visible to you.
          </CardDescription>
        </CardHeader>
      </Card>
    );
  }

  async function onDelete(id: number) {
    setBusyId(id);
    try {
      await deleteHistoryEntry(id);
      setEntries((xs) => xs.filter((x) => x.id !== id));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusyId(null);
    }
  }

  async function onReplay(id: number) {
    setBusyId(id);
    try {
      const entry = await getHistoryEntry(id);
      if (!entry.request) {
        setError("This entry was stored without a replayable payload.");
        return;
      }
      // Hand off to the Insights page via localStorage + event. Cheap,
      // avoids wiring a new route or a cross-tab context.
      localStorage.setItem(
        "bhulan.replay",
        JSON.stringify({ at: Date.now(), request: entry.request }),
      );
      window.dispatchEvent(new CustomEvent("bhulan:replay"));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div className="flex flex-col gap-4">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0">
          <div>
            <CardTitle className="flex items-center gap-2">
              <HistoryIcon className="h-4 w-4" />
              Your history
            </CardTitle>
            <CardDescription>
              The latest {entries.length} runs you&rsquo;ve made against
              /v1/insights while signed in.
            </CardDescription>
          </div>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => void refresh()}
            disabled={loading}
          >
            <RotateCw
              className={`mr-1 h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`}
            />
            Refresh
          </Button>
        </CardHeader>
        <CardContent>
          {error && (
            <div className="mb-3 rounded-md border border-rose-200 bg-rose-50 px-2 py-1.5 text-xs text-rose-800">
              {error}
            </div>
          )}
          {loading && entries.length === 0 && <LoadingSkeleton lines={5} />}
          {entries.length === 0 && !loading && (
            <div className="text-sm text-slate-500">
              No runs yet. Compute insights on the Insights tab and they&rsquo;ll
              show up here automatically.
            </div>
          )}
          <ul className="divide-y divide-slate-100">
            {entries.map((e) => {
              const s = e.summary?.summary;
              return (
                <li
                  key={e.id}
                  className="flex flex-col gap-1 py-2 sm:flex-row sm:items-center sm:justify-between"
                >
                  <div className="min-w-0">
                    <div className="text-sm font-medium">
                      {e.label || `Run #${e.id}`}
                    </div>
                    <div className="text-xs text-slate-500">
                      {formatWhen(e.created_at)} · kind: {e.kind}
                    </div>
                    <div className="text-xs text-slate-600">
                      {s
                        ? `${s.accepted_point_count ?? 0} pts · ` +
                          `${Number(s.total_distance_km ?? 0).toFixed(2)} km · ` +
                          `${e.summary.stop_count ?? 0} stops · ` +
                          `${e.summary.trip_count ?? 0} trips · ` +
                          `${e.summary.hotspot_count ?? 0} hotspots`
                        : "summary unavailable"}
                    </div>
                  </div>
                  <div className="flex shrink-0 gap-1">
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={busyId === e.id}
                      onClick={() => void onReplay(e.id)}
                    >
                      Replay
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={busyId === e.id}
                      onClick={() => void onDelete(e.id)}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                </li>
              );
            })}
          </ul>
        </CardContent>
      </Card>
    </div>
  );
}
