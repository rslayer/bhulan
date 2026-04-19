import { useState } from "react";
import { Plus, Trash2 } from "lucide-react";

import { HotspotsList } from "@/components/HotspotsList";
import { MapView, trackColor, type MapTrack } from "@/components/MapView";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  compareTracks,
  type CompareResponse,
  type CompareTrackInput,
  type InsightsOptions,
} from "@/lib/api";
import { formatMinutes, formatNumber } from "@/lib/utils";

interface TrackDraft {
  id: number;
  label: string;
  text: string;
}

const DEFAULT_OPTIONS: InsightsOptions = {
  stop_radius_m: 50,
  min_stop_minutes: 5,
  moving_speed_kmh: 3.6,
};

// Keep the client-side limit in sync with the backend's MAX_TRACKS.
const MAX_TRACKS = 20;

let nextId = 0;
function makeDraft(label: string): TrackDraft {
  nextId += 1;
  return { id: nextId, label, text: "" };
}

export function ComparePage() {
  // Start with two drafts so the "compare" UX is obvious on first paint.
  const [drafts, setDrafts] = useState<TrackDraft[]>(() => [
    makeDraft("Track A"),
    makeDraft("Track B"),
  ]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<CompareResponse | null>(null);
  const [options] = useState<InsightsOptions>(DEFAULT_OPTIONS);

  function updateDraft(id: number, patch: Partial<TrackDraft>) {
    setDrafts((prev) => prev.map((d) => (d.id === id ? { ...d, ...patch } : d)));
  }

  function addDraft() {
    setDrafts((prev) => {
      if (prev.length >= MAX_TRACKS) return prev;
      return [...prev, makeDraft(`Track ${String.fromCharCode(65 + prev.length)}`)];
    });
  }

  function removeDraft(id: number) {
    setDrafts((prev) => (prev.length <= 2 ? prev : prev.filter((d) => d.id !== id)));
  }

  async function run() {
    const filled = drafts.filter((d) => d.text.trim().length > 0);
    if (filled.length < 2) {
      setError("Please paste coordinates into at least two tracks.");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const payload: { tracks: CompareTrackInput[]; options?: InsightsOptions } = {
        tracks: filled.map((d) => ({
          label: d.label.trim() || undefined,
          text: d.text,
        })),
        options,
      };
      const res = await compareTracks(payload);
      setResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  const mapTracks: MapTrack[] =
    result?.tracks.map((t, i) => ({
      label: t.label,
      points: t.points,
      stops: t.report.stops,
      color: trackColor(i),
    })) ?? [];

  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-5">
      <div className="flex flex-col gap-4 lg:col-span-2">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle>Tracks to compare</CardTitle>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={addDraft}
              disabled={drafts.length >= MAX_TRACKS}
            >
              <Plus className="mr-1 h-3.5 w-3.5" /> Add
            </Button>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            {drafts.map((d, i) => (
              <div
                key={d.id}
                className="flex flex-col gap-2 rounded-md border border-slate-200 p-3"
              >
                <div className="flex items-center justify-between gap-2">
                  <div className="flex flex-1 items-center gap-2">
                    <span
                      className="inline-block h-3 w-3 rounded-full"
                      style={{ backgroundColor: trackColor(i) }}
                      aria-hidden
                    />
                    <Label className="sr-only" htmlFor={`label-${d.id}`}>
                      Track label
                    </Label>
                    <Input
                      id={`label-${d.id}`}
                      className="h-8 text-sm"
                      placeholder={`Track ${String.fromCharCode(65 + i)}`}
                      value={d.label}
                      onChange={(e) => updateDraft(d.id, { label: e.target.value })}
                    />
                  </div>
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => removeDraft(d.id)}
                    disabled={drafts.length <= 2}
                    aria-label="Remove track"
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
                <textarea
                  className="min-h-[110px] w-full rounded-md border border-slate-200 bg-slate-50 p-2 font-mono text-xs"
                  placeholder={"12.97,77.59,2025-01-01T09:00:00Z\n12.98,77.60,2025-01-01T09:01:00Z"}
                  value={d.text}
                  onChange={(e) => updateDraft(d.id, { text: e.target.value })}
                />
              </div>
            ))}

            {error && (
              <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-900">
                {error}
              </div>
            )}

            <div className="flex justify-end">
              <Button type="button" onClick={run} disabled={loading}>
                {loading ? "Comparing…" : "Compare tracks"}
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="flex flex-col gap-4 lg:col-span-3">
        <Card>
          <CardHeader>
            <CardTitle>Map</CardTitle>
          </CardHeader>
          <CardContent>
            <MapView
              tracks={mapTracks}
              hotspots={result?.shared_hotspots ?? []}
            />
          </CardContent>
        </Card>

        {result && result.tracks.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Side-by-side</CardTitle>
            </CardHeader>
            <CardContent className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs uppercase tracking-wide text-slate-500">
                    <th className="py-2 pr-4">Track</th>
                    <th className="py-2 pr-4">Distance</th>
                    <th className="py-2 pr-4">Moving</th>
                    <th className="py-2 pr-4">Idle</th>
                    <th className="py-2 pr-4">Avg kmh</th>
                    <th className="py-2 pr-4">Max kmh</th>
                    <th className="py-2 pr-4">Stops</th>
                    <th className="py-2 pr-4">Trips</th>
                    <th className="py-2 pr-4">Points</th>
                  </tr>
                </thead>
                <tbody>
                  {result.tracks.map((t, i) => {
                    const s = t.report.summary;
                    return (
                      <tr
                        key={t.label}
                        className="border-t border-slate-200 align-top"
                      >
                        <td className="py-2 pr-4">
                          <div className="flex items-center gap-2 font-medium">
                            <span
                              className="inline-block h-3 w-3 rounded-full"
                              style={{ backgroundColor: trackColor(i) }}
                              aria-hidden
                            />
                            {t.label}
                          </div>
                        </td>
                        <td className="py-2 pr-4">
                          {formatNumber(s.total_distance_km, 2)} km
                        </td>
                        <td className="py-2 pr-4">{formatMinutes(s.moving_time_min)}</td>
                        <td className="py-2 pr-4">{formatMinutes(s.idle_time_min)}</td>
                        <td className="py-2 pr-4">
                          {s.avg_moving_speed_kmh == null
                            ? "—"
                            : formatNumber(s.avg_moving_speed_kmh, 1)}
                        </td>
                        <td className="py-2 pr-4">
                          {s.max_speed_kmh == null
                            ? "—"
                            : formatNumber(s.max_speed_kmh, 1)}
                        </td>
                        <td className="py-2 pr-4">{t.report.stops.length}</td>
                        <td className="py-2 pr-4">{t.report.trips.length}</td>
                        <td className="py-2 pr-4">
                          {s.accepted_point_count} / {s.point_count}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </CardContent>
          </Card>
        )}

        {result && (
          <HotspotsList
            hotspots={result.shared_hotspots}
            title="Shared hotspots"
            emptyNote="No overlapping density regions across tracks."
          />
        )}

        {!result && !loading && (
          <Card>
            <CardContent className="py-10 text-center text-sm text-slate-500">
              Paste 2+ tracks on the left and press{" "}
              <span className="font-medium">Compare tracks</span> to see a
              side-by-side breakdown with an overlayed map and shared hotspots.
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
