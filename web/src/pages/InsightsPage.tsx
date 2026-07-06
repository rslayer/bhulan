import { useEffect, useRef, useState } from "react";
import { Activity, MapPin, Route } from "lucide-react";
import { CoordinateInput } from "@/components/CoordinateInput";
import { InsightsPanel } from "@/components/InsightsPanel";
import { MapView } from "@/components/MapView";
import { MapLayerToggle, type MapLayerMode } from "@/components/MapLayerToggle";
import { ShareLinkButton } from "@/components/ShareLinkButton";
import { useAuth } from "@/components/AuthProvider";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { computeInsights, validatePlot, type InsightsReport, type Point } from "@/lib/api";
import { decodeShareFragment } from "@/lib/permalink";

interface Options {
  stop_radius_m: number;
  min_stop_minutes: number;
  moving_speed_kmh: number;
  geocode_stops: boolean;
  /** Optional \u2014 only ever set via a share URL today since the UI
   * doesn't expose it. Threaded to ``HotspotsList`` so the \"grid ~N m\"
   * label matches the backend's actual grid. */
  hotspot_grid_m?: number;
}

const DEFAULT_OPTIONS: Options = {
  stop_radius_m: 50,
  min_stop_minutes: 5,
  moving_speed_kmh: 3.6,
  geocode_stops: false,
};

// See PlotPage for the rationale — above this threshold the per-sample
// CircleMarker paint dominates and the map gets unusable.
const HEATMAP_AUTO_THRESHOLD = 10000;

export function InsightsPage() {
  const { user, capabilities } = useAuth();
  const [text, setText] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<InsightsReport | null>(null);
  const [points, setPoints] = useState<Point[]>([]);
  const [options, setOptions] = useState<Options>(DEFAULT_OPTIONS);
  const [showOptions, setShowOptions] = useState(false);
  const [layerMode, setLayerMode] = useState<MapLayerMode>("markers");
  const [layerTouched, setLayerTouched] = useState(false);
  const hydratedRef = useRef(false);

  // Hydrate from a share fragment (``#s=v1.…``) on first mount. Only
  // fires once; subsequent edits to the textarea / options shouldn't be
  // clobbered if the user navigates back with a stale hash.
  useEffect(() => {
    if (hydratedRef.current) return;
    hydratedRef.current = true;
    const state = decodeShareFragment(window.location.hash);
    if (state && state.tab === "insights") {
      setText(state.text);
      if (state.options) {
        setOptions((prev) => ({ ...prev, ...(state.options as Partial<Options>) }));
        setShowOptions(true);
      }
    }
  }, []);

  // Replay from the History tab. HistoryPage stashes the stored /v1/insights
  // request body in localStorage("bhulan.replay") and fires a
  // "bhulan:replay" event; we consume that payload, rehydrate the form,
  // then clear the localStorage key so future mounts don't re-replay.
  useEffect(() => {
    function consumeReplay() {
      const raw = localStorage.getItem("bhulan.replay");
      if (!raw) return;
      try {
        const parsed = JSON.parse(raw) as {
          request?: { text?: string; points?: Point[]; options?: Partial<Options> };
        };
        const req = parsed?.request;
        if (!req) return;
        // Prefer raw text for display; fall back to re-serializing the
        // structured points so the user sees what they're replaying.
        if (typeof req.text === "string" && req.text.length > 0) {
          setText(req.text);
        } else if (Array.isArray(req.points) && req.points.length > 0) {
          setText(
              req.points
                .map((p) =>
                  p.ts_utc ? `${p.lat},${p.lon},${p.ts_utc}` : `${p.lat},${p.lon}`,
                )
                .join("\n"),
          );
        }
        if (req.options) {
          setOptions((prev) => ({ ...prev, ...(req.options as Partial<Options>) }));
          setShowOptions(true);
        }
        setReport(null);
        setPoints([]);
        setError(null);
      } catch {
        // Malformed payload \u2014 nothing to do.
      } finally {
        localStorage.removeItem("bhulan.replay");
      }
    }

    // Run once on mount in case we mount *after* the event was dispatched
    // (e.g. the user was on another tab), then subscribe for live events.
    consumeReplay();
    window.addEventListener("bhulan:replay", consumeReplay);
    return () => window.removeEventListener("bhulan:replay", consumeReplay);
  }, []);

  // Accepts an optional ``override`` so the preset "Try a sample"
  // buttons can compute against the freshly-loaded text without
  // waiting a render for ``setText`` to settle.
  async function run(override?: string) {
    const inputText = override ?? text;
    setLoading(true);
    setError(null);
    try {
      // Compute insights + fetch cleaned points for the map in parallel.
      const [rep, plot] = await Promise.all([
        computeInsights({ text: inputText, options }),
        validatePlot({ text: inputText }),
      ]);
      setReport(rep);
      setPoints(plot.points);
      // Two-directional auto-switch — otherwise submitting a small
      // track after a huge one leaves the user stuck on heatmap.
      if (!layerTouched) {
        setLayerMode(
          plot.points.length >= HEATMAP_AUTO_THRESHOLD ? "heatmap" : "markers",
        );
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setReport(null);
      setPoints([]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-5">
      <div className="lg:col-span-2">
        <Card>
          <CardHeader>
            <CardTitle>Input</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            <CoordinateInput
              value={text}
              onChange={setText}
              onSubmit={() => void run()}
              loading={loading}
              submitLabel="Compute insights"
              onLoadSample={(s) => void run(s.text)}
            />

            {!user && (
              <div className="rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-900">
                {capabilities.history_enabled
                  ? "Anonymous runs aren\u2019t stored. Sign in to save your history."
                  : "Public demo mode: coordinates are processed in memory and aren\u2019t stored."}
              </div>
            )}

            <div>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => setShowOptions((v) => !v)}
              >
                {showOptions ? "Hide" : "Show"} advanced options
              </Button>
            </div>

            {showOptions && (
              <div className="flex flex-col gap-3">
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                  <div>
                    <Label>Stop radius (m)</Label>
                    <Input
                      type="number"
                      min={5}
                      step={5}
                      value={options.stop_radius_m}
                      onChange={(e) =>
                        setOptions((o) => ({ ...o, stop_radius_m: Number(e.target.value) || 0 }))
                      }
                    />
                  </div>
                  <div>
                    <Label>Min stop (min)</Label>
                    <Input
                      type="number"
                      min={0.5}
                      step={0.5}
                      value={options.min_stop_minutes}
                      onChange={(e) =>
                        setOptions((o) => ({ ...o, min_stop_minutes: Number(e.target.value) || 0 }))
                      }
                    />
                  </div>
                  <div>
                    <Label>Moving threshold (km/h)</Label>
                    <Input
                      type="number"
                      min={0}
                      step={0.5}
                      value={options.moving_speed_kmh}
                      onChange={(e) =>
                        setOptions((o) => ({ ...o, moving_speed_kmh: Number(e.target.value) || 0 }))
                      }
                    />
                  </div>
                </div>
                {capabilities.reverse_geocoding_enabled && (
                  <label className="flex cursor-pointer items-start gap-2 rounded-md border border-slate-200 bg-slate-50 p-3 text-sm">
                    <input
                      type="checkbox"
                      className="mt-0.5 h-4 w-4"
                      checked={options.geocode_stops}
                      onChange={(e) =>
                        setOptions((o) => ({ ...o, geocode_stops: e.target.checked }))
                      }
                    />
                    <div>
                      <div className="font-medium text-slate-900">
                        Resolve place names for stops
                      </div>
                      <div className="text-xs text-slate-500">
                        Reverse-geocodes each stop via OpenStreetMap (Nominatim). Adds
                        up to 1s per unique stop and requires network access.
                      </div>
                    </div>
                  </label>
                )}
              </div>
            )}

            {error && (
              <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-900">
                {error}
              </div>
            )}

            <div className="flex justify-end">
              <ShareLinkButton
                state={{
                  tab: "insights",
                  text,
                  options: options as unknown as Record<string, unknown>,
                }}
                disabled={!text.trim()}
              />
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="flex flex-col gap-4 lg:col-span-3">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0">
            <CardTitle>Map</CardTitle>
            <MapLayerToggle
              value={layerMode}
              onChange={(m) => {
                setLayerMode(m);
                setLayerTouched(true);
              }}
              disabled={points.length === 0}
            />
          </CardHeader>
          <CardContent>
            <MapView
              points={points}
              stops={report?.stops ?? []}
              layers={
                layerMode === "heatmap"
                  ? ["heatmap"]
                  : layerMode === "both"
                    ? ["lines", "markers", "heatmap"]
                    : ["lines", "markers"]
              }
            />
          </CardContent>
        </Card>
        {report ? (
          <InsightsPanel report={report} hotspotGridM={options.hotspot_grid_m} />
        ) : (
          <Card className="border-dashed border-slate-300 bg-white/75">
            <CardContent className="py-10">
              <div className="mx-auto flex max-w-md flex-col items-center gap-4 text-center">
                <div className="grid grid-cols-3 gap-2 text-cyan-900">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-cyan-50">
                    <MapPin className="h-5 w-5" />
                  </div>
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-emerald-50 text-emerald-900">
                    <Route className="h-5 w-5" />
                  </div>
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-amber-50 text-amber-800">
                    <Activity className="h-5 w-5" />
                  </div>
                </div>
                <div className="text-sm text-slate-600">
                  Paste GPS coordinates on the left and press{" "}
                  <span className="font-medium text-slate-900">Compute insights</span> to see stops,
                  distance, speeds, and moving vs. idle time.
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
