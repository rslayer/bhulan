import { useEffect, useMemo, useRef, useState } from "react";
import {
  Activity,
  BookOpen,
  Crosshair,
  LocateFixed,
  MapPin,
  MousePointer2,
  Route,
} from "lucide-react";
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
import {
  computeInsights,
  validatePlot,
  type InsightsReport,
  type PlotResponse,
  type Point,
} from "@/lib/api";
import { decodeShareFragment } from "@/lib/permalink";
import { formatNumber } from "@/lib/utils";

interface Options {
  stop_radius_m: number;
  min_stop_minutes: number;
  moving_speed_kmh: number;
  geocode_stops: boolean;
  hotspot_grid_m?: number;
}

interface RadiusCenter {
  lat: number;
  lon: number;
}

const DEFAULT_OPTIONS: Options = {
  stop_radius_m: 50,
  min_stop_minutes: 5,
  moving_speed_kmh: 3.6,
  geocode_stops: false,
};

const HEATMAP_AUTO_THRESHOLD = 10000;
const PLOT_DEBOUNCE_MS = 450;

function haversineMeters(a: RadiusCenter, b: RadiusCenter): number {
  const toRad = (v: number) => (v * Math.PI) / 180;
  const earthM = 6371000;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lon - a.lon);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * earthM * Math.asin(Math.sqrt(h));
}

function formatCoord(n: number): string {
  if (!Number.isFinite(n)) return "";
  return n.toFixed(6);
}

function radiusStats(points: Point[], center: RadiusCenter | null, radiusM: number) {
  if (!center || points.length === 0 || radiusM <= 0) {
    return { inside: 0, percent: 0, startInside: false, endInside: false };
  }
  const inside = points.filter(
    (p) => haversineMeters(center, { lat: p.lat, lon: p.lon }) <= radiusM,
  ).length;
  const first = points[0];
  const last = points[points.length - 1];
  return {
    inside,
    percent: points.length === 0 ? 0 : (inside / points.length) * 100,
    startInside: haversineMeters(center, { lat: first.lat, lon: first.lon }) <= radiusM,
    endInside: haversineMeters(center, { lat: last.lat, lon: last.lon }) <= radiusM,
  };
}

export function InsightsPage() {
  const { user, capabilities } = useAuth();
  const [text, setText] = useState("");
  const [loading, setLoading] = useState(false);
  const [plotLoading, setPlotLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [plotError, setPlotError] = useState<string | null>(null);
  const [report, setReport] = useState<InsightsReport | null>(null);
  const [plotSummary, setPlotSummary] = useState<PlotResponse | null>(null);
  const [points, setPoints] = useState<Point[]>([]);
  const [options, setOptions] = useState<Options>(DEFAULT_OPTIONS);
  const [showOptions, setShowOptions] = useState(false);
  const [showGuide, setShowGuide] = useState(true);
  const [layerMode, setLayerMode] = useState<MapLayerMode>("markers");
  const [radiusM, setRadiusM] = useState(500);
  const [customCenter, setCustomCenter] = useState<RadiusCenter | null>(null);
  const hydratedRef = useRef(false);
  const plotRequestRef = useRef(0);
  const layerTouchedRef = useRef(false);

  const firstPointCenter = points.length > 0 ? { lat: points[0].lat, lon: points[0].lon } : null;
  const radiusCenter = customCenter ?? firstPointCenter;
  const stats = useMemo(
    () => radiusStats(points, radiusCenter, radiusM),
    [points, radiusCenter, radiusM],
  );

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
        // Malformed payload: ignore it.
      } finally {
        localStorage.removeItem("bhulan.replay");
      }
    }

    consumeReplay();
    window.addEventListener("bhulan:replay", consumeReplay);
    return () => window.removeEventListener("bhulan:replay", consumeReplay);
  }, []);

  useEffect(() => {
    const inputText = text.trim();
    const requestId = ++plotRequestRef.current;

    setReport(null);
    setError(null);

    if (!inputText) {
      setPoints([]);
      setPlotSummary(null);
      setPlotError(null);
      setPlotLoading(false);
      setCustomCenter(null);
      return;
    }

    setPlotLoading(true);
    setPlotError(null);

    const timeout = window.setTimeout(() => {
      void validatePlot({ text: inputText })
        .then((plot) => {
          if (requestId !== plotRequestRef.current) return;
          setPlotSummary(plot);
          setPoints(plot.points);
          if (!layerTouchedRef.current) {
            setLayerMode(
              plot.points.length >= HEATMAP_AUTO_THRESHOLD ? "heatmap" : "markers",
            );
          }
        })
        .catch((e) => {
          if (requestId !== plotRequestRef.current) return;
          setPoints([]);
          setPlotSummary(null);
          setPlotError(e instanceof Error ? e.message : String(e));
        })
        .finally(() => {
          if (requestId === plotRequestRef.current) setPlotLoading(false);
        });
    }, PLOT_DEBOUNCE_MS);

    return () => window.clearTimeout(timeout);
  }, [text]);

  async function run(override?: string) {
    const inputText = (override ?? text).trim();
    if (!inputText) return;
    setLoading(true);
    setError(null);
    try {
      const [rep, plot] = await Promise.all([
        computeInsights({ text: inputText, options }),
        validatePlot({ text: inputText }),
      ]);
      setReport(rep);
      setPlotSummary(plot);
      setPoints(plot.points);
      if (!layerTouchedRef.current) {
        setLayerMode(
          plot.points.length >= HEATMAP_AUTO_THRESHOLD ? "heatmap" : "markers",
        );
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setReport(null);
    } finally {
      setLoading(false);
    }
  }

  function updateText(next: string) {
    setText(next);
  }

  function updateCenter(patch: Partial<RadiusCenter>) {
    const base = radiusCenter ?? { lat: 0, lon: 0 };
    setCustomCenter({ ...base, ...patch });
  }

  const hasText = text.trim().length > 0;
  const hasPoints = points.length > 0;

  return (
    <div className="flex flex-col gap-5">
      {showGuide && (
        <Card className="border-cyan-100 bg-cyan-50/80">
          <CardContent className="flex flex-col gap-3 py-4 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex items-start gap-3">
              <div className="mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-white text-cyan-900 shadow-sm">
                <BookOpen className="h-5 w-5" />
              </div>
              <div>
                <div className="font-medium text-cyan-950">Quick guide</div>
                <div className="text-sm text-cyan-900">
                  Paste coordinates or try a sample, inspect the map immediately, then
                  analyze stops and trips. Use the radius tool to check how much of the
                  track falls near a starting location.
                </div>
              </div>
            </div>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="self-start text-cyan-950 sm:self-center"
              onClick={() => setShowGuide(false)}
            >
              Hide guide
            </Button>
          </CardContent>
        </Card>
      )}

      <div className="grid grid-cols-1 gap-5 xl:grid-cols-[minmax(360px,420px)_1fr]">
        <div className="flex flex-col gap-4">
          <Card>
            <CardHeader>
              <CardTitle>Paste GPS Data</CardTitle>
              <p className="text-sm text-slate-500">
                The map updates automatically as soon as Bhulan can parse your points.
              </p>
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
              <CoordinateInput
                value={text}
                onChange={updateText}
                onSubmit={() => void run()}
                loading={loading}
                submitLabel="Analyze track"
              />

              {!user && (
                <div className="rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-900">
                  {capabilities.history_enabled
                    ? "Anonymous runs aren\u2019t stored. Sign in to save your history."
                    : "Public demo mode: coordinates are processed in memory and aren\u2019t stored."}
                </div>
              )}

              {plotSummary && (
                <div className="grid grid-cols-3 gap-2 text-xs">
                  <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
                    <div className="text-slate-500">Accepted</div>
                    <div className="text-base font-semibold text-slate-950">
                      {plotSummary.accepted.toLocaleString()}
                    </div>
                  </div>
                  <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
                    <div className="text-slate-500">Rejected</div>
                    <div className="text-base font-semibold text-slate-950">
                      {plotSummary.rejected.toLocaleString()}
                    </div>
                  </div>
                  <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
                    <div className="text-slate-500">Status</div>
                    <div className="text-base font-semibold text-slate-950">
                      {plotLoading ? "Parsing" : hasPoints ? "Mapped" : "Ready"}
                    </div>
                  </div>
                </div>
              )}

              {plotError && hasText && (
                <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                  {plotError}
                </div>
              )}

              {error && (
                <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-900">
                  {error}
                </div>
              )}

              <div className="flex flex-wrap items-center justify-between gap-2">
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={() => setShowOptions((v) => !v)}
                >
                  {showOptions ? "Hide" : "Show"} advanced options
                </Button>
                <ShareLinkButton
                  state={{
                    tab: "insights",
                    text,
                    options: options as unknown as Record<string, unknown>,
                  }}
                  disabled={!text.trim()}
                />
              </div>

              {showOptions && (
                <div className="flex flex-col gap-3 rounded-lg border border-slate-200 bg-slate-50/70 p-3">
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-3 xl:grid-cols-1">
                    <div>
                      <Label>Stop radius (m)</Label>
                      <Input
                        type="number"
                        min={5}
                        step={5}
                        value={options.stop_radius_m}
                        onChange={(e) =>
                          setOptions((o) => ({
                            ...o,
                            stop_radius_m: Number(e.target.value) || 0,
                          }))
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
                          setOptions((o) => ({
                            ...o,
                            min_stop_minutes: Number(e.target.value) || 0,
                          }))
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
                          setOptions((o) => ({
                            ...o,
                            moving_speed_kmh: Number(e.target.value) || 0,
                          }))
                        }
                      />
                    </div>
                  </div>
                  {capabilities.reverse_geocoding_enabled && (
                    <label className="flex cursor-pointer items-start gap-2 rounded-md border border-slate-200 bg-white p-3 text-sm">
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
                          Reverse-geocodes each stop via OpenStreetMap.
                        </div>
                      </div>
                    </label>
                  )}
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Crosshair className="h-4 w-4 text-cyan-900" />
                Radius Check
              </CardTitle>
              <p className="text-sm text-slate-500">
                Defaults to the first point. Type a center or click the map to move it.
              </p>
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <Label>Center lat</Label>
                  <Input
                    type="number"
                    step="0.000001"
                    value={radiusCenter ? formatCoord(radiusCenter.lat) : ""}
                    onChange={(e) => updateCenter({ lat: Number(e.target.value) || 0 })}
                    disabled={!hasPoints}
                  />
                </div>
                <div>
                  <Label>Center lon</Label>
                  <Input
                    type="number"
                    step="0.000001"
                    value={radiusCenter ? formatCoord(radiusCenter.lon) : ""}
                    onChange={(e) => updateCenter({ lon: Number(e.target.value) || 0 })}
                    disabled={!hasPoints}
                  />
                </div>
              </div>
              <div>
                <div className="flex items-center justify-between gap-2">
                  <Label>Radius</Label>
                  <span className="text-xs font-medium text-slate-600">
                    {radiusM.toLocaleString()} m
                  </span>
                </div>
                <input
                  type="range"
                  min={50}
                  max={5000}
                  step={50}
                  value={radiusM}
                  disabled={!hasPoints}
                  className="mt-2 w-full accent-cyan-900"
                  onChange={(e) => setRadiusM(Number(e.target.value))}
                />
              </div>

              <div className="grid grid-cols-2 gap-2 text-xs">
                <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
                  <div className="text-slate-500">Points inside</div>
                  <div className="text-base font-semibold text-slate-950">
                    {stats.inside.toLocaleString()}
                  </div>
                </div>
                <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
                  <div className="text-slate-500">Track share</div>
                  <div className="text-base font-semibold text-slate-950">
                    {formatNumber(stats.percent, 1)}%
                  </div>
                </div>
                <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
                  <div className="text-slate-500">Start</div>
                  <div className="text-base font-semibold text-slate-950">
                    {stats.startInside ? "Inside" : "Outside"}
                  </div>
                </div>
                <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
                  <div className="text-slate-500">End</div>
                  <div className="text-base font-semibold text-slate-950">
                    {stats.endInside ? "Inside" : "Outside"}
                  </div>
                </div>
              </div>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setCustomCenter(null)}
                disabled={!hasPoints || !customCenter}
              >
                <LocateFixed className="h-3.5 w-3.5" />
                Reset to first point
              </Button>
            </CardContent>
          </Card>
        </div>

        <div className="flex flex-col gap-4">
          <Card className="overflow-hidden">
            <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0">
              <div>
                <CardTitle>Live Map</CardTitle>
                <p className="mt-1 text-sm text-slate-500">
                  {hasPoints
                    ? `${points.length.toLocaleString()} parsed points. Click the map to place the radius center.`
                    : "Paste coordinates to plot a route here."}
                </p>
              </div>
              <MapLayerToggle
                value={layerMode}
                onChange={(m) => {
                  setLayerMode(m);
                  layerTouchedRef.current = true;
                }}
                disabled={points.length === 0}
              />
            </CardHeader>
            <CardContent>
              <MapView
                points={points}
                stops={report?.stops ?? []}
                radiusOverlay={
                  radiusCenter
                    ? {
                        center: [radiusCenter.lat, radiusCenter.lon],
                        radiusM,
                        label: customCenter ? "Selected center" : "First track point",
                      }
                    : null
                }
                onPickCenter={
                  hasPoints
                    ? (lat, lon) => setCustomCenter({ lat, lon })
                    : undefined
                }
                layers={
                  layerMode === "heatmap"
                    ? ["heatmap"]
                    : layerMode === "both"
                      ? ["lines", "markers", "heatmap"]
                      : ["lines", "markers"]
                }
                className="h-[420px] w-full overflow-hidden rounded-lg border border-slate-200 bg-slate-100 shadow-inner lg:h-[640px]"
              />
            </CardContent>
          </Card>

          {!hasPoints && !plotLoading && (
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
                    Paste GPS coordinates or try a sample. The map will draw the track
                    first, then <span className="font-medium text-slate-900">Analyze track</span>{" "}
                    will compute stops, trips, and hotspots.
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {hasPoints && !report && (
            <Card className="border-cyan-100 bg-cyan-50/70">
              <CardContent className="flex flex-col gap-3 py-4 sm:flex-row sm:items-center sm:justify-between">
                <div className="flex items-start gap-3">
                  <MousePointer2 className="mt-0.5 h-5 w-5 text-cyan-900" />
                  <div className="text-sm text-cyan-950">
                    The route is mapped. Adjust the radius, click the map for a custom center,
                    or analyze the track for stops and trips.
                  </div>
                </div>
                <Button type="button" onClick={() => void run()} disabled={loading}>
                  {loading ? "Analyzing..." : "Analyze track"}
                </Button>
              </CardContent>
            </Card>
          )}

          {report && (
            <InsightsPanel report={report} hotspotGridM={options.hotspot_grid_m} />
          )}
        </div>
      </div>
    </div>
  );
}
