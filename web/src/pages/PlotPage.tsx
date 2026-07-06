import { useEffect, useRef, useState } from "react";
import { MapPinned } from "lucide-react";
import { CoordinateInput } from "@/components/CoordinateInput";
import { MapView } from "@/components/MapView";
import { MapLayerToggle, type MapLayerMode } from "@/components/MapLayerToggle";
import { ShareLinkButton } from "@/components/ShareLinkButton";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { validatePlot, type Point } from "@/lib/api";
import { decodeShareFragment } from "@/lib/permalink";

// For very large datasets the per-sample CircleMarker rendering becomes
// the dominant paint cost and the map turns into a blob. Above this
// threshold we default to the heatmap layer instead; the user can still
// flip back via the toggle.
const HEATMAP_AUTO_THRESHOLD = 10000;

export function PlotPage() {
  const [text, setText] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [points, setPoints] = useState<Point[]>([]);
  const [accepted, setAccepted] = useState(0);
  const [rejected, setRejected] = useState(0);
  const [issues, setIssues] = useState<string[]>([]);
  const [layerMode, setLayerMode] = useState<MapLayerMode>("markers");
  const [layerTouched, setLayerTouched] = useState(false);
  const hydratedRef = useRef(false);

  // Hydrate from a share fragment on first mount — see InsightsPage for
  // the full rationale.
  useEffect(() => {
    if (hydratedRef.current) return;
    hydratedRef.current = true;
    const state = decodeShareFragment(window.location.hash);
    if (state && state.tab === "plot") {
      setText(state.text);
    }
  }, []);

  async function run() {
    setLoading(true);
    setError(null);
    try {
      const res = await validatePlot({ text });
      setPoints(res.points);
      setAccepted(res.accepted);
      setRejected(res.rejected);
      setIssues(res.issues);
      // Auto-switch to the appropriate mode for the new dataset size
      // unless the user has already picked a mode explicitly. This is
      // two-directional: submitting a tiny track after a huge one flips
      // back to Markers, otherwise the user would see 50 points as a
      // heatmap they never chose.
      if (!layerTouched) {
        setLayerMode(
          res.points.length >= HEATMAP_AUTO_THRESHOLD ? "heatmap" : "markers",
        );
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setPoints([]);
      setAccepted(0);
      setRejected(0);
      setIssues([]);
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
              onSubmit={run}
              loading={loading}
              submitLabel="Plot on map"
            />
            {error && (
              <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-900">
                {error}
              </div>
            )}
            {(accepted > 0 || rejected > 0) && (
              <div className="rounded-md border border-cyan-100 bg-cyan-50/70 p-3 text-sm text-cyan-950">
                <div>
                  <span className="font-semibold">{accepted}</span> accepted,{" "}
                  <span className="font-semibold">{rejected}</span> rejected
                </div>
                {issues.length > 0 && (
                  <ul className="mt-2 list-disc pl-5 text-xs text-slate-600">
                    {issues.slice(0, 5).map((i, idx) => (
                      <li key={idx}>{i}</li>
                    ))}
                    {issues.length > 5 && <li>…and {issues.length - 5} more</li>}
                  </ul>
                )}
              </div>
            )}
            <div className="flex justify-end">
              <ShareLinkButton state={{ tab: "plot", text }} disabled={!text.trim()} />
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="lg:col-span-3">
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
              layers={
                layerMode === "heatmap"
                  ? ["heatmap"]
                  : layerMode === "both"
                    ? ["lines", "markers", "heatmap"]
                    : ["lines", "markers"]
              }
            />
            {points.length >= HEATMAP_AUTO_THRESHOLD && layerMode !== "heatmap" && (
              <div className="mt-2 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
                {points.length.toLocaleString()} points — switch to heatmap for
                faster rendering.
              </div>
            )}
          </CardContent>
        </Card>
        {points.length === 0 && !loading && (
          <Card className="mt-4 border-dashed border-slate-300 bg-white/75">
            <CardContent className="py-8 text-center text-sm text-slate-600">
              <MapPinned className="mx-auto mb-3 h-7 w-7 text-cyan-900" />
              Plot a track to inspect route shape, sample density, and coordinate quality.
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
