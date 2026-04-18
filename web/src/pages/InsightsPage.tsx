import { useEffect, useRef, useState } from "react";
import { CoordinateInput } from "@/components/CoordinateInput";
import { InsightsPanel } from "@/components/InsightsPanel";
import { MapView } from "@/components/MapView";
import { ShareLinkButton } from "@/components/ShareLinkButton";
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
}

const DEFAULT_OPTIONS: Options = {
  stop_radius_m: 50,
  min_stop_minutes: 5,
  moving_speed_kmh: 3.6,
  geocode_stops: false,
};

export function InsightsPage() {
  const [text, setText] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<InsightsReport | null>(null);
  const [points, setPoints] = useState<Point[]>([]);
  const [options, setOptions] = useState<Options>(DEFAULT_OPTIONS);
  const [showOptions, setShowOptions] = useState(false);
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

  async function run() {
    setLoading(true);
    setError(null);
    try {
      // Compute insights + fetch cleaned points for the map in parallel.
      const [rep, plot] = await Promise.all([
        computeInsights({ text, options }),
        validatePlot({ text }),
      ]);
      setReport(rep);
      setPoints(plot.points);
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
              onSubmit={run}
              loading={loading}
              submitLabel="Compute insights"
            />

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
          <CardHeader>
            <CardTitle>Map</CardTitle>
          </CardHeader>
          <CardContent>
            <MapView points={points} stops={report?.stops ?? []} />
          </CardContent>
        </Card>
        {report ? (
          <InsightsPanel report={report} />
        ) : (
          <Card>
            <CardContent className="py-10 text-center text-sm text-slate-500">
              Paste GPS coordinates on the left and press{" "}
              <span className="font-medium">Compute insights</span> to see stops, distance, speeds,
              and moving vs. idle time.
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
