import { useState } from "react";
import { CoordinateInput } from "@/components/CoordinateInput";
import { InsightsPanel } from "@/components/InsightsPanel";
import { MapView } from "@/components/MapView";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { computeInsights, validatePlot, type InsightsReport, type Point } from "@/lib/api";

interface Options {
  stop_radius_m: number;
  min_stop_minutes: number;
  moving_speed_kmh: number;
}

const DEFAULT_OPTIONS: Options = {
  stop_radius_m: 50,
  min_stop_minutes: 5,
  moving_speed_kmh: 3.6,
};

export function InsightsPage() {
  const [text, setText] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<InsightsReport | null>(null);
  const [points, setPoints] = useState<Point[]>([]);
  const [options, setOptions] = useState<Options>(DEFAULT_OPTIONS);
  const [showOptions, setShowOptions] = useState(false);

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
            )}

            {error && (
              <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-900">
                {error}
              </div>
            )}
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
