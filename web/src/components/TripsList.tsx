import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { TripOut } from "@/lib/api";
import { formatMinutes, formatNumber } from "@/lib/utils";
import { Route } from "lucide-react";

interface Props {
  trips: TripOut[];
}

function fmtTs(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

/**
 * Pretty-print a list of detected trips. Each row shows the per-trip
 * distance / duration / moving-idle split so the user can see "you did
 * 3 trips today, this one was the longest" at a glance. Start + end
 * coordinates are also shown so an export-to-map flow has everything
 * it needs.
 */
export function TripsList({ trips }: Props) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Trips ({trips.length})</CardTitle>
      </CardHeader>
      <CardContent>
        {trips.length === 0 ? (
          <div className="text-sm text-slate-500">No trips detected.</div>
        ) : (
          <div className="divide-y divide-slate-200">
            {trips.map((t) => (
              <div key={t.index} className="flex items-start justify-between gap-3 py-2 text-sm">
                <div className="min-w-0">
                  <div className="flex items-center gap-1.5 font-medium">
                    <Route className="h-3.5 w-3.5 text-slate-500" />
                    Trip #{t.index + 1}
                  </div>
                  <div className="text-xs text-slate-500">
                    {fmtTs(t.start_ts)} &rarr; {fmtTs(t.end_ts)}
                  </div>
                  <div className="text-xs text-slate-500">
                    {t.start_lat.toFixed(4)}, {t.start_lon.toFixed(4)}
                    {" → "}
                    {t.end_lat.toFixed(4)}, {t.end_lon.toFixed(4)}
                  </div>
                </div>
                <div className="text-right text-sm">
                  <div className="font-semibold">
                    {formatNumber(t.distance_km, 2)} km
                  </div>
                  <div className="text-xs text-slate-500">
                    {formatMinutes(t.duration_min)} ({formatMinutes(t.moving_time_min)} moving)
                  </div>
                  <div className="text-xs text-slate-500">
                    max {formatNumber(t.max_speed_kmh, 1)} km/h · {t.sample_count} pts
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
