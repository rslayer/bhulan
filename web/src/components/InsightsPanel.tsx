import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { HotspotsList } from "@/components/HotspotsList";
import { TripsList } from "@/components/TripsList";
import type { InsightsReport } from "@/lib/api";
import { formatMinutes, formatNumber } from "@/lib/utils";
import {
  Activity,
  CircleSlash,
  Gauge,
  MapPin,
  Route,
  Timer,
  TrendingUp,
} from "lucide-react";

interface Props {
  report: InsightsReport;
  /** Pass-through to HotspotsList so the "grid ~N m" label reflects the
   * actual setting when the caller tuned ``hotspot_grid_m``. */
  hotspotGridM?: number;
}

interface StatProps {
  label: string;
  value: string;
  icon: React.ReactNode;
  hint?: string;
}

function Stat({ label, value, icon, hint }: StatProps) {
  return (
    <div className="flex items-start gap-3 rounded-lg border border-slate-200 bg-white p-3">
      <div className="mt-0.5 flex h-8 w-8 items-center justify-center rounded-md bg-slate-100 text-slate-700">
        {icon}
      </div>
      <div className="min-w-0">
        <div className="text-xs uppercase tracking-wide text-slate-500">{label}</div>
        <div className="text-lg font-semibold leading-tight">{value}</div>
        {hint && <div className="text-xs text-slate-500">{hint}</div>}
      </div>
    </div>
  );
}

function fmtTs(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

export function InsightsPanel({ report, hotspotGridM }: Props) {
  const s = report.summary;

  return (
    <div className="flex flex-col gap-4">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3">
        <Stat
          label="Distance"
          value={`${formatNumber(s.total_distance_km, 2)} km`}
          icon={<Route className="h-4 w-4" />}
        />
        <Stat
          label="Moving time"
          value={formatMinutes(s.moving_time_min)}
          icon={<Activity className="h-4 w-4" />}
        />
        <Stat
          label="Idle time"
          value={formatMinutes(s.idle_time_min)}
          icon={<CircleSlash className="h-4 w-4" />}
        />
        <Stat
          label="Avg moving speed"
          value={s.avg_moving_speed_kmh == null ? "—" : `${formatNumber(s.avg_moving_speed_kmh, 1)} km/h`}
          icon={<Gauge className="h-4 w-4" />}
        />
        <Stat
          label="Max speed"
          value={s.max_speed_kmh == null ? "—" : `${formatNumber(s.max_speed_kmh, 1)} km/h`}
          icon={<TrendingUp className="h-4 w-4" />}
        />
        <Stat
          label="Points accepted"
          value={`${s.accepted_point_count} / ${s.point_count}`}
          icon={<MapPin className="h-4 w-4" />}
          hint={
            report.quality.rejected_points > 0
              ? `${report.quality.rejected_points} rejected`
              : undefined
          }
        />
      </div>

      {s.time_range && (
        <div className="flex items-center gap-2 text-xs text-slate-500">
          <Timer className="h-3.5 w-3.5" />
          <span>
            {fmtTs(s.time_range.start)} &rarr; {fmtTs(s.time_range.end)}
          </span>
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Stops ({report.stops.length})</CardTitle>
        </CardHeader>
        <CardContent>
          {report.stops.length === 0 ? (
            <div className="text-sm text-slate-500">No stops detected with current settings.</div>
          ) : (
            <div className="divide-y divide-slate-200">
              {report.stops.map((s, i) => (
                <div key={i} className="flex items-start justify-between py-2 text-sm">
                  <div>
                    <div className="font-medium">
                      {s.place_name ?? `Stop #${i + 1}`}
                    </div>
                    <div className="text-xs text-slate-500">
                      {fmtTs(s.start_ts)} &rarr; {fmtTs(s.end_ts)}
                    </div>
                    <div className="text-xs text-slate-500">
                      {s.lat.toFixed(5)}, {s.lon.toFixed(5)} &middot; {s.sample_count} samples
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="font-semibold">{formatMinutes(s.duration_min)}</div>
                    <div className="text-xs text-slate-500">~{formatNumber(s.radius_m, 0)} m</div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <TripsList trips={report.trips ?? []} />
      <HotspotsList hotspots={report.hotspots ?? []} gridM={hotspotGridM} />

      {report.quality.issues.length > 0 && (
        <Card className="border-amber-300 bg-amber-50">
          <CardHeader>
            <CardTitle className="text-sm text-amber-900">Quality notes</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="list-disc pl-5 text-xs text-amber-900">
              {report.quality.issues.map((iss, i) => (
                <li key={i}>{iss}</li>
              ))}
            </ul>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
