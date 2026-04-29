import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { HotspotOut } from "@/lib/api";
import { formatMinutes, formatNumber } from "@/lib/utils";
import { MapPin } from "lucide-react";

interface Props {
  hotspots: HotspotOut[];
  title?: string;
  emptyNote?: string;
  /**
   * Grid size (metres) that produced these hotspots — shown as a
   * "grid ~N m" hint on each row so users who tune ``hotspot_grid_m``
   * see the setting reflected in the UI. Falls back to the backend
   * default (100 m) when the caller doesn't know, which is still
   * correct for the common case.
   */
  gridM?: number;
  /** Optional DOM id on the outer card so summary chips can scroll to it. */
  id?: string;
}

/**
 * Chip-style list of detected density hotspots. Surfaces per-hotspot
 * visit count, time-spent, and a reverse-geocoded place name when the
 * caller asked for it. Used both on the single-track Insights page and
 * inside the Compare tab for "shared places" across tracks.
 */
export function HotspotsList({
  hotspots,
  title = "Hotspots",
  emptyNote,
  gridM = 100,
  id,
}: Props) {
  return (
    <Card id={id}>
      <CardHeader>
        <CardTitle className="text-sm">
          {title} ({hotspots.length})
        </CardTitle>
      </CardHeader>
      <CardContent>
        {hotspots.length === 0 ? (
          <div className="text-sm text-slate-500">
            {emptyNote ?? "No hotspots detected with current settings."}
          </div>
        ) : (
          <div className="flex flex-col gap-2">
            {hotspots.map((h, i) => (
              <div
                key={i}
                className="flex items-start justify-between gap-3 rounded-md border border-slate-200 bg-white px-3 py-2 text-sm"
              >
                <div className="min-w-0">
                  <div className="flex items-center gap-1.5 font-medium">
                    <MapPin className="h-3.5 w-3.5 text-slate-500" />
                    {h.place_name ?? `Hotspot #${i + 1}`}
                  </div>
                  <div className="text-xs text-slate-500">
                    {h.lat.toFixed(5)}, {h.lon.toFixed(5)}
                  </div>
                </div>
                <div className="text-right text-xs text-slate-600">
                  <div className="font-semibold text-slate-900">
                    {h.sample_count} samples · {h.visit_count} visits
                  </div>
                  {h.time_spent_min != null && (
                    <div>{formatMinutes(h.time_spent_min)} total</div>
                  )}
                  {h.time_spent_min == null && <div>—</div>}
                  <div className="text-slate-500">
                    grid ~{formatNumber(gridM, 0)} m
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
