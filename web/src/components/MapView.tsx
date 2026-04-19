import { Fragment, useEffect } from "react";
import {
  CircleMarker,
  MapContainer,
  Polyline,
  Popup,
  TileLayer,
  useMap,
} from "react-leaflet";
import L from "leaflet";
import iconUrl from "leaflet/dist/images/marker-icon.png";
import iconRetinaUrl from "leaflet/dist/images/marker-icon-2x.png";
import shadowUrl from "leaflet/dist/images/marker-shadow.png";
import type { HotspotOut, Point, StopOut } from "@/lib/api";

// Fix default marker icon paths so they load via Vite.
L.Icon.Default.mergeOptions({
  iconUrl,
  iconRetinaUrl,
  shadowUrl,
});

// Palette used for multi-track comparison overlays. These were picked
// from Tailwind's 500/600-saturation band so they stay readable against
// the default OSM basemap. More than 6 tracks is rare — we wrap around
// after the last colour.
const TRACK_COLORS = [
  "#0f172a", // slate-900 (default single-track colour)
  "#dc2626", // red-600
  "#2563eb", // blue-600
  "#16a34a", // green-600
  "#a855f7", // purple-500
  "#f59e0b", // amber-500
  "#0891b2", // cyan-600
  "#db2777", // pink-600
];

export function trackColor(index: number): string {
  return TRACK_COLORS[index % TRACK_COLORS.length];
}

export interface MapTrack {
  label?: string;
  points: Point[];
  stops?: StopOut[];
  color?: string;
}

interface Props {
  // Simple single-track shape (backwards compatible with existing callers).
  points?: Point[];
  stops?: StopOut[];
  // Multi-track compare shape. When provided, ``points``/``stops`` are ignored.
  tracks?: MapTrack[];
  // Optional shared hotspots layered on top of everything.
  hotspots?: HotspotOut[];
  className?: string;
}

function allPoints(tracks: MapTrack[]): Point[] {
  const out: Point[] = [];
  for (const t of tracks) out.push(...t.points);
  return out;
}

function FitBounds({ points }: { points: Point[] }) {
  const map = useMap();
  useEffect(() => {
    if (points.length === 0) return;
    const latlngs: [number, number][] = points.map((p) => [p.lat, p.lon]);
    const bounds = L.latLngBounds(latlngs);
    map.fitBounds(bounds, { padding: [24, 24], maxZoom: 17 });
  }, [map, points]);
  return null;
}

export function MapView({
  points,
  stops = [],
  tracks,
  hotspots = [],
  className,
}: Props) {
  // Normalize both shapes into a ``tracks`` array so the render path is
  // uniform. Single-track callers keep the original black-slate colour.
  const normalizedTracks: MapTrack[] =
    tracks && tracks.length > 0
      ? tracks.map((t, i) => ({ ...t, color: t.color ?? trackColor(i) }))
      : [{ points: points ?? [], stops, color: TRACK_COLORS[0] }];

  const pooled = allPoints(normalizedTracks);
  const hasPoints = pooled.length > 0;
  const center: [number, number] = hasPoints
    ? [pooled[0].lat, pooled[0].lon]
    : [20, 0];

  return (
    <div className={className ?? "h-[520px] w-full"}>
      <MapContainer
        center={center}
        zoom={hasPoints ? 14 : 2}
        scrollWheelZoom
        className="h-full w-full rounded-md"
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {normalizedTracks.map((t, tIdx) => {
          const line: [number, number][] = t.points.map((p) => [p.lat, p.lon]);
          const color = t.color ?? TRACK_COLORS[0];
          // Use Fragment (not a DOM <div>) so we don't render a real
          // <div> inside the Leaflet container — that can intercept
          // pointer events and confuse Leaflet's layer DOM.
          return (
            <Fragment key={`track-${tIdx}`}>
              {line.length > 1 && (
                <Polyline
                  positions={line}
                  color={color}
                  weight={3}
                  opacity={0.8}
                />
              )}
              {t.points.map((p, i) => (
                <CircleMarker
                  key={`pt-${tIdx}-${i}`}
                  center={[p.lat, p.lon]}
                  radius={3}
                  pathOptions={{
                    color,
                    fillColor: color,
                    fillOpacity: 0.7,
                  }}
                />
              ))}
              {(t.stops ?? []).map((s, i) => (
                <CircleMarker
                  key={`stop-${tIdx}-${i}`}
                  center={[s.lat, s.lon]}
                  radius={Math.max(
                    8,
                    Math.min(24, Math.log2(s.sample_count + 1) * 3),
                  )}
                  pathOptions={{
                    color: "#dc2626",
                    fillColor: "#dc2626",
                    fillOpacity: 0.25,
                    weight: 2,
                  }}
                >
                  <Popup>
                    <div className="text-xs">
                      <div className="font-semibold">
                        {t.label ? `${t.label} · ` : ""}Stop #{i + 1}
                      </div>
                      <div>Duration: {s.duration_min.toFixed(1)} min</div>
                      <div>Samples: {s.sample_count}</div>
                      <div className="text-slate-500">{s.start_ts}</div>
                      <div className="text-slate-500">to {s.end_ts}</div>
                    </div>
                  </Popup>
                </CircleMarker>
              ))}
            </Fragment>
          );
        })}
        {hotspots.map((h, i) => (
          <CircleMarker
            key={`hotspot-${i}`}
            center={[h.lat, h.lon]}
            radius={Math.max(10, Math.min(28, Math.log2(h.sample_count + 1) * 4))}
            pathOptions={{
              color: "#f59e0b",
              fillColor: "#f59e0b",
              fillOpacity: 0.2,
              weight: 2,
              dashArray: "4 3",
            }}
          >
            <Popup>
              <div className="text-xs">
                <div className="font-semibold">
                  {h.place_name ?? `Hotspot #${i + 1}`}
                </div>
                <div>{h.sample_count} samples · {h.visit_count} visits</div>
                {h.time_spent_min != null && (
                  <div>{h.time_spent_min.toFixed(1)} min total</div>
                )}
              </div>
            </Popup>
          </CircleMarker>
        ))}
        <FitBounds points={pooled} />
      </MapContainer>
    </div>
  );
}
