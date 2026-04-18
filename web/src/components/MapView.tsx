import { useEffect } from "react";
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
import type { Point, StopOut } from "@/lib/api";

// Fix default marker icon paths so they load via Vite.
L.Icon.Default.mergeOptions({
  iconUrl,
  iconRetinaUrl,
  shadowUrl,
});

interface Props {
  points: Point[];
  stops?: StopOut[];
  className?: string;
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

export function MapView({ points, stops = [], className }: Props) {
  const hasPoints = points.length > 0;
  const center: [number, number] = hasPoints ? [points[0].lat, points[0].lon] : [20, 0];
  const line: [number, number][] = points.map((p) => [p.lat, p.lon]);

  return (
    <div className={className ?? "h-[520px] w-full"}>
      <MapContainer center={center} zoom={hasPoints ? 14 : 2} scrollWheelZoom className="h-full w-full rounded-md">
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {line.length > 1 && <Polyline positions={line} color="#0f172a" weight={3} opacity={0.8} />}
        {points.map((p, i) => (
          <CircleMarker
            key={i}
            center={[p.lat, p.lon]}
            radius={3}
            pathOptions={{ color: "#0f172a", fillColor: "#0f172a", fillOpacity: 0.7 }}
          />
        ))}
        {stops.map((s, i) => (
          <CircleMarker
            key={`stop-${i}`}
            center={[s.lat, s.lon]}
            radius={Math.max(8, Math.min(24, Math.log2(s.sample_count + 1) * 3))}
            pathOptions={{ color: "#dc2626", fillColor: "#dc2626", fillOpacity: 0.25, weight: 2 }}
          >
            <Popup>
              <div className="text-xs">
                <div className="font-semibold">Stop #{i + 1}</div>
                <div>Duration: {s.duration_min.toFixed(1)} min</div>
                <div>Samples: {s.sample_count}</div>
                <div className="text-slate-500">{s.start_ts}</div>
                <div className="text-slate-500">to {s.end_ts}</div>
              </div>
            </Popup>
          </CircleMarker>
        ))}
        <FitBounds points={points} />
      </MapContainer>
    </div>
  );
}
