import { useEffect } from "react";
import { useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet.heat";
import type { Point } from "@/lib/api";

interface Props {
  points: Point[];
  // Leaflet.heat "radius" in pixels — bigger = blurrier. Default matches
  // the plugin's own default but slightly larger for readability at urban
  // zoom levels.
  radius?: number;
  // Per-sample intensity. Useful if the caller wants to weight points
  // (e.g. speed-based). Default: constant 1.0.
  intensityOf?: (p: Point) => number;
}

// Minimal typing for leaflet.heat — the @types/leaflet.heat package is
// thin and doesn't expose the options we use.
interface HeatLayerOptions {
  radius?: number;
  blur?: number;
  maxZoom?: number;
  minOpacity?: number;
  max?: number;
  gradient?: Record<number, string>;
}
type HeatLatLng = [number, number, number];
interface HeatLayerFactory {
  (latlngs: HeatLatLng[], options?: HeatLayerOptions): L.Layer;
}

/**
 * Renders a Leaflet heatmap layer for the given points. Mounts on map
 * init, tears down on unmount. Changing `points` or `radius` rebuilds
 * the layer — Leaflet.heat supports `setLatLngs` but not `setOptions`,
 * so full re-creation is simpler and fine at our data volumes.
 */
export function HeatLayer({ points, radius = 18, intensityOf }: Props) {
  const map = useMap();
  useEffect(() => {
    if (points.length === 0) return;
    const heat = (L as unknown as { heatLayer: HeatLayerFactory }).heatLayer(
      points.map((p) => [p.lat, p.lon, intensityOf ? intensityOf(p) : 1.0]),
      {
        radius,
        blur: Math.max(8, Math.round(radius * 0.8)),
        maxZoom: 17,
        minOpacity: 0.35,
      },
    );
    heat.addTo(map);
    return () => {
      heat.remove();
    };
  }, [map, points, radius, intensityOf]);
  return null;
}
