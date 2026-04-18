export interface Point {
  lat: number;
  lon: number;
  ts_utc?: string | null;
  speed_mps?: number | null;
}

export interface InsightsOptions {
  stop_radius_m?: number;
  min_stop_minutes?: number;
  moving_speed_kmh?: number;
  merge_stops_within_m?: number | null;
  geocode_stops?: boolean;
}

export interface TimeRange {
  start: string;
  end: string;
}

export interface BBox {
  min_lat: number;
  min_lon: number;
  max_lat: number;
  max_lon: number;
}

export interface InsightsSummary {
  point_count: number;
  accepted_point_count: number;
  time_range: TimeRange | null;
  total_distance_km: number;
  moving_time_min: number;
  idle_time_min: number;
  avg_moving_speed_kmh: number | null;
  max_speed_kmh: number | null;
  bbox: BBox | null;
}

export interface StopOut {
  lat: number;
  lon: number;
  start_ts: string;
  end_ts: string;
  duration_min: number;
  radius_m: number;
  sample_count: number;
  place_name?: string | null;
}

export interface SegmentOut {
  kind: "moving" | "stopped";
  start_ts: string;
  end_ts: string;
  distance_km: number;
  duration_min: number;
  avg_speed_kmh: number | null;
}

export interface QualityReport {
  rejected_points: number;
  issues: string[];
}

export interface InsightsReport {
  summary: InsightsSummary;
  stops: StopOut[];
  segments: SegmentOut[];
  quality: QualityReport;
}

export interface PlotResponse {
  accepted: number;
  rejected: number;
  issues: string[];
  points: Point[];
}

const BASE = (import.meta.env.VITE_BACKEND_URL as string | undefined) || "";

async function postJSON<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    let msg = `${res.status} ${res.statusText}`;
    try {
      const data = await res.json();
      if (data?.detail) msg = typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail);
    } catch {
      // ignore
    }
    throw new Error(msg);
  }
  return res.json() as Promise<T>;
}

export function computeInsights(
  input: { points?: Point[]; text?: string; options?: InsightsOptions },
): Promise<InsightsReport> {
  return postJSON("/v1/insights", input);
}

export function validatePlot(
  input: { points?: Point[]; text?: string },
): Promise<PlotResponse> {
  return postJSON("/v1/plot/validate", input);
}

export interface ParseFileResponse {
  filename: string;
  accepted: number;
  rejected: number;
  issues: string[];
  points: Point[];
}

export async function parseFile(file: File): Promise<ParseFileResponse> {
  const fd = new FormData();
  fd.append("file", file);
  const res = await fetch(`${BASE}/v1/parse/file`, {
    method: "POST",
    body: fd,
  });
  if (!res.ok) {
    let msg = `${res.status} ${res.statusText}`;
    try {
      const data = await res.json();
      if (data?.detail) msg = typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail);
    } catch {
      // ignore
    }
    throw new Error(msg);
  }
  return res.json() as Promise<ParseFileResponse>;
}

/**
 * Serialize a list of parsed points back into a CSV string that the
 * textarea can display and downstream ``/v1/insights`` / ``/v1/plot/validate``
 * calls can re-parse. Used after a file upload so the UI has a single
 * source of truth (the textarea). We emit ``lat,lon[,ts[,speed]]`` — the
 * most compatible subset of what :func:`parse_any` accepts.
 */
export function pointsToCsv(points: Point[]): string {
  return points
    .map((p) => {
      const parts = [p.lat.toString(), p.lon.toString()];
      // Preserve column positions: col 2 is always the timestamp, col 3
      // is speed. Without this, a point with a speed but no timestamp
      // (e.g. a FIT record from a device that doesn't stamp each frame)
      // would serialize as ``lat,lon,speed`` and the headerless CSV
      // parser would interpret the speed string as a timestamp via
      // ``_parse_ts``, fabricating a bogus date and dropping speed
      // entirely. Emit an empty ts column to keep the shape stable.
      if (p.ts_utc || p.speed_mps != null) parts.push(p.ts_utc ?? "");
      if (p.speed_mps != null) parts.push(String(p.speed_mps));
      return parts.join(",");
    })
    .join("\n");
}
