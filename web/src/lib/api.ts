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
  trip_split_stop_minutes?: number;
  trip_split_gap_minutes?: number;
  hotspot_grid_m?: number;
  hotspot_min_samples?: number;
  hotspot_max_results?: number;
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

export interface TripOut {
  index: number;
  start_ts: string | null;
  end_ts: string | null;
  start_lat: number;
  start_lon: number;
  end_lat: number;
  end_lon: number;
  distance_km: number;
  duration_min: number;
  moving_time_min: number;
  idle_time_min: number;
  max_speed_kmh: number;
  sample_count: number;
}

export interface HotspotOut {
  lat: number;
  lon: number;
  sample_count: number;
  visit_count: number;
  time_spent_min: number | null;
  first_ts: string | null;
  last_ts: string | null;
  place_name?: string | null;
}

export interface QualityReport {
  rejected_points: number;
  issues: string[];
}

export interface InsightsReport {
  summary: InsightsSummary;
  stops: StopOut[];
  segments: SegmentOut[];
  trips: TripOut[];
  hotspots: HotspotOut[];
  quality: QualityReport;
}

export interface CompareTrackInput {
  label?: string;
  points?: Point[];
  text?: string;
}

export interface CompareTrackResult {
  label: string;
  report: InsightsReport;
  points: Point[];
}

export interface CompareResponse {
  tracks: CompareTrackResult[];
  shared_hotspots: HotspotOut[];
}

export interface PlotResponse {
  accepted: number;
  rejected: number;
  issues: string[];
  points: Point[];
}

const BASE = (import.meta.env.VITE_BACKEND_URL as string | undefined) || "";

// Pluggable auth-header provider. The auth layer registers a getter here at
// module load so api.ts doesn't have to import the auth module (avoids a
// circular dependency between the client and the React context that owns
// the session token).
let authHeaderProvider: (() => Record<string, string>) | null = null;

export function registerAuthHeaderProvider(
  provider: () => Record<string, string>,
): void {
  authHeaderProvider = provider;
}

function authHeaders(): Record<string, string> {
  return authHeaderProvider ? authHeaderProvider() : {};
}

async function readError(res: Response): Promise<string> {
  let msg = `${res.status} ${res.statusText}`;
  try {
    const data = await res.json();
    if (data?.detail)
      msg =
        typeof data.detail === "string"
          ? data.detail
          : JSON.stringify(data.detail);
  } catch {
    // ignore
  }
  return msg;
}

async function request<T>(
  path: string,
  init: RequestInit & { body?: BodyInit | null } = {},
): Promise<T> {
  const headers = new Headers(init.headers);
  const auth = authHeaders();
  for (const [k, v] of Object.entries(auth)) headers.set(k, v);
  const res = await fetch(`${BASE}${path}`, { ...init, headers });
  if (!res.ok) throw new Error(await readError(res));
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

async function postJSON<T>(path: string, body: unknown): Promise<T> {
  return request<T>(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

async function getJSON<T>(path: string): Promise<T> {
  return request<T>(path, { method: "GET" });
}

async function delJSON<T>(path: string): Promise<T> {
  return request<T>(path, { method: "DELETE" });
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

export function compareTracks(input: {
  tracks: CompareTrackInput[];
  options?: InsightsOptions;
}): Promise<CompareResponse> {
  return postJSON("/v1/compare", input);
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
  return request<ParseFileResponse>("/v1/parse/file", {
    method: "POST",
    body: fd,
  });
}

// --- Auth + history ---------------------------------------------------

export interface CurrentUser {
  id: number;
  email: string;
  created_at: number;
}

export interface AuthRequestResponse {
  ok: boolean;
  email: string;
  dev_magic_link?: string | null;
}

export interface AuthVerifyResponse {
  session_token: string;
  expires_in_days: number;
  user: CurrentUser;
}

export function authRequestLink(
  email: string,
): Promise<AuthRequestResponse> {
  return postJSON("/v1/auth/request", { email });
}

export function authVerifyLink(token: string): Promise<AuthVerifyResponse> {
  return postJSON("/v1/auth/verify", { token });
}

export function authMe(): Promise<CurrentUser> {
  return getJSON("/v1/auth/me");
}

export function authLogout(): Promise<{ ok: boolean }> {
  return postJSON("/v1/auth/logout", {});
}

export interface HistorySummaryEntry {
  id: number;
  created_at: number;
  kind: string;
  label: string | null;
  summary: {
    summary?: InsightsSummary;
    quality?: QualityReport;
    stop_count?: number;
    trip_count?: number;
    hotspot_count?: number;
  };
}

export interface HistoryListResponse {
  entries: HistorySummaryEntry[];
}

export interface HistoryDetail {
  id: number;
  created_at: number;
  kind: string;
  label: string | null;
  request: {
    points?: Point[];
    text?: string;
    options?: InsightsOptions;
    label?: string | null;
  } | null;
  summary: HistorySummaryEntry["summary"];
}

export function listHistory(limit = 50): Promise<HistoryListResponse> {
  return getJSON(`/v1/history?limit=${encodeURIComponent(limit)}`);
}

export function getHistoryEntry(id: number): Promise<HistoryDetail> {
  return getJSON(`/v1/history/${id}`);
}

export function deleteHistoryEntry(
  id: number,
): Promise<{ ok: boolean; id: number }> {
  return delJSON(`/v1/history/${id}`);
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
