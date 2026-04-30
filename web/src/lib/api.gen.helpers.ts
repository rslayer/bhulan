/**
 * Convenience aliases for the generated OpenAPI types.
 *
 * Import from here instead of digging into
 * ``components["schemas"]["…"]`` every time.
 */

import type { components } from "./api.gen";

export type GenPoint = components["schemas"]["PointIn"];
export type GenInsightsOptions = components["schemas"]["InsightsOptions"];
export type GenInsightsReport = components["schemas"]["InsightsReport"];
export type GenInsightsSummary = components["schemas"]["InsightsSummary"];
export type GenStopOut = components["schemas"]["StopOut"];
export type GenSegmentOut = components["schemas"]["SegmentOut"];
export type GenTripOut = components["schemas"]["TripOut"];
export type GenHotspotOut = components["schemas"]["HotspotOut"];
export type GenInsightsQuality = components["schemas"]["InsightsQuality"];
export type GenBBox = components["schemas"]["BBox"];
export type GenTimeRange = components["schemas"]["TimeRange"];
export type GenCompareTrack = components["schemas"]["CompareTrack"];
export type GenCompareTrackResult = components["schemas"]["CompareTrackResult"];
export type GenCompareResponse = components["schemas"]["CompareResponse"];
export type GenPlotResponse = components["schemas"]["PlotResponse"];
export type GenParseFileResponse = components["schemas"]["ParseFileResponse"];
export type GenCurrentUser = components["schemas"]["MeResponse"];
export type GenHistorySummary = components["schemas"]["HistorySummary"];
export type GenHistoryListResponse = components["schemas"]["HistoryListResponse"];
export type GenHistoryDetail = components["schemas"]["HistoryDetail"];
