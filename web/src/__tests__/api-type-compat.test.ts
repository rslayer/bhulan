/**
 * Compile-time check that the hand-written types in api.ts remain
 * structurally compatible with the OpenAPI-generated types.
 *
 * If this file stops compiling, one of two things happened:
 *   1. The backend schema changed and ``npm run gen:api`` was run, but
 *      api.ts was not updated → update api.ts.
 *   2. Someone edited api.ts and the types no longer match the backend
 *      → reconcile with the generated types.
 */
import { describe, it, expect } from "vitest";
import type {
  GenPoint,
  GenInsightsOptions,
  GenInsightsReport,
  GenInsightsSummary,
  GenStopOut,
  GenSegmentOut,
  GenTripOut,
  GenHotspotOut,
  GenBBox,
  GenTimeRange,
  GenCompareResponse,
  GenPlotResponse,
  GenParseFileResponse,
  GenCurrentUser,
} from "@/lib/api.gen.helpers";

import type {
  Point,
  InsightsOptions,
  InsightsReport,
  InsightsSummary,
  StopOut,
  SegmentOut,
  TripOut,
  HotspotOut,
  BBox,
  TimeRange,
  CompareResponse,
  PlotResponse,
  ParseFileResponse,
  CurrentUser,
} from "@/lib/api";

// Helper: asserts A is assignable to B at compile time.
// eslint-disable-next-line @typescript-eslint/no-unused-vars
function assertAssignable<A extends B, B>() {}

describe("api.ts ↔ api.gen.ts type compatibility", () => {
  it("hand-written types are assignable to generated types", () => {
    // These calls have no runtime effect — they only cause a type error
    // at build time if the hand-written type is NOT assignable to the
    // generated one.
    assertAssignable<Point, GenPoint>();
    assertAssignable<InsightsOptions, GenInsightsOptions>();
    assertAssignable<BBox, GenBBox>();
    assertAssignable<TimeRange, GenTimeRange>();
    assertAssignable<StopOut, GenStopOut>();
    assertAssignable<SegmentOut, GenSegmentOut>();
    assertAssignable<TripOut, GenTripOut>();
    assertAssignable<HotspotOut, GenHotspotOut>();

    // The runtime assertion is just a placeholder so vitest counts this
    // as a passing test.
    expect(true).toBe(true);
  });
});
