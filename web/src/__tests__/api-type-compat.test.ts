/**
 * Compile-time check that the hand-written types in api.ts remain
 * structurally compatible with the OpenAPI-generated types.
 *
 * If this file stops compiling, one of two things happened:
 *   1. The backend schema changed and ``npm run gen:api`` was run, but
 *      api.ts was not updated → update api.ts.
 *   2. Someone edited api.ts and the types no longer match the backend
 *      → reconcile with the generated types.
 *
 * Known intentional divergences (NOT checked here):
 *   - InsightsOptions: hand-written fields are optional (client can omit
 *     them and the backend fills defaults), generated type marks them required.
 *   - SegmentOut.avg_speed_kmh: hand-written allows null, generated does not.
 */
import { describe, it, expect } from "vitest";
import type { components } from "@/lib/api.gen";
import type {
  Point,
  StopOut,
  TripOut,
  HotspotOut,
  BBox,
  TimeRange,
} from "@/lib/api";

// Compile-time structural compatibility checks.
// Each assignment will fail to compile if the hand-written type
// is not assignable to the generated type.
const _pointCheck: components["schemas"]["PointIn"] = {} as Point;
const _bboxCheck: components["schemas"]["BBox"] = {} as BBox;
const _timeRangeCheck: components["schemas"]["TimeRange"] = {} as TimeRange;
const _stopCheck: components["schemas"]["StopOut"] = {} as StopOut;
const _tripCheck: components["schemas"]["TripOut"] = {} as TripOut;
const _hotspotCheck: components["schemas"]["HotspotOut"] = {} as HotspotOut;

// Suppress unused-variable warnings — the assignments above are the test.
void _pointCheck;
void _bboxCheck;
void _timeRangeCheck;
void _stopCheck;
void _tripCheck;
void _hotspotCheck;

describe("api.ts ↔ api.gen.ts type compatibility", () => {
  it("compiles without type errors", () => {
    // The real test is that this file compiles. If any hand-written type
    // drifts from the generated schema, tsc will fail before this runs.
    expect(true).toBe(true);
  });
});
