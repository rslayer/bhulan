import { describe, expect, it } from "vitest";
import { pointsToCsv, type Point } from "@/lib/api";

describe("pointsToCsv", () => {
  it("serializes lat,lon only", () => {
    const pts: Point[] = [
      { lat: 12.97, lon: 77.59 },
      { lat: 13.0, lon: 77.6 },
    ];
    expect(pointsToCsv(pts)).toBe("12.97,77.59\n13,77.6");
  });

  it("serializes lat,lon,ts when timestamp present", () => {
    const pts: Point[] = [
      { lat: 12.97, lon: 77.59, ts_utc: "2025-01-01T09:00:00Z" },
    ];
    expect(pointsToCsv(pts)).toBe("12.97,77.59,2025-01-01T09:00:00Z");
  });

  it("preserves column positions when speed is present but ts is null", () => {
    const pts: Point[] = [
      { lat: 12.97, lon: 77.59, ts_utc: null, speed_mps: 3.5 },
    ];
    // Should emit empty ts column: lat,lon,,speed
    expect(pointsToCsv(pts)).toBe("12.97,77.59,,3.5");
  });

  it("includes both ts and speed when both present", () => {
    const pts: Point[] = [
      { lat: 12.97, lon: 77.59, ts_utc: "2025-01-01T09:00:00Z", speed_mps: 1.2 },
    ];
    expect(pointsToCsv(pts)).toBe("12.97,77.59,2025-01-01T09:00:00Z,1.2");
  });

  it("returns empty string for empty array", () => {
    expect(pointsToCsv([])).toBe("");
  });
});
