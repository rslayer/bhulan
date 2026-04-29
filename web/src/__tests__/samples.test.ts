import { describe, expect, it } from "vitest";
import { SAMPLES } from "@/lib/samples";

describe("SAMPLES", () => {
  it("has exactly three presets", () => {
    expect(SAMPLES).toHaveLength(3);
  });

  it.each(SAMPLES)("$id has non-empty text with lat,lon lines", (sample) => {
    expect(sample.text.length).toBeGreaterThan(0);
    const lines = sample.text.split("\n").filter((l) => !l.startsWith("#") && l.trim());
    expect(lines.length).toBeGreaterThan(0);
    for (const line of lines) {
      const parts = line.split(",");
      expect(parts.length).toBeGreaterThanOrEqual(2);
      const lat = parseFloat(parts[0]);
      const lon = parseFloat(parts[1]);
      expect(lat).toBeGreaterThanOrEqual(-90);
      expect(lat).toBeLessThanOrEqual(90);
      expect(lon).toBeGreaterThanOrEqual(-180);
      expect(lon).toBeLessThanOrEqual(180);
    }
  });

  it("has unique IDs", () => {
    const ids = SAMPLES.map((s) => s.id);
    expect(new Set(ids).size).toBe(ids.length);
  });
});
