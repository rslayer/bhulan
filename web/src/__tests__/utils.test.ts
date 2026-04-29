import { describe, expect, it } from "vitest";
import { cn, formatNumber, formatMinutes } from "@/lib/utils";

describe("cn", () => {
  it("merges tailwind classes", () => {
    expect(cn("px-2", "px-4")).toBe("px-4");
  });

  it("handles conditional classes", () => {
    expect(cn("text-sm", false && "hidden", "font-bold")).toBe(
      "text-sm font-bold",
    );
  });
});

describe("formatNumber", () => {
  it("formats with default 2 decimal digits", () => {
    const result = formatNumber(3.14159);
    expect(result).toContain("3");
    expect(result).toContain("14");
  });

  it("formats with custom digits", () => {
    const result = formatNumber(3.14159, 1);
    expect(result).toContain("3");
    expect(result).toContain("1");
  });

  it("returns dash for non-finite numbers", () => {
    expect(formatNumber(Infinity)).toBe("—");
    expect(formatNumber(NaN)).toBe("—");
    expect(formatNumber(-Infinity)).toBe("—");
  });
});

describe("formatMinutes", () => {
  it("returns '0 min' for zero or negative", () => {
    expect(formatMinutes(0)).toBe("0 min");
    expect(formatMinutes(-5)).toBe("0 min");
  });

  it("returns seconds for sub-minute values", () => {
    expect(formatMinutes(0.5)).toBe("30 s");
  });

  it("returns minutes for values under an hour", () => {
    expect(formatMinutes(15)).toBe("15.0 min");
    expect(formatMinutes(45.3)).toBe("45.3 min");
  });

  it("returns hours + minutes for >= 60", () => {
    expect(formatMinutes(90)).toBe("1 h 30 min");
    expect(formatMinutes(125)).toBe("2 h 5 min");
  });

  it("returns '0 min' for non-finite", () => {
    expect(formatMinutes(NaN)).toBe("0 min");
    expect(formatMinutes(Infinity)).toBe("0 min");
  });
});
