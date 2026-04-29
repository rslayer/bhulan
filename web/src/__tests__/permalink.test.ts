import { describe, expect, it } from "vitest";
import {
  encodeShareState,
  decodeShareFragment,
  MAX_FRAGMENT_BYTES,
  type ShareState,
} from "@/lib/permalink";

describe("encodeShareState / decodeShareFragment round-trip", () => {
  const state: ShareState = {
    tab: "insights",
    text: "12.97,77.59,2025-01-01T09:00:00Z",
    options: { stop_radius_m: 50 },
  };

  it("round-trips a valid state", () => {
    const fragment = encodeShareState(state);
    expect(fragment).not.toBeNull();
    expect(fragment!.startsWith("#s=v1.")).toBe(true);
    const decoded = decodeShareFragment(fragment!);
    expect(decoded).toEqual(state);
  });

  it("handles the plot tab", () => {
    const plotState: ShareState = { tab: "plot", text: "12.97,77.59" };
    const fragment = encodeShareState(plotState);
    const decoded = decodeShareFragment(fragment!);
    expect(decoded).toEqual(plotState);
  });

  it("returns null for payloads exceeding max size", () => {
    const huge: ShareState = {
      tab: "insights",
      text: "x".repeat(MAX_FRAGMENT_BYTES * 2),
    };
    expect(encodeShareState(huge)).toBeNull();
  });
});

describe("decodeShareFragment edge cases", () => {
  it("returns null for empty fragment", () => {
    expect(decodeShareFragment("")).toBeNull();
  });

  it("returns null for fragment without s= prefix", () => {
    expect(decodeShareFragment("#foo=bar")).toBeNull();
  });

  it("returns null for unknown version prefix", () => {
    expect(decodeShareFragment("#s=v2.abc")).toBeNull();
  });

  it("returns null for invalid base64", () => {
    expect(decodeShareFragment("#s=v1.!!!invalid!!!")).toBeNull();
  });

  it("returns null for valid base64 with invalid JSON", () => {
    // btoa("not json") = "bm90IGpzb24="
    expect(decodeShareFragment("#s=v1.bm90IGpzb24")).toBeNull();
  });

  it("returns null when tab is not insights or plot", () => {
    // {"tab":"other","text":"x"} → base64url
    const json = JSON.stringify({ tab: "other", text: "x" });
    const b64 = btoa(json).replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "");
    expect(decodeShareFragment(`#s=v1.${b64}`)).toBeNull();
  });

  it("strips leading # from fragment", () => {
    const state: ShareState = { tab: "plot", text: "abc" };
    const fragment = encodeShareState(state)!;
    // Already has #, but test that it works with and without
    expect(decodeShareFragment(fragment)).toEqual(state);
    expect(decodeShareFragment(fragment.slice(1))).toEqual(state);
  });
});
