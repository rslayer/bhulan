import { afterEach, describe, expect, it, vi } from "vitest";
import { getStoredToken, setStoredToken, clearStoredToken } from "@/lib/auth";

describe("auth token storage", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("stores and retrieves a token", () => {
    setStoredToken("test-token-123");
    expect(getStoredToken()).toBe("test-token-123");
  });

  it("clears a stored token", () => {
    setStoredToken("test-token-123");
    clearStoredToken();
    expect(getStoredToken()).toBeNull();
  });

  it("returns null when no token is stored", () => {
    expect(getStoredToken()).toBeNull();
  });

  it("handles localStorage exceptions gracefully", () => {
    const spy = vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("QuotaExceeded");
    });
    expect(getStoredToken()).toBeNull();
    spy.mockRestore();
  });
});
