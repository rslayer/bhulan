import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import App from "@/App";

// Stub out heavy children that depend on network / Leaflet / etc.
vi.mock("@/pages/InsightsPage", () => ({
  InsightsPage: () => <div data-testid="insights-page">InsightsPage</div>,
}));
vi.mock("@/pages/ComparePage", () => ({
  ComparePage: () => <div data-testid="compare-page">ComparePage</div>,
}));
vi.mock("@/pages/HistoryPage", () => ({
  HistoryPage: () => <div data-testid="history-page">HistoryPage</div>,
}));

// Stub auth so the provider boots without network.
vi.mock("@/lib/auth", () => ({
  getStoredToken: () => null,
  setStoredToken: vi.fn(),
  clearStoredToken: vi.fn(),
  extractAndClearMagicTokenFromHash: () => null,
}));
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return {
    ...actual,
    registerAuthHeaderProvider: vi.fn(),
    getCapabilities: vi.fn().mockResolvedValue({
      auth_enabled: false,
      history_enabled: false,
      reverse_geocoding_enabled: false,
      public_demo: true,
    }),
    authMe: vi.fn().mockRejectedValue(new Error("no session")),
  };
});

describe("App shell", () => {
  it("renders the header with branding", () => {
    render(<App />);
    expect(screen.getByText("Bhulan")).toBeInTheDocument();
  });

  it("shows the Analyze tab by default", () => {
    render(<App />);
    expect(screen.getByRole("tab", { name: "Analyze" })).toBeInTheDocument();
    expect(screen.getByTestId("insights-page")).toBeInTheDocument();
  });

  it("does not show a separate Plot tab", () => {
    render(<App />);
    expect(screen.queryByRole("tab", { name: "Plot" })).not.toBeInTheDocument();
  });

  it("switches to Compare tab on click", async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(screen.getByRole("tab", { name: "Compare" }));
    expect(screen.getByTestId("compare-page")).toBeInTheDocument();
  });

  it("hides History in public demo mode", () => {
    render(<App />);
    expect(screen.queryByRole("tab", { name: "History" })).not.toBeInTheDocument();
  });

  it("shows Privacy link in footer", () => {
    render(<App />);
    expect(screen.getByText("Privacy")).toBeInTheDocument();
  });

  it("shows public demo storage message when auth is disabled", async () => {
    render(<App />);
    expect(
      await screen.findByText(/Public demo mode.*aren.t stored/),
    ).toBeInTheDocument();
  });
});
