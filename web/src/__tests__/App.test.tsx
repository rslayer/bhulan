import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import App from "@/App";

// Stub out heavy children that depend on network / Leaflet / etc.
vi.mock("@/pages/InsightsPage", () => ({
  InsightsPage: () => <div data-testid="insights-page">InsightsPage</div>,
}));
vi.mock("@/pages/PlotPage", () => ({
  PlotPage: () => <div data-testid="plot-page">PlotPage</div>,
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
    authMe: vi.fn().mockRejectedValue(new Error("no session")),
  };
});

describe("App shell", () => {
  it("renders the header with branding", () => {
    render(<App />);
    expect(screen.getByText("Bhulan")).toBeInTheDocument();
  });

  it("shows the Insights tab by default", () => {
    render(<App />);
    expect(screen.getByTestId("insights-page")).toBeInTheDocument();
  });

  it("switches to Plot tab on click", async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(screen.getByRole("tab", { name: "Plot" }));
    expect(screen.getByTestId("plot-page")).toBeInTheDocument();
    expect(screen.queryByTestId("insights-page")).not.toBeInTheDocument();
  });

  it("switches to Compare tab on click", async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(screen.getByRole("tab", { name: "Compare" }));
    expect(screen.getByTestId("compare-page")).toBeInTheDocument();
  });

  it("switches to History tab on click", async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(screen.getByRole("tab", { name: "History" }));
    expect(screen.getByTestId("history-page")).toBeInTheDocument();
  });

  it("shows Privacy link in footer", () => {
    render(<App />);
    expect(screen.getByText("Privacy")).toBeInTheDocument();
  });

  it("shows anonymous message when not signed in", () => {
    render(<App />);
    expect(
      screen.getByText(/Anonymous runs aren.t stored/),
    ).toBeInTheDocument();
  });
});
