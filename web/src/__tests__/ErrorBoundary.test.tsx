import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ErrorBoundary } from "@/components/ErrorBoundary";

function Bomb(): JSX.Element {
  throw new Error("kaboom");
}

describe("ErrorBoundary", () => {
  it("renders children when no error", () => {
    render(
      <ErrorBoundary label="Test">
        <div>hello</div>
      </ErrorBoundary>,
    );
    expect(screen.getByText("hello")).toBeDefined();
  });

  it("shows fallback UI when a child throws", () => {
    // Suppress React's console.error for the expected throw
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    render(
      <ErrorBoundary label="Map">
        <Bomb />
      </ErrorBoundary>,
    );
    expect(screen.getByText("Map crashed")).toBeDefined();
    expect(screen.getByText("kaboom")).toBeDefined();
    expect(screen.getByText("Try again")).toBeDefined();
    spy.mockRestore();
  });

  it("recovers when 'Try again' is clicked", async () => {
    let shouldThrow = true;
    function MaybeThrow() {
      if (shouldThrow) throw new Error("oops");
      return <div>recovered</div>;
    }

    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    const { rerender } = render(
      <ErrorBoundary label="Widget">
        <MaybeThrow />
      </ErrorBoundary>,
    );
    expect(screen.getByText("Widget crashed")).toBeDefined();

    // Fix the component and click retry
    shouldThrow = false;
    const user = userEvent.setup();
    await user.click(screen.getByText("Try again"));

    rerender(
      <ErrorBoundary label="Widget">
        <MaybeThrow />
      </ErrorBoundary>,
    );
    expect(screen.getByText("recovered")).toBeDefined();
    spy.mockRestore();
  });
});
