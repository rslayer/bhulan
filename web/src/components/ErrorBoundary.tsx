import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
  children: ReactNode;
  /** Shown in the fallback UI so the user knows which section broke. */
  label?: string;
}

interface State {
  error: Error | null;
}

/**
 * Catches render-time errors in a subtree and shows a recoverable
 * fallback instead of white-screening the entire app.
 */
export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error(`ErrorBoundary [${this.props.label ?? "unknown"}]:`, error, info.componentStack);
  }

  render() {
    if (this.state.error) {
      return (
        <div className="rounded-lg border border-red-200 bg-red-50 p-6">
          <h3 className="text-sm font-semibold text-red-900">
            {this.props.label ? `${this.props.label} crashed` : "Something went wrong"}
          </h3>
          <p className="mt-1 text-xs text-red-700">{this.state.error.message}</p>
          <button
            type="button"
            className="mt-3 rounded-md bg-red-100 px-3 py-1.5 text-xs font-medium text-red-900 hover:bg-red-200"
            onClick={() => this.setState({ error: null })}
          >
            Try again
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}
