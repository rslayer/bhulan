import { cn } from "@/lib/utils";

interface Props {
  /** Number of skeleton rows to show. */
  lines?: number;
  className?: string;
}

/**
 * Pulsing placeholder shown while async content loads.
 */
export function LoadingSkeleton({ lines = 3, className }: Props) {
  return (
    <div className={cn("flex flex-col gap-3", className)} role="status" aria-label="Loading">
      {Array.from({ length: lines }, (_, i) => (
        <div
          key={i}
          className={cn(
            "h-4 animate-pulse rounded bg-slate-200",
            i === lines - 1 && "w-2/3",
          )}
        />
      ))}
      <span className="sr-only">Loading…</span>
    </div>
  );
}
