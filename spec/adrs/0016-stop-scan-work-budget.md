# ADR 0016 — Bound `detect_stops` scan work so a large body can't tie up a worker

**Status:** accepted (cockpit decision, owner-requested)
**Date:** 2026-07-21
**Related:** [[0013]] (merge cap), [[0014]] (progressive-movement rejection),
[[0015]] (request-body depth guard)

## Context

`detect_stops` grows a spatial cluster from a start sample, and when that cluster
is *rejected* — too short in time, or progressive movement ([[0014]]) — advances
the start by one and re-grows. For a real track (dwells are accepted and the scan
jumps past them) that is ~O(n). But a crafted **single giant cluster** is never
accepted, so the scan re-grows an O(n)-sized cluster from every sample:
O(n·cluster_size). At the public `MAX_PUBLIC_POINTS` cap of 100 000 a very slow
drift or a same-timestamp mass takes **~70 s of CPU on one worker**, from a single
unauthenticated `POST /v1/insights` — an availability/DoS hole the per-IP rate
limit (30/min) doesn't close. (Progressive-movement rejection [[0014]] widened
this: slow drifts that used to become stops are now rejected and re-grown.)

A provably-O(n) rewrite needs an incrementally-maintained centroid spread under
both add and remove (a dynamic farthest-point problem) or a different, results-
changing spread metric — out of proportion to a demo-API DoS mitigation.

## Decision

Two bounded, real-data-safe guards:

1. **Zero-duration skip (exact).** If a grown cluster's samples all share one
   timestamp, no sub-window can meet `min_duration_s`, so the whole cluster is
   skipped (`i = end + 1`) instead of re-grown. This makes a same-timestamp mass
   O(n) — a 100k same-timestamp body drops from ~70 s to **~0.8 s** — and never
   affects a real track.

2. **Absolute scan-work budget.** `detect_stops` accepts `max_scan_work` and
   raises `StopScanBudgetExceeded` once the cumulative scan work passes it, where
   *work* counts grown samples **plus the window size of every exact-radius
   recompute** — the real time driver (~2–3 M units/s). The public pipeline
   passes an **absolute** cap (`_MAX_STOP_SCAN_WORK = 12_000_000`, ≈ 5 s), not a
   per-point one: a realistic track does well under a million units regardless of
   size, while any pathological input is bounded to ~5 s. `compute_insights`
   catches the exception and **degrades gracefully** — reports no stops (and so no
   trips) plus a `quality` note, but still returns distance, speed, bbox, and
   hotspots. `max_scan_work=None` (the default) leaves direct callers uncapped.

## Consequences

- Worst-case stop scanning drops from **~70 s to ~5 s**; the same-timestamp
  variant to **~0.8 s**. Realistic and small-dense tracks are untouched (verified:
  a 41k drive+stop+drive and a 6k dense track both run < 1 s, no degradation).
- The cap is **absolute**, so it never false-positives a small dense track whose
  absolute time is fine — only genuinely large/pathological inputs (a 100k slow
  drift, or a misconfigured 100k dense mass of just-below-`min_duration` dwells)
  degrade, and they degrade *gracefully* with an actionable note ("too dense or
  degenerate … reduce points or adjust `min_stop_minutes`").
- **Residual / follow-up:** ~5 s is a bound, not a real O(n) fix. Two options for
  later — a true O(n) sliding-window `detect_stops` (hard), or lowering
  `MAX_PUBLIC_POINTS` (a product call) to shrink the absolute worst case further.
