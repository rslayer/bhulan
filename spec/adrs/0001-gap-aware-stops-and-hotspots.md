# ADR 0001 — Time-gap-aware stops and hotspots

**Status:** Accepted
**Cycle:** 1
**Date:** 2026-07-20

## Context

`detect_stops` (`bhulan/analytics/stops.py`) and `detect_hotspots`
(`bhulan/analytics/hotspots.py`) were gap-blind. Both grouped samples purely by
adjacency — spatial spread for stops, array-index runs for hotspots — with no
inspection of the elapsed time between consecutive samples. A device parked at a
spot for 10 min, switched off for a week (no samples), then parked at the *same*
spot again was reported as **one** stop of ~10099 min and **one** hotspot visit
spanning the whole calendar gap, instead of two ~9-minute visits. The error grew
unboundedly with the gap, with no crash to signal it (robustness sweep run 4).

`trips.py::_trip_bounds` already solved the same problem for trips: it splits the
track wherever the gap between consecutive samples reaches
`trip_split_gap_seconds`.

## Decision

Give stops and hotspots the same gap-awareness, reusing the trips approach rather
than inventing a second mechanism.

1. **Stops** — `_cluster_end` now takes the cluster's timestamps and a
   `split_gap_s`. Growing the window stops at the first sample sitting across a
   gap `>= split_gap_s`, so a cluster can never span an absence. `detect_stops`
   gained a `split_gap_s` parameter.
2. **Hotspots** — `_visit_count` starts a new visit when two array-adjacent
   samples are `>= split_gap_s` apart, and `_time_spent_s` excludes any such
   inter-sample gap from dwell time. `detect_hotspots` gained a `split_gap_s`
   parameter.
3. **Wiring** — `insights.py` and `compare.py` pass the existing
   `trip_split_gap_minutes` option (× 60) as `split_gap_s` to both functions. No
   new request field is added; one knob drives trips, stops, and hotspots
   consistently.

### Threshold semantics

A gap **equal to** the threshold splits (`>=`), matching `_trip_bounds` exactly.
This keeps the three split rules from diverging at the boundary.

### Default value

Function-level defaults (`DEFAULT_SPLIT_GAP_S` in `stops.py`,
`DEFAULT_HOTSPOT_SPLIT_GAP_S` in `hotspots.py`) are both 60 minutes — numerically
identical to `trips.DEFAULT_TRIP_SPLIT_GAP_S`. They are defined locally rather
than imported because `trips` imports `Stop` from `stops`; importing the constant
back would create a circular import. The runtime path never relies on these
defaults — it always passes the shared `trip_split_gap_minutes` option — so the
defaults only affect direct callers (e.g. unit tests) and are documented as kept
in sync.

## Consequences

- Two visits to the same place separated by a long gap now yield two stops and
  two hotspot visits, with duration/dwell reflecting only actual presence (AC1,
  AC2), across both `/v1/insights` and `/v1/compare`.
- Normal continuous tracks (sample spacing well below 60 min) are unaffected —
  the gap check never fires, so results are byte-identical to before (AC3). All
  prior unit + integration tests and the earlier stop-detection fixes stay green.
- The O(n) fast path in `_cluster_end` is preserved; the gap check is a constant
  extra comparison per step.
- The KML-parsing quadratic remains out of scope for this cycle (backlog).

## Alternatives considered

- **A separate `stop_split_gap_seconds` / `hotspot_split_gap_seconds` request
  field.** Rejected: it would be a second, divergent gap knob and an API-shape
  change, both ruled out by the spec. Reusing `trip_split_gap_minutes` keeps one
  coherent notion of "a real-world absence".
