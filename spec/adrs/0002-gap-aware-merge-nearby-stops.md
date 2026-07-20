# ADR 0002 — Gap-aware `merge_nearby_stops`

**Status:** Accepted
**Cycle:** 2
**Date:** 2026-07-20

## Context

Cycle 1 (ADR 0001) made `detect_stops` gap-aware: two visits to the same spot a
week apart become two ~9-minute stops rather than one ~10 000-minute stop. But
`compute_insights` post-processes `detect_stops`' output through
`merge_nearby_stops` whenever the documented `InsightsOptions.merge_stops_within_m`
field is set:

```
raw_stops = detect_stops(..., split_gap_s=opts.trip_split_gap_minutes * 60.0)
stops = merge_nearby_stops(raw_stops, merge_radius_m=opts.merge_stops_within_m)
```

`merge_nearby_stops` (`bhulan/analytics/stops.py`) merged any two consecutive
stops whose centroids were within `merge_radius_m` **with no elapsed-time check**,
and recomputed duration as `combined_end - combined_start` — the entire calendar
span, gap and all. So two stops at the same location a week apart (distance 0,
merged by any positive radius) were folded back into one ~10 000-minute stop,
silently undoing cycle 1's fix through an option that exists specifically to clean
up GPS jitter. Meanwhile `hotspots[].time_spent_min` in the same response stayed
gap-aware, so the two fields disagreed by three orders of magnitude about the same
physical dwell.

Found by cycle 1's own adversary
(`tests/adversary/test_merge_nearby_stops_reintroduces_time_gap_bug.py`).

## Decision

Give `merge_nearby_stops` the same gap-awareness as `detect_stops`, reusing the
same `split_gap_s` notion rather than inventing a second gap concept.

1. **Gap check** — `merge_nearby_stops` gained a `split_gap_s` parameter
   (default `stops.DEFAULT_SPLIT_GAP_S`, the same constant `detect_stops` uses).
   Two stops merge only when close in space **and** the inter-stop gap
   (`s.start_ts - prev.end_ts`) is `< split_gap_s`. A gap `>= split_gap_s` is a
   real-world absence, so the stops stay separate — mirroring the `>=`-splits
   rule of `_trip_bounds` / `_cluster_end`.
2. **Duration on merge** — a legitimate merge now reports
   `prev.duration_s + s.duration_s` (the sum of the real dwells) instead of
   `combined_end - combined_start`. Even a small inter-stop gap is travel, not
   presence; and for a chained merge the calendar span would silently include any
   gap. Chaining accumulates correctly because the running `merged[-1]` already
   holds the summed dwell.
3. **Wiring** — `compute_insights` passes the existing `trip_split_gap_minutes`
   option (× 60) as `split_gap_s`, the same value it already passes to
   `detect_stops` and `detect_hotspots`. No new request field, no divergent
   constant, no API-shape change.

### Chained merges

The gap is always checked against `merged[-1].end_ts`, i.e. the running merged
stop's end. So for stops A–B–C where A–B are close in time but B–C span a gap, A
and B merge and C stays separate — the gap between the (A+B) merge and C is the
real B–C gap.

## Consequences

- Two visits to the same place separated by a long gap remain two stops with
  gap-aware durations even when `merge_stops_within_m` is set (AC1), across both
  `/v1/insights` and `/v1/compare`.
- `stops[].duration_min` and `hotspots[].time_spent_min` in the same response now
  agree within rounding (AC2).
- The jitter use case still works: two stops close in both space and time merge
  into one, with a duration equal to the real combined dwell (AC3).
- The gap threshold is the same `split_gap_s` cycle 1 introduced — one knob for
  detect and merge (AC4). All 198 unit + integration tests and cycle 1's
  gap-aware adversary tests stay green (AC5).
- The KML-parsing quadratic remains out of scope (backlog).

## Alternatives considered

- **Keep `combined_end - combined_start` for the merged duration when the gap is
  small.** Rejected: it counts the inter-stop travel as dwell and is fragile for
  chained merges. Summing the real dwells is what both duration fields should
  agree on.
- **A separate `merge_split_gap_seconds` request field.** Rejected as a second,
  divergent gap knob and an API-shape change, both ruled out by the spec.
