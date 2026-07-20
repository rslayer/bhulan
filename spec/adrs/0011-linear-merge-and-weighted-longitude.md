# ADR 0011 — Linear-time merge accumulation + `sample_count`-weighted longitude

**Status:** Accepted
**Cycle:** 9
**Date:** 2026-07-20

## Context

Cycle 8 (ADR 0008) made the merged-stop centroid and `radius_m` truthful by
computing them from the *real* members via `_merge_members`. That fixed the
chain-drift geometry, but it introduced two regressions into
`bhulan/analytics/stops.py::merge_nearby_stops`:

1. **O(n²) accumulation (availability / DoS).** The loop recomputed the blob's
   geometry on *every* append:

   ```python
   groups[-1].append(s)
   merged[-1] = _merge_members(groups[-1])
   ```

   `_merge_members` is O(group size), so a single chain of `n` pairwise-close
   stops cost `1 + 2 + … + n` = O(n²). Measured 0.125 s at 500 stops → 7.36 s
   at 4000 (clean 4×-per-doubling). Reachable unauthenticated via `/v1/insights`
   with `merge_stops_within_m` and an ordinary slow-drift GPS trail — the same
   CPU-exhaustion class the sibling `detect_stops` blow-up test documents.
   Cycle 7 was O(n); cycle 8 regressed it.

2. **Inconsistent centroid (correctness / AC1).** `_merge_members` weighted
   `lat` by `sample_count` but took an *unweighted* `circular_mean_lon` for
   `lon`. For an unequal-weight chain (a dense dwell between two brief stops),
   `lat` snapped toward the dense member while `lon` did not, dragging the
   reported centroid ~49 m off the true member line — outside `merge_radius_m`
   of a member, violating cycle 8's own AC1.

## Decision

**1. Compute each group's geometry once, after the single pass.** The pass now
builds only the list of member groups (the cycle-7 single-linkage decision
against the immediately preceding *original* stop is unchanged, so *which*
stops merge is byte-for-byte identical). `_merge_members` is applied exactly
once per group, after the loop:

```python
return [g[0] if len(g) == 1 else _merge_members(g) for g in groups]
```

A one-member group never merged, so it is passed through **byte-for-byte** (the
original `Stop`); recomputing its geometry would needlessly round-trip its `lon`
through trig and shift the last bit. Total cost is now linear in the number of
stops.

**2. Weight longitude by `sample_count`, matching latitude.** `_merge_members`
now uses a weighted circular mean for `lon`, inlined, with `wᵢ = sample_count`:

```python
sin_sum = sum(m.sample_count * sin(radians(m.lon)) for m in members)
cos_sum = sum(m.sample_count * cos(radians(m.lon)) for m in members)
lon = degrees(atan2(sin_sum, cos_sum))
```

Both axes now share the same weighting, so the reported centroid is the true
weighted centroid and, for a single-linkage chain, stays within `merge_radius_m`
of every member. The module-level `circular_mean_lon` (unweighted, used by
`detect_stops`/`detect_hotspots`) is left **unchanged** — the weighted variant
lives only inside `_merge_members`.

## Consequences

- **AC1:** merging an `n`-stop chain is linear-time; the `chain_quadratic_blowup`
  timing test passes with comfortable margin.
- **AC2:** for an unequal-`sample_count` chain the centroid is within
  `merge_radius_m` of every member.
- **AC3:** `radius_m` is untouched — still `max(haversine(centroid, member) +
  member.radius_m)`, bounding every member (cycle 8 correctness intact).
- **AC4:** the *set* of stops that merge is unchanged; equal-`sample_count`
  groups keep byte-identical `lat`/`radius_m`/`duration_s`/indices/count. For
  equal weights the weighted circular mean equals the unweighted `atan2`
  circular mean (the constant weight factors out); the tolerance-based
  regression tests are unaffected by the ~1e-13° float difference between the
  `atan2` mean and the prior arithmetic mean in the non-antimeridian case.
- No API-shape, dependency, or schema change. `haversine_m`, `latlon_to_xy_m`,
  `bounding_box`, `circular_mean_lon`, and `detect_stops`/`_cluster_end` are
  untouched.

## Out of scope (backlog)

The transitive-merge **span cap** (a chain spanning ≫ `merge_radius_m` still
merges into one wide stop) remains a deferred product decision — see
`test_merge_nearby_stops_runaway_chain_span.py`. AC2 holds here only because
single-linkage keeps members clustered around the mass; a genuinely long drift
chain can still exceed it.
