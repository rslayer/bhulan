# ADR 0007 — Transitive `merge_nearby_stops` (single-linkage over consecutive stops)

**Status:** Accepted
**Cycle:** 7
**Date:** 2026-07-20

## Context

`merge_nearby_stops` (`bhulan/analytics/stops.py`) folds a stop `s` into the
running result when it is close in space and time to its predecessor. The
spatial decision compared `s` against `prev = merged[-1]` — the *already-merged
blob's* recomputed centroid, which drifts toward the middle of everything folded
in so far — instead of against the original stop that immediately preceded `s`.

Concrete failure (the cycle-7 adversary,
`tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py`): three
co-linear stops A, B, C, each 40 m from its immediate neighbour, with
`merge_radius_m = 45` and no time gap.

1. A + B merge (40 m ≤ 45 m) into a blob AB centred ~20 m from both.
2. C is then compared against AB's drifted centroid (~60 m from C) rather than
   against B (40 m from C, well inside the radius), so C is left out.

The endpoint returns **2 stops instead of 1**, even though every adjacent
original pair satisfies the documented distance test ("merge consecutive stops
whose centroids are within `merge_radius_m`"). The result is order/drift
dependent: whether two genuinely-close stops merge depends on what merged before
them. Reachable unauthenticated via `/v1/insights` with `merge_stops_within_m`
on an ordinary GPS-jitter trail.

## Decision

Make the merge decision against the **immediately preceding original stop**, not
the accumulating blob — single-linkage over consecutive stops. A chain where
every adjacent original pair is within `merge_radius_m` (and not split by a gap)
collapses fully.

1. **Track the original predecessor separately.** A `prev_original` variable
   holds the original stop that immediately preceded `s` in the input, kept
   distinct from the blob `merged[-1]`. The spatial test
   (`haversine_m(prev_original, s) <= merge_radius_m`) and the time-gap test both
   use `prev_original`.
2. **Gap-awareness is preserved (cycle 2 / ADR 0002).** The time-gap boundary is
   `s.start_ts - prev_original.end_ts`. Because a blob always ends at its last
   member, `prev_original.end_ts == merged[-1].end_ts`, so this is the exact same
   original A–B boundary the blob-based check used — the fix changes only the
   *spatial* basis, never the temporal one. A gap `>= split_gap_s` between two
   original consecutive stops still splits them.
3. **Accumulation is unchanged.** The merged stop still accumulates against the
   blob: `duration_s = blob.duration_s + s.duration_s` (sum of real dwells, never
   the calendar span), midpoint `lat`/`circular_mean_lon(lon)` (cycle 5, so a
   chain straddling ±180° still centres inside the cluster),
   `start_index`/`end_index`/`sample_count` spanning all merged members, and the
   spread heuristic `centroid_dist_m / 2 + max(blob.radius_m, s.radius_m)`.

## Why the original predecessor is the correct basis

The docstring contract is stated over *consecutive* stops, pairwise. Single
linkage (merge if adjacent originals are within the radius) is the reading that
makes the contract order-independent: it depends only on the fixed original
pairwise distances, not on the running average's drift. Comparing against the
blob makes membership depend on accumulation history, which is what produced the
silent under-merge.

## Consequences

- **AC1/AC2:** A–B–C each within `merge_radius_m`, no gap → one stop; its
  `duration_s` is the sum of the three real dwells and its centroid lands inside
  the cluster.
- **AC3:** a `>= split_gap_s` gap between two members still splits (gap check uses
  the original boundary, which equals the blob boundary).
- **AC4 — byte-identical for the non-chain case.** The basis differs from the old
  code only once a blob of ≥ 2 members exists *and* a further stop is folded onto
  it. For the first merge of any pair `prev_original == merged[-1]`, so every
  two-stop merge — and any sequence that never chains a third stop onto a blob —
  produces identical output.
- **AC5 / cycles 1–6 green.** `haversine_m`, `latlon_to_xy_m`, `bounding_box`,
  `circular_mean_lon` untouched; antimeridian + minimal-bbox adversaries stay
  green; full unit + integration suites pass.

## Alternatives considered

- **Recompute the blob's true centroid and compare against that.** Rejected: the
  drifted centroid is precisely the wrong basis — the contract is pairwise over
  consecutive originals, and any blob-centred distance is history-dependent.
- **Complete-linkage / true clustering (compare against all members).** Rejected
  as over-scoped and behaviour-changing for the non-chain case, violating AC4.
  Single-linkage over consecutive stops is the minimal fix that satisfies the
  documented contract.

## Follow-up (backlog)

Now that the merge is transitive, an unbounded chain of stops each just within
`merge_radius_m` collapses into one arbitrarily wide "stop"; the only current
bound is the time-gap split. A span/total-extent cap is flagged in spec §4 for
the adversary to probe next.
