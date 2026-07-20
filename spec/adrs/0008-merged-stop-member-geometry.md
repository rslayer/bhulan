# ADR 0008 — Truthful merged-stop centroid + `radius_m` (member-accumulated geometry)

**Status:** Accepted
**Cycle:** 8
**Date:** 2026-07-20

## Context

Cycle 7 (ADR 0007) made `merge_nearby_stops` transitive so a chain of
consecutive pairwise-close stops collapses fully — it fixed the merge *count*.
But it still accumulated the merged *geometry* by repeated **pairwise midpoint**:

```python
lat = (blob.lat + s.lat) / 2.0
lon = circular_mean_lon([blob.lon, s.lon])
radius_m = centroid_dist_m / 2.0 + max(blob.radius_m, s.radius_m)
```

where `centroid_dist_m = haversine_m(prev_original, s)` (the last hop only).

For a group of 3+ members this is wrong two ways (the cycle-8 adversary,
`tests/adversary/test_merge_nearby_stops_chain_centroid_radius_wrong.py`, on
co-linear A, B, C at 0/40/80 m with `merge_radius_m = 45`):

- **Centroid drifts toward the tail.** Each new point pulls the running blob
  halfway, so the reported centroid lands ~50 m from A — *outside*
  `merge_radius_m`, violating cycle 7's own AC that the centroid lie within the
  cluster.
- **`radius_m` understates the true spread.** It is sized from
  `dist(prev_original, s)` (the B→C hop) rather than the distance from the
  reported centroid to the farthest member, so the field a caller uses to judge
  how tight a "stop" really is silently underreports by ~20% at 3 stops and
  more as the chain grows. Reachable unauthenticated via `/v1/insights` with
  `merge_stops_within_m`.

## Decision

Compute the merged centroid and `radius_m` from the **real member stops**, not
by drifting midpoint. This cycle is **geometry only** — the merge *decision*
(cycle 7's single-linkage against the immediately preceding original stop) is
unchanged, so the set of stops that merge is identical.

1. **Track members per blob.** A `groups: List[List[Stop]]` runs parallel to
   `merged`; when a stop is folded in, it is appended to the current group and
   the blob is re-emitted from the whole group via `_merge_members`.
2. **`_merge_members` computes truthful geometry:**
   - `lat` = the `sample_count`-weighted mean of member centroid lats.
   - `lon` = `circular_mean_lon` of the member centroid lons (unweighted circular
     mean — the spec permits this; it keeps the ±180°-straddle behaviour and the
     two-member byte parity below). Weighting the circular mean was judged not
     worth the added float noise this cycle.
   - `radius_m` = the **true enclosing radius**: `max` over members of
     `haversine_m(centroid, member) + member.radius_m`. This provably bounds the
     distance from the reported centroid to every member's own samples (each
     member's samples lie within `member.radius_m` of its centroid, and the
     centroid is `haversine_m(centroid, member)` away), so AC2 holds.
   - `duration_s` = sum of member dwells; `start_ts`/`end_ts`,
     `start_index`/`end_index`, `sample_count` span all members — all unchanged.

## Why this is byte-compatible for the two-stop case

For a two-member group of equal `sample_count` (n, n):

- weighted `lat` = `(n·A + n·B)/(2n)` = `(A+B)/2` — the old midpoint;
- `lon` = `circular_mean_lon([A.lon, B.lon])` — identical to the old call;
- `radius_m` = `max(d_A + A.r, d_B + B.r)` where `d_A ≈ d_B ≈ dist(A,B)/2` for a
  midpoint centroid, i.e. `≈ dist/2 + max(A.r, B.r)` — the old formula.

So the single-merge path is unchanged in behaviour (existing two-stop merge,
antimeridian-midpoint, and `radius_m` adversary tests stay green). The only
observable change is for a group of ≥ 3 members, which is exactly what the cycle
targets.

## Consequences

- **AC1:** merged centroid within `merge_radius_m` of every member A, B, C.
- **AC2:** reported `radius_m` ≥ the true farthest-member distance (no
  understatement) — by construction of the enclosing radius.
- **AC3:** merge decision and count unchanged from cycle 7 (single-linkage
  against `prev_original` is untouched; only emission geometry changed).
- **AC4:** two-stop merge byte-compatible (see above).
- **AC5 / cycles 1–7 green.** `haversine_m`, `latlon_to_xy_m`, `bounding_box`,
  `circular_mean_lon` untouched; antimeridian, minimal-bbox, and chain-drift
  adversaries stay green; unit + integration suites pass.

## Deferred (product decision, not this cycle)

The **transitive-merge span cap** flagged in spec §4 stays deferred to the
owner. Now that `radius_m` is truthful, a caller can *see* when a chain has
collapsed into an over-wide "stop" (e.g. a 6-stop chain spanning ~175 m under a
45 m radius); whether to additionally *split* such a merge is a semantics change
to cycle 7's single-linkage and is left out here on purpose. No span guard was
added.

## Alternatives considered

- **Recompute geometry from the raw samples of all members.** Rejected:
  `merge_nearby_stops` operates on `Stop` summaries and does not receive the
  underlying samples; member centroid + `radius_m` already summarise them, and
  the enclosing-radius bound is exact given those summaries.
- **`sample_count`-weighted circular mean for `lon`.** Considered; deferred as
  not worth the extra float perturbation this cycle (the spec explicitly allows
  the unweighted circular mean), and it would break the two-member byte parity.
