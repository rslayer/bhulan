# Product Spec — bhulan

> Living spec. The cockpit writes it; the loop reads it, builds, iterates
> (eval → adversarial → robustness → refute → triage), then opens a PR.
> The loop NEVER merges to master and NEVER deploys — you do. bhulan is a public
> demo; keep it PR-gated.

**Status:** cycle 9
**Cycle of last revision:** 9

---

## 1. This cycle's single outcome

**Fix the two regressions cycle 8 introduced into `merge_nearby_stops`**: an
O(n²) accumulator (a DoS) and an inconsistent centroid (weighted latitude vs.
unweighted longitude) that violates AC1 for unequal-`sample_count` chains.

Cycle 8 made the merged centroid/`radius_m` truthful, but:

- **O(n²) (availability / DoS).** It calls `_merge_members(groups[-1])` on *every*
  append to a growing group, and `_merge_members` is O(group size), so a single
  chain of n pairwise-close stops costs O(n²). Measured on master: 0.125 s at
  500 stops → 7.36 s at 4000 (clean 4×-per-doubling). Reachable unauthenticated
  via `/v1/insights` with `merge_stops_within_m` and an ordinary slow-drift GPS
  trail. Cycle 7 was O(n); this is a regression.
- **Inconsistent centroid (correctness / AC1).** It weights latitude by
  `sample_count` but takes an *unweighted* `circular_mean_lon`. For a chain of
  unequal-weight stops (a dense dwell between two brief stops) the lat is pulled
  toward the dense member while the lon is not, so the centroid lands ~49 m from
  a member — outside `merge_radius_m`, violating cycle 8's own AC1.

Acceptance tests (already on master, currently red):
`tests/adversary/test_merge_nearby_stops_chain_quadratic_blowup.py`,
`tests/adversary/test_merge_nearby_stops_weighted_lat_unweighted_lon_breaks_ac1.py`.

## 2. Hard constraints

- **The named tests are the acceptance tests** — they already exist and are
  failing. Fix the product code; do NOT weaken them.
- **Make the accumulation O(n).** Decide group membership in the single pass (the
  cycle-7 single-linkage decision against the immediately preceding original
  stop is unchanged), but compute each group's merged `Stop` **once** — e.g.
  build the list of groups in the loop, then map each group through
  `_merge_members` a single time after the loop. Do NOT call `_merge_members`
  inside the per-stop loop. Total cost must be linear in the number of stops
  (verify the timing test passes and the scaling is ~linear, not ~quadratic).
- **Make the centroid consistent and weighted.** Longitude must use the **same
  `sample_count` weighting** as latitude — a weighted circular mean:
  `atan2(Σ wᵢ·sin(lonᵢ), Σ wᵢ·cos(lonᵢ))` with `wᵢ = sample_count`. Then the
  centroid is the true weighted centroid, which for a single-linkage chain (every
  member within `merge_radius_m` of the mass around the dense member) lies within
  `merge_radius_m` of every member. `circular_mean_lon` itself is used elsewhere
  and must stay unchanged — add the weighted variant alongside it (or inline the
  weighted circular mean in `_merge_members`).
- **Do NOT change the merge decision or count** — the set of stops that merge is
  identical to cycles 7–8; only performance and the centroid longitude change.
- **Preserve cycle 8's correctness.** `radius_m` stays the true enclosing radius
  (`max` over members of `haversine_m(centroid, member) + member.radius_m`) and
  must still bound every member. `duration_s`, indices, timestamps, count span
  all members. Equal-`sample_count` merges (including every two-stop equal-weight
  merge) stay byte-identical to cycle 8.
- **Do NOT regress cycles 1–8** — antimeridian projection/centroid, largest-gap
  bbox, gap-aware detect/merge, transitive merge count/geometry all green. Do NOT
  touch `haversine_m`, `latlon_to_xy_m`, `bounding_box`, `circular_mean_lon`, or
  `detect_stops`/`_cluster_end`.
- Backend/analytics only. No new dependencies, no API-shape changes, no
  DB/schema changes.

## 3. Non-negotiable acceptance criteria

- **AC1:** merging a chain of n pairwise-close stops is linear-time — the
  `chain_quadratic_blowup` timing test passes with comfortable margin.
- **AC2:** for an unequal-`sample_count` chain (dense dwell between brief stops),
  the reported centroid is within `merge_radius_m` (haversine) of every member.
- **AC3:** `radius_m` still bounds the distance from the reported centroid to the
  farthest member (cycle 8 correctness intact).
- **AC4:** the *set* of stops that merge and equal-weight merge geometry are
  unchanged from cycle 8 (byte-identical for equal-`sample_count` groups).
- **AC5:** `poetry run pytest tests/unit/ tests/adversary/test_merge_nearby_stops_chain_quadratic_blowup.py tests/adversary/test_merge_nearby_stops_weighted_lat_unweighted_lon_breaks_ac1.py tests/adversary/test_merge_nearby_stops_chain_centroid_radius_wrong.py tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py -q` is green.

## 4. Known traps for the adversary to probe next (backlog / product decisions)

- **Transitive-merge span cap (PRODUCT DECISION, owner):** a chain spanning
  ≫ `merge_radius_m` still merges into one wide stop; AC2 above holds only
  because single-linkage keeps members clustered around the mass, but a genuinely
  long drift chain can still exceed it. Whether to cap the merge span (splitting
  the chain) remains the owner's call — see
  `test_merge_nearby_stops_runaway_chain_span.py` on the parallel driver's branch.
- **Polar dwell false-negative** (`test_pole_dwell_stop_false_negative.py`).
- Deeply-nested-JSON recursion → 500; non-finite reported `speed_mps`;
  datetime-overflow / malformed-type.

## 5. Definition of done for this cycle

- AC1–AC5 pass. Group geometry computed once per group (O(n)); centroid longitude
  `sample_count`-weighted to match latitude; cycle-8 `radius_m` correctness and
  merge decision/count unchanged; cycles 1–8 green.
- ADR recorded in `spec/adrs/` (document the O(n) restructure and the weighted
  circular mean).
- A PR is opened for review.

## 6. Deploy target

None from the loop. The loop opens a PR; you review and merge. bhulan is a
public demo — production stays gated.
