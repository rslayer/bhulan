# Product Spec — bhulan

> Living spec. The cockpit writes it; the loop reads it, builds, iterates
> (eval → adversarial → robustness → refute → triage), then opens a PR.
> The loop NEVER merges to master and NEVER deploys — you do. bhulan is a public
> demo; keep it PR-gated.

**Status:** cycle 7
**Cycle of last revision:** 7

---

## 1. This cycle's single outcome

**Make `merge_nearby_stops` transitive over a chain of consecutive
pairwise-close stops**, so the merge no longer under-merges as an artifact of
scan order.

`bhulan/analytics/stops.py::merge_nearby_stops` decides whether to fold each
incoming stop `s` into the run by comparing it against `prev = merged[-1]` — the
**already-merged blob's drifted centroid** — instead of the original stop that
immediately preceded `s`. Concrete failure: three co-linear stops A, B, C, each
40 m from its immediate neighbour, with `merge_radius_m = 45` and no time gap.
A+B merge into a blob centred ~20 m from both; C is then compared against that
drifted centroid (~60 m away, outside the radius) rather than against B (40 m
away, inside it), so C is left out — the endpoint returns **2 stops instead of
1**. The docstring promises "merge consecutive stops whose centroids are within
`merge_radius_m`", so this is a contract violation, and the returned stop count /
dwell durations are silently wrong. Reachable unauthenticated via `/v1/insights`
with the documented `merge_stops_within_m` option and an ordinary GPS-jitter
trail (e.g. a walk along a building perimeter).

Acceptance test (already on master, currently red):
`tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py::test_chain_of_pairwise_close_stops_fully_merges`.

## 2. Hard constraints

- **The named test is the acceptance test** — it already exists and is failing.
  Fix the product code; do NOT weaken it.
- **Fix the comparison basis.** The merge decision for `s` must be made against
  the **immediately preceding original stop**, not the drifted merged blob, so a
  chain where every adjacent original pair is within `merge_radius_m` collapses
  fully (single-linkage over consecutive stops). Track the previous *original*
  stop separately from the accumulating blob.
- **Preserve gap-awareness (cycle 2).** Two stops close in space but separated by
  a real-world absence of ≥ `split_gap_s` remain two stops. The time-gap check
  must still use the boundary between the two *original* consecutive stops.
- **Keep the accumulation correct.** The merged stop's `duration_s` stays the sum
  of the real per-stop dwells (never the calendar span); its centroid must land
  inside the merged cluster (reuse `circular_mean_lon` for longitude, from cycle
  5, so a chain straddling ±180° still centres correctly); `start_index` /
  `end_index` / `sample_count` span all merged members.
- **No regression for the non-chain case.** Any sequence where no third stop
  chains onto an already-merged blob (in particular every two-stop merge) must
  produce **byte-identical** output to today — the comparison basis only differs
  once a blob has formed.
- **Do NOT regress cycles 1–6** — gap-aware detect/merge, KML O(n), antimeridian
  projection/centroid, largest-gap bbox all stay green. Do NOT touch
  `haversine_m`, `latlon_to_xy_m`, `bounding_box`, or `circular_mean_lon`.
- Backend/analytics only. No new dependencies, no API-shape changes, no
  DB/schema changes.

## 3. Non-negotiable acceptance criteria

- **AC1:** three consecutive stops A, B, C with every adjacent pair within
  `merge_radius_m` and no disqualifying time gap merge into **one** stop.
- **AC2:** the merged stop's `duration_s` is the sum of the three real dwells and
  its centroid lies within the cluster (within ~`merge_radius_m` of each member).
- **AC3:** a chain broken by a ≥ `split_gap_s` gap between two members does NOT
  merge across that gap (gap-awareness intact).
- **AC4:** every input that does not chain a third stop onto a merged blob
  (including all two-stop merges) yields byte-identical output to before.
- **AC5:** `poetry run pytest tests/unit/ tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py tests/adversary/test_antimeridian_centroid_reports_wrong_location.py tests/adversary/test_bounding_box_not_minimal_multi_cluster.py -q` is green.

## 4. Known traps for the adversary to probe next (backlog)

- **Runaway chain over-merge:** now that the merge is transitive, does an
  unbounded chain of stops each just within `merge_radius_m` collapse into one
  implausibly wide "stop"? Probe whether a span cap or total-extent guard is
  warranted (the time-gap split is the only current bound).
- **Polar dwell false-negative** (`test_pole_dwell_stop_false_negative.py`):
  global `meters_per_deg_lon` overstates a tight polar dwell's spread ~57%.
- Deeply-nested-JSON recursion → 500; non-finite reported `speed_mps`;
  datetime-overflow / malformed-type from the original robustness sweep.

## 5. Definition of done for this cycle

- AC1–AC5 pass. The merge is transitive over consecutive pairwise-close stops via
  comparison to the original preceding stop; gap-awareness and accumulation
  correct; non-chain output byte-identical; cycles 1–6 green.
- ADR recorded in `spec/adrs/` (document single-linkage-over-consecutive-stops
  and why the comparison basis is the original preceding stop).
- A PR is opened for review.

## 6. Deploy target

None from the loop. The loop opens a PR; you review and merge. bhulan is a
public demo — production stays gated.
