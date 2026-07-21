# Product Spec — bhulan

> Living spec. The cockpit writes it; the loop reads it, builds, iterates
> (eval → adversarial → robustness → refute → triage), then opens a PR.
> The loop NEVER merges to master and NEVER deploys — you do. bhulan is a public
> demo; keep it PR-gated.

**Status:** cycle 11
**Cycle of last revision:** 11

---

## 1. This cycle's single outcome

**Cap the transitive merge so a merged stop stays a bounded cluster around one
center.** A "stop" is one place — points clustered around a common centre — not
a smear along a path. Cycles 7–9 made `merge_nearby_stops` transitive
(single-linkage over consecutive stops), which correctly reunites GPS-jitter
fragments of one dwell but has **no brake**: a chain of stops each within
`merge_stops_within_m` of the next collapses into one implausibly wide "stop"
(e.g. 6 stops 40 m apart → a ~175 m span, ~4× a 45 m merge radius — that's a slow
walk/drift, not a dwell). The reported centroid then sits far from the chain's
ends, and `radius_m` balloons.

Fix: bound the merge to the **stop-size definition itself**. A stop may join the
current merged group only while the group stays a genuine cluster — every member
stop's centroid within **`stop_radius_m`** of the group's reference centre. When
adding the next stop would break that (the chain has wandered beyond one stop's
worth of ground), it is **not** merged in; it begins a new group. This directly
encodes "clustered around a common center," ties the cap to the caller's own
`stop_radius_m` (not an arbitrary multiple of the merge radius), and splits a
walk/drive back into distinct stops instead of one fake wide one.

Acceptance test (already on master, currently red):
`tests/adversary/test_merge_nearby_stops_heavy_dwell_drags_centroid_far_outside_merge_radius.py`.

## 2. Hard constraints

- **The named test is the acceptance test** — it already exists and is failing.
  Fix the product code; do NOT weaken it.
- **Thread `stop_radius_m` into the merge.** Add a `stop_radius_m: float =
  DEFAULT_RADIUS_M` parameter to `merge_nearby_stops` and pass the caller's
  `options.stop_radius_m` from `insights.compute_insights` (the same value
  `detect_stops` uses), so the merge and the detector share one definition of
  "how big one stop is." Do NOT change `DEFAULT_RADIUS_M` (stays 50 m).
- **The cap gates the single-linkage decision; it does not replace it.** Keep the
  cycle-7 rule (compare each incoming stop to the immediately preceding
  *original* stop) and the cycle-2 gap-awareness. The cap is an ADDITIONAL
  condition: even if `s` is close to `prev_original` and within the time gap, it
  only merges if the resulting group is still a cluster within `stop_radius_m` of
  its centre; otherwise `s` starts a new group.
- **Keep it O(n).** Do NOT re-check every member against a recomputed centroid on
  each append (that reintroduces the cycle-9 O(n²) DoS). Use an O(1)-per-stop
  test — e.g. gate on the incoming stop's centroid staying within `stop_radius_m`
  of the group's fixed **anchor** (its first member's centroid), which keeps
  every member within one stop-radius of a common reference and bounds the group
  span to ≤ 2·`stop_radius_m`. Document the reference choice in the ADR. Verify
  the merge stays linear (the cycle-9 `chain_quadratic_blowup` test must pass).
- **Preserve cycles 7–9 geometry.** The reported centroid stays the
  `sample_count`-weighted (circular-in-longitude) mean of the members actually in
  the group, and `radius_m` the true enclosing radius. For any input that never
  hits the cap (short jitter merges — the common case), output is **byte-identical**
  to cycle 9.
- **Do NOT regress cycles 1–10** — antimeridian projection/centroid, largest-gap
  bbox, gap-aware/transitive/linear merge, depth-guard all stay green. Do NOT
  touch `haversine_m`, `latlon_to_xy_m`, `bounding_box`, `circular_mean_lon`, or
  `detect_stops`/`_cluster_end` (progressive-movement rejection is the NEXT
  cycle, not this one).
- Backend/analytics only. No new dependencies; no API-shape change beyond the
  merge already honouring `stop_radius_m`; no DB/schema changes.

## 3. Non-negotiable acceptance criteria

- **AC1:** a chain of many pairwise-close stops no longer merges into one stop
  spanning ≫ `stop_radius_m`; it splits into groups each bounded to ≤
  2·`stop_radius_m` span, and each reported centroid lies within `stop_radius_m`
  of its own members.
- **AC2:** an ordinary two-fragment jitter dwell (both fragments within
  `stop_radius_m` of a common centre) still merges into exactly one stop.
- **AC3:** the merge remains O(n) — the `chain_quadratic_blowup` timing test
  passes with margin.
- **AC4:** any input that does not trip the cap produces byte-identical output to
  cycle 9 (centroid, `radius_m`, count, indices).
- **AC5:** `poetry run pytest tests/unit/ tests/adversary/test_merge_nearby_stops_heavy_dwell_drags_centroid_far_outside_merge_radius.py tests/adversary/test_merge_nearby_stops_chain_quadratic_blowup.py tests/adversary/test_merge_nearby_stops_chain_centroid_radius_wrong.py tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py -q` is green.

## 4. Known traps for the adversary to probe next (backlog)

- **NEXT CYCLE — progressive-movement rejection (`detect_stops`):** a continuous
  walk/drive is currently chopped into ≤`stop_radius_m` chunks and each long-
  enough chunk is reported as a stop (a ~190 m walk → 2 phantom stops). A stop
  must be a cluster *around a fixed centre*, not a translating window. Probe the
  walk-vs-dwell boundary (net directional drift vs. jitter; first-half centroid
  vs. second-half centroid) — this is the next spec.
- Structured-points deep-nesting 500 (FastAPI error-render recursion).
- `zero_time_delta` movement mis-handling; polar dwell false-negative; nonfinite
  `speed_mps`; datetime-overflow / malformed-type.

## 5. Definition of done for this cycle

- AC1–AC5 pass. The transitive merge is capped by `stop_radius_m` (clustered
  around a common centre), remains O(n), and preserves cycle-9 geometry and
  byte-identity off the cap; cycles 1–10 green.
- ADR recorded in `spec/adrs/` (document the cap = `stop_radius_m`, the reference
  choice, and why it is tied to the stop radius rather than the merge radius).
- A PR is opened for review.

## 6. Deploy target

None from the loop. The loop opens a PR; you review and merge. bhulan is a
public demo — production stays gated.
