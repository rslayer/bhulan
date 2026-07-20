# Product Spec — bhulan

> Living spec. The cockpit writes it; the loop reads it, builds, iterates
> (eval → adversarial → robustness → refute → triage), then opens a PR.
> The loop NEVER merges to master and NEVER deploys — you do. bhulan is a public
> demo; keep it PR-gated.

**Status:** cycle 6
**Cycle of last revision:** 6

---

## 1. This cycle's single outcome

**Make `bounding_box` return the true minimal-longitude-span box for any number
of clusters**, so `summary.bbox` is never wider than the track actually warrants.

Cycle 4 made `bounding_box` antimeridian-aware with a **two-candidate** heuristic:
it compares the raw `[min, max]` framing against a single shifted-at-0 framing
and takes the tighter. That handles one cluster straddling ±180°, but it only
ever considers **two** of the possible circular cuts. For a track that visits
**3+ genuinely spread-out longitudes**, the true minimal box is obtained by
cutting at the **single largest angular gap** between adjacent longitudes (the
box spans the *complement* of that gap). The two-candidate heuristic misses this,
so `summary.bbox` can be reported up to ~80° (~8,900 km) wider than minimal, yet
is documented as "the minimal box." A map client that fits to this bbox zooms
out far more than the data requires.

Reachable unauthenticated via ordinary `POST /v1/insights` input — any real
track visiting three or more distant longitudes, no antimeridian jitter needed.

Acceptance test (already on master, currently red):
`tests/adversary/test_bounding_box_not_minimal_multi_cluster.py`
(`test_bounding_box_is_not_actually_minimal_for_three_plus_clusters`,
`test_insights_bbox_endpoint_is_not_minimal_for_three_plus_clusters`).

## 2. Hard constraints

- **The named test is the acceptance test** — it already exists and is failing.
  Fix the product code; do NOT weaken it.
- **Fix at the root, in `geodesy.py::bounding_box`.** Replace the two-candidate
  longitude heuristic with the **largest-gap** algorithm: sort the longitudes,
  compute the angular gaps between each adjacent pair *and* the wraparound gap
  (from the max back to the min, i.e. `min + 360 - max`); the largest gap is the
  part of the circle the track does *not* occupy, so the minimal box spans from
  the longitude just after that gap, eastward, to the longitude just before it.
  This subsumes the cycle-4 two-cluster case as a special case.
- **Keep the `min_lon > max_lon` = crosses-antimeridian convention** from ADR
  0004 unchanged; the largest-gap box uses the same encoding when it wraps.
- **Latitude is unaffected** — plain `min`/`max` as today.
- **Byte-identical for non-wraparound tracks.** When the largest gap is the
  wraparound gap (the common case — all points within a <180° arc that doesn't
  cross ±180°), the result must be exactly the naive `min_lon`/`max_lon` as
  before. Verify byte-identity over random non-wraparound tracks.
- **Do NOT regress cycles 4–5** — the antimeridian projection, centroid, and the
  single-cluster straddle bbox tests must all stay green. Do NOT touch
  `haversine_m`, `latlon_to_xy_m`, or `circular_mean_lon`.
- Backend/analytics only. No new dependencies, no API-shape changes, no
  DB/schema changes.

## 3. Non-negotiable acceptance criteria

- **AC1:** for points forming 3+ separated longitude clusters, `bounding_box`
  returns the minimal-span box (the complement of the largest angular gap), not
  a wider superset.
- **AC2:** `/v1/insights` `summary.bbox` reflects the same minimal box for such a
  track.
- **AC3:** the cycle-4 single-cluster antimeridian bbox behaviour is unchanged
  (a tight dwell straddling ±180° still reports its true small crossing box).
- **AC4:** every non-wraparound track produces a byte-identical bbox to before.
- **AC5:** `poetry run pytest tests/unit/ tests/adversary/test_bounding_box_not_minimal_multi_cluster.py tests/adversary/test_antimeridian_projection_breaks_stops_and_bbox.py tests/adversary/test_antimeridian_centroid_reports_wrong_location.py -q` is green.

## 4. Known traps for the adversary to probe next (backlog)

Still red on master (from earlier sweeps / harvested cycle-7 findings):

- **merge_nearby_stops chain-drift** (`test_merge_nearby_stops_chain_drift_undermerges.py`):
  compares each stop against the drifted merge result, not the original
  preceding stop → order-dependent under-merge.
- **Polar dwell false-negative** (`test_pole_dwell_stop_false_negative.py`):
  the global `meters_per_deg_lon` overstates a tight polar dwell's spread ~57%.
- Deeply-nested-JSON recursion → 500; non-finite reported `speed_mps`;
  datetime-overflow / malformed-type from the original robustness sweep.

## 5. Definition of done for this cycle

- AC1–AC5 pass. `bounding_box` is truly minimal via the largest-gap cut;
  non-wraparound output byte-identical; cycles 4–5 untouched and green.
- ADR recorded in `spec/adrs/` (document the largest-gap minimal-box algorithm,
  superseding the two-candidate note in ADR 0004).
- A PR is opened for review. **No merge to `master` without your review.**

## 6. Deploy target

None from the loop. The loop opens a PR; you review and merge. bhulan is a
public demo — production stays gated.
