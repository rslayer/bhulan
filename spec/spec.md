# Product Spec — bhulan

> Living spec. The cockpit writes it; the loop reads it, builds, iterates
> (eval → adversarial → robustness → refute → triage), then opens a PR.
> The loop NEVER merges to master and NEVER deploys — you do. bhulan is a public
> demo; keep it PR-gated.

**Status:** cycle 5
**Cycle of last revision:** 5

---

## 1. This cycle's single outcome

**Make the reported stop/hotspot *location* antimeridian-aware**, so a dwell
straddling ±180° is pinned where the samples actually are — not at the antipode.

Cycle 4 fixed the *projection* so a dwell near the date line is now correctly
**clustered** into one stop/hotspot. That un-masked a second, pre-existing bug
one layer up: the cluster's reported centroid is still computed with a plain,
non-wraparound mean over raw longitude. Averaging `+179.9999` and `-179.9999`
the naive way gives `~0.0` — the **antipode**, ~17,800 km away on the opposite
side of the planet. So the response now contains a confident, specific stop —
real duration, real sample count — pinned in the wrong ocean. That is arguably
*worse* than the pre-cycle-4 empty result, because nothing signals the error.

Three call sites share the one root cause (a non-wraparound longitude average):

- `bhulan/analytics/stops.py::detect_stops` — `lon_c = float(np.mean([p.lon ...]))`
- `bhulan/analytics/hotspots.py::detect_hotspots` — the cluster centroid longitude
- `bhulan/analytics/stops.py::merge_nearby_stops` — `(prev.lon + s.lon) / 2.0`

All are reachable unauthenticated via `/v1/insights` and `/v1/compare`
(`current_user_optional`) — any real device dwelling near Fiji, Tonga, Kiribati,
the Aleutians, or Chukotka hits this with zero adversarial crafting.

Acceptance test (already on master, currently red):
`tests/adversary/test_antimeridian_centroid_reports_wrong_location.py`
(4 tests: stop centroid, hotspot centroid, compare-pooled shared hotspot,
merge_nearby_stops midpoint).

## 2. Hard constraints

- **The named test is the acceptance test** — it already exists and is failing.
  Fix the product code; do NOT weaken it.
- **Fix at the root, once.** Introduce a single shared wraparound-aware longitude
  mean (e.g. `bhulan/analytics/geodesy.py::circular_mean_lon(lons)`), and reuse
  it at all three call sites rather than patching each. Latitude is unaffected
  (no wraparound at ±90 for an average) — keep the plain mean for latitude.
- **Gate on the straddle, like cycle 4.** For a cluster whose raw longitude span
  is ≤ 180°, return the plain arithmetic mean so output is **byte-identical** to
  today. Only when the cluster straddles ±180° (`raw_max - raw_min > 180`) use
  the wraparound mean (mean over longitudes shifted into `[0, 360)`, folded back
  to `(-180, 180]`). This mirrors `latlon_to_xy_m` / `bounding_box` exactly.
- **Consistency with the projection.** The reported centroid must land inside the
  cluster — i.e. within `radius_m` of every sample when measured with
  `haversine_m`, the same as for a cluster anywhere else on Earth.
- **`haversine_m` is correct — do NOT touch it.** It is the cross-check. Do not
  regress cycle 4's `latlon_to_xy_m` / `bounding_box`.
- **Preserve every existing passing test** — geodesy units, cycles 1–4, and the
  cycle-4 antimeridian projection/bbox test must stay green.
- Backend/analytics only. No new dependencies, no API-shape changes, no
  DB/schema changes.

## 3. Non-negotiable acceptance criteria

- **AC1:** a tight dwell straddling ±180° is reported as a stop whose `lat`/`lon`
  centroid is within the dwell (within `radius_m` of the samples), NOT at lon≈0.
- **AC2:** the same holds for `detect_hotspots` centroids and for the
  `/v1/compare` `shared_hotspots` pooled centroid.
- **AC3:** `merge_nearby_stops` of two stops split across ±180° reports a merged
  centroid inside the real cluster, not the antipode.
- **AC4:** every non-antimeridian track produces byte-identical stops, hotspots,
  merged stops, and bbox to before (the wraparound mean is a no-op there).
- **AC5:** `poetry run pytest tests/unit/ tests/adversary/test_antimeridian_centroid_reports_wrong_location.py tests/adversary/test_antimeridian_projection_breaks_stops_and_bbox.py -q` is green.

## 4. Known traps for the adversary to probe next (this cycle's backlog)

Already harvested onto master as failing tests (from parallel driver cycle 7);
NOT this cycle's target, fix in a later cycle:

- **Polar dwell false-negative** (`test_pole_dwell_stop_false_negative.py`):
  `latlon_to_xy_m`'s single global `meters_per_deg_lon` overstates a genuinely
  tight dwell's spread near the geographic poles by ~57%, dropping a real stop.
- **Non-minimal bounding box for 3+ clusters**
  (`test_bounding_box_not_minimal_multi_cluster.py`): the two-candidate cut
  (raw vs shifted-at-0) misses the true largest-gap cut; the box can be ~80°
  wider than minimal. Degrades gracefully (always a superset).
- **merge_nearby_stops chain-drift** (`test_merge_nearby_stops_chain_drift_undermerges.py`):
  compares against the drifted merge result, not the original preceding stop.

Also still open from earlier sweeps: deeply-nested-JSON recursion → 500,
non-finite reported `speed_mps` handling, datetime-overflow / malformed-type.

## 5. Definition of done for this cycle

- AC1–AC5 pass. One shared wraparound-aware longitude mean, gated on the
  straddle; non-antimeridian output byte-identical; haversine and cycle-4
  projection/bbox untouched.
- ADR recorded in `spec/adrs/` (document the circular-mean-for-longitude
  convention and its straddle gate).
- A PR is opened for review. **No merge to `master` without your review.**

## 6. Deploy target

None from the loop. The loop opens a PR; you review and merge. bhulan is a
public demo — production stays gated.
