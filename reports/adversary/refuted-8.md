# Refuted findings — cycle 8

Reviewed all 4 tests failing under `poetry run pytest tests/adversary/ -q`
at the start of this pass (39 passed, 4 failed). All 4 are pre-existing,
previously-triaged backlog defects — none are new this cycle (cycle 5's own
target, the antimeridian centroid fix, is green: `4 tests passed` in
`test_antimeridian_centroid_reports_wrong_location.py`).

## Refuted (deleted)

None. All 4 failing tests were independently re-verified against current
product code and kept as real defects.

## Kept (not refuted)

- `tests/adversary/test_pole_dwell_stop_false_negative.py::test_polar_dwell_within_true_radius_is_detected_as_a_stop`
  — re-verified directly: `haversine_m(89.9999, 0, 89.9999, 180)` = 22.24m
  true separation (11.12m true cluster radius, well under the test's
  `stop_radius_m=15`), while `latlon_to_xy_m`'s single global
  `meters_per_deg_lon = 111_320 * cos(lat0)` linear-longitude projection
  inflates the same cluster's projected spread to ~17.5m, pushing it over
  the radius and causing `detect_stops` to report 0 stops instead of 1. This
  is explicitly named in `spec/spec.md` §4 as a known, already-harvested
  backlog defect ("Polar dwell false-negative") deliberately deferred past
  this cycle (cycle 5's scope is the antimeridian centroid fix only). Real,
  reproducible, out of scope — kept.

- `tests/adversary/test_bounding_box_not_minimal_multi_cluster.py` (both
  cases) — re-read `bounding_box()` (`bhulan/analytics/geodesy.py:54-100`):
  it only evaluates two candidate longitude framings (raw min/max, and
  longitudes shifted into `[0, 360)`), not the true minimal-span box found
  by cutting at the single largest circular gap. For longitudes
  `[-170, -100, -30, 0, 60, 170]` the largest gap is 60→170 (110°), giving a
  true minimal span of 250°; both of `bounding_box`'s candidates land on
  330° instead, an ~80° (~8,900 km) overshoot — confirmed by direct
  execution against `bounding_box()` and via `/v1/insights`. Also named in
  spec.md §4 ("Non-minimal bounding box for 3+ clusters") as backlog,
  explicitly out of scope this cycle. This defect degrades gracefully
  (always reports a superset box, never an incorrect/undersized one) and
  does not interact with — nor is it fixed by — this cycle's circular-mean
  centroid change. Real, kept.

- `tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py::test_chain_of_pairwise_close_stops_fully_merges`
  — re-read `merge_nearby_stops` (`bhulan/analytics/stops.py:219-258`):
  `prev = merged[-1]` compares each incoming stop against the *previous
  merge result's* recomputed (and drifted) centroid rather than the
  original immediately-preceding stop, so a chain A-B-C where every
  adjacent original pair is within `merge_radius_m` can still under-merge
  (B folds into A first, drifting the blob centroid away from C). Confirmed
  by direct read of the loop body — matches the test's account exactly.
  Already independently verified as real in `reports/adversary/refuted-5.md`
  and re-confirmed again in `refuted-7.md`; code is unchanged since. Named
  in spec.md §4 as backlog ("merge_nearby_stops chain-drift"), explicitly
  not this cycle's target (this cycle only changes the *midpoint longitude*
  computation inside the merge — `circular_mean_lon` — not the comparison
  logic that causes this defect). Real, kept.

No test files under `tests/adversary/` were modified or deleted this pass.
