# Refuted findings — cycle 13

Scope: all cases failing under `poetry run pytest tests/adversary/ -q` at
the start of this pass (4 failing cases across 3 files):

- `tests/adversary/test_merge_nearby_stops_heavy_dwell_drags_centroid_far_outside_merge_radius.py`
  (1 case)
- `tests/adversary/test_pole_dwell_stop_false_negative.py` (1 case)
- `tests/adversary/test_zero_time_delta_movement_misclassified_as_stopped.py`
  (2 cases)

**0 of 4 cases refuted. All 4 kept as real defects.**

## Verification

### Heavy-dwell centroid (AC2 violation, worse than cycle 9's fix)

Re-ran in isolation against `bhulan/analytics/stops.py`'s
`sample_count`-weighted `_merge_members` centroid. The test's own captured
output confirms the claim exactly: merging one 300-sample dwell with three
2-sample stops walking away in 40m hops (well inside `merge_stops_within_m
=45`) produces per-member distances `['38.4m', '78.3m', '118.3m']` from the
reported centroid — 2.6x the 45m merge radius, despite every consecutive
pairwise merge-decision distance being only 40m. This is not a hypothetical:
`spec/spec.md` §4 ("Known traps for the adversary to probe next") lists this
exact failure mode verbatim as an open, unresolved item — "Transitive-merge
span cap (PRODUCT DECISION, owner): heavy-dwell / runaway-chain findings —
a merged stop's centroid can't stay within `merge_radius_m` once the chain
spans > 2x it." The spec authors already know this is real and unfixed;
they've explicitly deferred the fix to a product decision, not disputed the
defect. Kept.

### Polar dwell false negative

Re-derived the fixture's own sanity check independently: `haversine_m` puts
the true separation between (89.9999, 0) and (89.9999, 180) at ~22.2m, so a
true cluster radius of ~11.1m — comfortably inside the requested
`stop_radius_m=15`. `latlon_to_xy_m`'s linear-longitude local-tangent-plane
projection is mathematically unsound this close to a pole (longitude is
near-degenerate there: two points meters apart can differ by up to 180
degrees of longitude), inflating the projected spread past the 15m
threshold and causing `detect_stops` to report zero stops for a physically
tight dwell. `spec/spec.md` §4 names this test file by filename as a known,
open trap ("Polar dwell false-negative
(`test_pole_dwell_stop_false_negative.py`)"). Physically sound fixture,
confirmed defect, spec-acknowledged. Kept.

### Zero-time-delta movement (2 cases)

Both cases in this file fail with a 429 when the full `tests/adversary/`
suite runs together — but that is a shared, process-wide rate-limiter
artifact (`RATE_LIMIT_INSIGHTS=30/minute`, keyed by the constant TestClient
peer address, accumulating unreset across every `/v1/insights` POST made by
earlier test files in the same pytest process), not a property of these
tests or the defect they claim. Re-ran this file in isolation
(`poetry run pytest tests/adversary/test_zero_time_delta_movement_misclassified_as_stopped.py
-q`) where the shared bucket starts empty: both cases fail on their actual
assertions, not on rate limiting. Traced the root cause directly in
`bhulan/analytics/mobility.py`:

- `segment_by_motion` (line 157): `step_speed = np.where(step_secs > 0,
  step_dist / step_secs, 0.0)` — a same-timestamp step's speed is forced to
  0.0 regardless of real displacement, classifying the segment "stopped".
- `speed_stats_mps` (lines 248-249): `for d, s in zip(step_dist,
  step_secs): if s > 0: ...` — the same same-timestamp step is skipped
  entirely, so it never contributes to `max_speed_kmh`.

Meanwhile the segment's own `distance_km` (and `summary.total_distance_km`)
sums `step_dist` unconditionally, with no such guard. The reproduced
response is exactly as the test describes: a `"stopped"` segment reporting
`distance_km: 2.1127`, and a top-level `total_distance_km > 2` alongside
`max_speed_kmh: 0.0` and empty `quality.issues` — internally
self-contradictory, and `spec/spec.md` §4 names this exact failure mode as
an open trap ("Zero-time-delta movement
(`test_zero_time_delta_movement_*`): segments with identical timestamps
mis-handled in speed/distance."). The 429 is a test-suite-ordering artifact,
not evidence against the claim; the assertions it's failing to reach are
independently confirmed true. Kept, untouched.

## Disposition

No tests deleted this pass. All cases in all 3 files left untouched in
`tests/adversary/`.
