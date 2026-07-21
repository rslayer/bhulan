# Refuted findings — cycle 14

Scope: all tests failing under `poetry run pytest tests/adversary/ -q` at the
start of this pass (4 failing cases across 3 files). Each was re-run in
isolation and checked against `bhulan/analytics/mobility.py`,
`bhulan/analytics/stops.py`, `bhulan/analytics/geodesy.py`, `spec/spec.md`,
and `spec/adrs/0011-linear-merge-and-weighted-longitude.md`.

**0 of 4 failing cases refuted. 4 kept as real defects.** This matches the
disposition of the same three tests when last reviewed
(`reports/adversary/refuted-12.md`) — nothing has changed underneath them
since; none are new claims.

## Kept (4 cases, real defects — left untouched)

- `test_merge_nearby_stops_heavy_dwell_drags_centroid_far_outside_merge_radius.py`
  (1 case): this is spec/spec.md §1's own **named acceptance test** for the
  current cycle ("Acceptance test (already on master, currently red)"). The
  claim — that the `sample_count`-weighted centroid in `_merge_members` lands
  ~118m from the farthest merged member (2.6x `merge_radius_m=45m`) for a
  heavy dwell followed by a 3-hop 40m/hop walk-away chain — is exactly the
  defect §1–§3 describe and direct this cycle's fix at (cap the transitive
  merge to `stop_radius_m`). Confirmed by reading `_merge_members` in
  `bhulan/analytics/stops.py`: the cap described in spec §2/§3 is not yet
  implemented there. Since product code is read-only for this pass and the
  spec explicitly expects this test red until the fix lands, this is the
  furthest thing from a false positive. Untouched.
- `test_pole_dwell_stop_false_negative.py` (1 case): pre-existing (cycle 7),
  explicitly named in spec/spec.md §4's backlog as a known, still-open
  finding ("polar dwell false-negative"), and spec §2 explicitly forbids
  touching `latlon_to_xy_m` this cycle. Re-verified in isolation: a 30-sample
  dwell near the north pole with true cluster radius ~11.1m (per
  `haversine_m`, well inside `stop_radius_m=15`) is projected by
  `latlon_to_xy_m`'s linear-longitude approximation to a ~17.5m spread,
  causing `detect_stops` to report 0 stops instead of 1. The described
  mechanism (longitude near-degeneracy at the pole breaking the local-tangent
  linear projection) checks out against the code. Real, deliberately
  deferred, not a false positive. Untouched.
- `test_zero_time_delta_movement_misclassified_as_stopped.py` (2 cases):
  named in spec/spec.md §4's backlog ("`zero_time_delta` movement
  mis-handling"). Re-verified both in isolation (`--no-cov`): 20 points ~2.11
  km apart in latitude, all sharing one timestamp, produce a single segment
  reported as `"kind": "stopped"` with `distance_km: 2.1127` — internally
  contradictory — and `summary.max_speed_kmh: 0.0` / `avg_moving_speed_kmh:
  0.0` alongside `total_distance_km: 2.1127` with an empty `quality.issues`
  list, so the anomaly is never surfaced to the caller. Traced the root
  cause directly in `bhulan/analytics/mobility.py`: `segment_by_motion`'s
  `np.where(step_secs > 0, step_dist / step_secs, 0.0)` (line ~156) and
  `speed_stats_mps`'s `if s > 0` scan (line ~251) both silently zero the
  per-step speed when `step_secs == 0`, while the segment's own
  `distance_m`/`distance_km` is summed from the same unconditional
  `step_dist` array — so the "stopped, 0 km/h" classification and the
  multi-km distance for the same span both end up in the response,
  contradicting each other, exactly as claimed. Real. Untouched.

## Note on 429s in the full-suite run

Both `test_zero_time_delta_movement_misclassified_as_stopped.py` cases show
`429 Too Many Requests` instead of their documented failure when run as part
of the full `tests/adversary/` directory, rather than in isolation — the
per-IP `slowapi` rate limiter (`30/minute`) accumulates across every test
module in one pytest process because `bhulan.api.app`'s `app`/limiter
singleton is imported once and shared across all adversary test files
(confirmed: alphabetically this file sorts late, after many other tests that
already hit `/v1/insights`). This is test-harness cross-pollution from
running the whole suite in one process, not a product defect and not
evidence the test's own claim is wrong — re-running in isolation recovers the
real, contradictory-response failure documented above. Not refuted on this
basis; same conclusion as the prior review in `refuted-12.md`.

## Disposition

- No files deleted.
- All 4 failing tests left untouched — all are confirmed real, and 3 of the
  4 are explicitly acknowledged as known/open in spec/spec.md (one as this
  cycle's own acceptance test, two as backlog).
