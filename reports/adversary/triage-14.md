# Triage — cycle 14

`poetry run pytest tests/adversary/ -q` → **4 failing tests** (1 error mode
duplicated across 2 test functions in the same file, so 3 distinct root
causes). All 4 are confirmed real defects, not false positives — see
`reports/adversary/refuted-14.md`. Ranked most severe first.

| rank | severity | finding (test file::test) | unauth? | blast radius | why it matters |
|---|---|---|---|---|---|
| 1 | **high** | `test_merge_nearby_stops_heavy_dwell_drags_centroid_far_outside_merge_radius.py::test_heavy_dwell_then_light_walk_merges_but_centroid_leaves_merge_radius` | yes — trivial | correctness / silently-wrong-answer | Reports a stop location up to **118m** (2.6x the 45m `merge_radius_m` the caller asked for) from where a merged member actually was, with no error or warning — an ordinary "idle then crawl in traffic" GPS pattern, not an edge case. |
| 2 | **high** | `test_zero_time_delta_movement_misclassified_as_stopped.py::test_stopped_segment_does_not_report_multi_km_distance` | yes — trivial | correctness / silently-wrong-answer | A segment tagged `"kind": "stopped"` reports `distance_km: 2.1127` in the same breath — internally self-contradictory output that any client filtering/summing by segment kind (e.g. "how far did they travel while stopped") will silently trust. |
| 3 | **high** | `test_zero_time_delta_movement_misclassified_as_stopped.py::test_zero_time_delta_movement_does_not_silently_zero_max_speed` | yes — trivial | correctness / silently-wrong-answer | Same root cause as #2 (`step_secs==0` divide-by-zero guard): `max_speed_kmh: 0.0` alongside `total_distance_km: 2.1127` and an **empty** `quality.issues` list — the anomaly (a physically-impossible zero-elapsed-time hop, usually a logger clock/batching glitch) is fully absorbed with no signal to the caller. |
| 4 | **medium** | `test_pole_dwell_stop_false_negative.py::test_polar_dwell_within_true_radius_is_detected_as_a_stop` | yes, but narrow input class | correctness / false negative (missed detection) | A genuine ~11m-radius dwell near a pole (lat ≈ 89.9999) is dropped entirely (0 stops instead of 1) because `latlon_to_xy_m`'s linear-longitude tangent-plane projection is degenerate near the poles. Real, but the trigger condition (dwelling within ~15-20m of true N/S pole) is geographically rare — polar research stations, polar expeditions — so exploitability by an ordinary caller is low even though the mechanism is trivial to hit once positioned there. |

Note: the two `test_zero_time_delta_movement_misclassified_as_stopped.py`
cases share one underlying defect (the `np.where(step_secs > 0, ...)` /
`if s > 0` speed guards in `bhulan/analytics/mobility.py` both collapse to
0.0 on a zero-elapsed-time step instead of flagging it) surfaced through two
separate assertions on the same response. Fixing the root cause resolves
both. When run inside the full `tests/adversary/` directory (not in
isolation) both show up as `429 Too Many Requests` instead of their real
failure — this is test-harness cross-pollution (a shared rate-limiter
singleton accumulating hits across every adversary test module in one
pytest process, confirmed by re-running the file alone), not a product
defect, and not counted as a fifth finding.

## Critical / high — failure scenarios

**#1 — merge centroid drags off to 118m (heavy dwell + light walk-away chain).**
A vehicle idles somewhere for a few minutes (300 one-second GPS samples,
`sample_count` ≈ 300), then crawls forward in heavy traffic, pausing briefly
every ~40m for a 3-hop, ~120m total walk. Every consecutive hop is well
inside the requested `merge_stops_within_m=45`, so cycle 7's single-linkage
logic correctly folds all four dwells into one merged stop — but
`_merge_members`'s `sample_count`-weighted centroid in
`bhulan/analytics/stops.py` is dominated almost entirely by the 300-sample
dwell and essentially ignores the three light (2-sample) members. The
reported centroid ends up ~118m from the farthest member it claims to
summarize — a silently wrong location for a normal, everyday movement
pattern (not a contrived "runaway drift chain"), reachable by any
unauthenticated caller who supplies ordinary `points` plus the documented
`merge_stops_within_m` option. This is spec/spec.md §1's own named
acceptance test for the current fix cycle and is intentionally still red.

**#2 / #3 — "stopped" segment reports real multi-km travel with no warning.**
20 points spread across ~2.1km of latitude, all sharing one identical
timestamp (a realistic artifact of loggers that batch-flush buffered fixes
with a single wall-clock stamp, or round to whole seconds) produce a single
segment classified `"kind": "stopped"`, `duration_min: 0.0`,
`avg_speed_kmh: 0.0`, yet `distance_km: 2.1127` — and the top-level summary
repeats the contradiction (`total_distance_km: 2.1127`,
`max_speed_kmh: 0.0`) with `quality.issues` left empty. Root cause: two
independent divide-by-zero guards in `segment_by_motion` and
`speed_stats_mps` (`bhulan/analytics/mobility.py`) silently force per-step
speed to `0.0` whenever `step_secs == 0`, but the segment's own
`distance_m`/`distance_km` is summed unconditionally from the same
`step_dist` array — so the (correct) distance and the (silently wrong)
"stopped, 0 km/h" classification for the same span both reach the caller
together, contradicting each other, with nothing in `quality` to flag it
(contrast with the existing `"Removed N duplicate points"` issue for exact
duplicates). Any unauthenticated caller triggers this trivially — the input
requires no adversarial construction, just same-timestamp consecutive
samples, which real-world loggers produce routinely.
