# Triage — cycle 11 surviving findings

`poetry run pytest tests/adversary/ -q` → 4 failed, 43 passed. Each failing
test below is one surviving finding, ranked most severe first.

| rank | severity | finding (test file::test) | unauth? | blast radius | why it matters |
|---|---|---|---|---|---|
| 1 | high | `test_merge_nearby_stops_chain_quadratic_blowup.py::test_merging_long_drift_chain_completes_in_reasonable_time` | yes — `/v1/insights` with public `merge_stops_within_m` | availability / CPU-exhaustion DoS | A single ordinary-looking, benign-shaped request (a slowly-drifting GPS trail) already blows 17x past the 0.5s budget at just 2,000 stops (8.81s), and the O(n²) `_merge_members` accumulator means it keeps getting worse (~26.8s at 8,000) — a newly introduced regression in code shipped this cycle, not a theoretical corner case. |
| 2 | high | `test_stop_detection_quadratic_blowup.py::test_insights_clustered_track_completes_in_reasonable_time` | yes — `/v1/insights`, no special params needed | availability / CPU-exhaustion DoS | Any parked-vehicle-shaped track (many samples clustered at one spot, the endpoint's core use case) triggers `detect_stops`'s true O(n²) behavior; scales to ~15.5s of CPU per request at the service's own advertised `MAX_POINTS=100,000` cap, and the 30/min per-IP rate limit does nothing to stop a handful of such requests from saturating a worker. |
| 3 | high | `test_merge_nearby_stops_weighted_lat_unweighted_lon_breaks_ac1.py::test_chain_merge_with_unequal_sample_counts_centroid_leaves_merge_radius` | yes — `/v1/insights` with public `merge_stops_within_m`, no contrived input needed | correctness / silently-wrong-answer | An entirely ordinary track — one long dense dwell sandwiched between two brief stops, exactly the shape real GPS logging produces — makes the reported merged-stop centroid land ~48.8m from a member it claims to summarize, violating the spec's own "non-negotiable" AC1 with no error or warning surfaced to the caller. |
| 4 | medium | `test_pole_dwell_stop_false_negative.py::test_polar_dwell_within_true_radius_is_detected_as_a_stop` | yes — `/v1/insights`, but only for coordinates within ~15-20m of true north/south pole | correctness / silently-wrong-answer (false negative) | A genuine dwell physically well inside `stop_radius_m` near the pole is reported as zero stops because the tangent-plane projection breaks down there; real but narrow exposure (polar research/tourism GPS traces only), so lower likelihood of hitting real users than the other three findings. |

## Critical/High detail

**#1 — `merge_nearby_stops` O(n²) blob-geometry recompute (high).** Cycle 8's
fix replaced the old O(1)-per-step pairwise-midpoint recentring with a
member-accumulation scheme: every time a new stop folds into a growing
blob, `_merge_members` recomputes the weighted centroid and enclosing
radius over *every* member accumulated so far. For a chain of n stops that
all merge (an ordinary slowly-drifting trail using the documented, public
`merge_stops_within_m` option), total work is `1+2+...+n` = O(n²). This is
measured directly: 1,000 stops ~0.44s, 2,000 ~1.72s, 4,000 ~6.78s, 8,000
~26.8s (each doubling roughly quadruples runtime — textbook O(n²)), and
end-to-end through `/v1/insights` a 2,000-stop chain takes ~8.81s against a
0.5s budget. It is the most severe finding here because it is a *new*
regression in code that just shipped this cycle, is trivially reachable
with an unauthenticated, ordinary-shaped payload, and already blows the
budget by more than an order of magnitude at a modest scale.

**#2 — `detect_stops` O(n²) despite documented O(n log n) (high).** The
module docstring claims an O(n log n) worst case via a "running bound"
optimization, but the actual double-`while` loop recomputes the full
cluster radius (a fresh centroid + max-distance pass) on every window
growth step, making it O(k²) for any single long cluster of k points. A
single track of clustered points at the service's own advertised
`MAX_POINTS=100,000` cap takes ~15.5s of CPU for one request; the per-IP
rate limit (30/minute) permits enough such requests before throttling to
tie up a worker for tens of seconds. Ranked just below #1 because it
requires closer-to-max-size input (tens of thousands of points) to reach
dramatic wall-clock times, whereas #1 already blows its budget by 17x at a
much smaller chain length — but it's an equally real, unauthenticated
CPU-exhaustion vector against the service's primary workload shape (a
parked vehicle logging GPS).

**#3 — Weighted-lat / unweighted-lon centroid breaks AC1 (high).** The new
`_merge_members` computes `lat` as the `sample_count`-weighted mean of
member centroids but `lon` as the *unweighted* `circular_mean_lon` of the
same members — an asymmetry the spec itself explicitly permits for `lon`
in isolation, but which, combined with weighted `lat`, violates the spec's
own AC1 ("the reported centroid is within `merge_radius_m` of every member
A, B, and C") once member weights are unequal. The cycle-8 acceptance test
only exercised three *equal*-weight members, masking the bug. A concrete,
non-contrived track (A: 2 samples, B: 300 samples, C: 2 samples, each
consecutive pair ~40m apart, well inside `merge_stops_within_m=45`) — the
shape of an ordinary long dwell sandwiched between two brief ones —
produces a reported centroid ~48.8m from C, outside the 45m merge radius,
with no error surfaced. This is ranked alongside the two DoS findings
because it silently returns a wrong location for an ordinary, everyday
input on a feature whose entire purpose is reporting accurate stop
locations — a downstream consumer (dispatch, geofencing, billing) would
have no way to know the reported point is untrustworthy.
