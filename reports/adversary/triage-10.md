# Triage — cycle 10

`poetry run pytest tests/adversary/ -q` → **6 failed, 41 passed**. Each
failing test is one finding below, ranked most severe first. All are
reachable through the single public, unauthenticated surface
`POST /v1/insights` (stateless, no MongoDB, no auth — see
`bhulan/api/routes/insights.py`).

| rank | severity | finding (test file::test) | unauth? | blast radius | why it matters |
|---|---|---|---|---|---|
| 1 | **critical** | `test_stop_detection_quadratic_blowup.py::test_insights_clustered_track_completes_in_reasonable_time` | yes, trivial | availability / DoS | a single request with ordinary, in-limit input (one parked-vehicle track, 20k pts, well under the documented 100k cap) burns real CPU time on an actual O(n²) hot loop, and the per-IP rate limiter does nothing to stop a handful of these from queuing up and stalling the worker. |
| 2 | **high** | `test_pole_dwell_stop_false_negative.py::test_polar_dwell_within_true_radius_is_detected_as_a_stop` | yes, trivial (real-world GPS trace shape) | correctness / silently-wrong-answer | a genuine dwell physically well inside the requested `stop_radius_m` is reported as *zero* stops near a pole — this is a false negative in the core detection primitive, not a display quirk, and it fails silently (200 OK, wrong answer) rather than erroring. |
| 3 | **high** | `test_merge_nearby_stops_chain_centroid_radius_wrong.py::test_chain_merge_centroid_stays_within_merge_radius_of_every_member` | yes, trivial (3-stop chain, the cycle-7 acceptance scenario itself) | correctness / silently-wrong-answer | the reported centroid for a merged stop lands outside the caller's own configured `merge_radius_m` from a real member (50m vs. 45m), directly violating the spec's non-negotiable AC2 for the exact scenario cycle 7 was built to fix. |
| 4 | **high** | `test_merge_nearby_stops_chain_centroid_radius_wrong.py::test_chain_merge_radius_m_undersells_true_spread` | yes, trivial | correctness / silently-wrong-answer | `radius_m` — the field a caller is supposed to use to sanity-check how tight a reported "stop" really is — understates true spread by ~20% on a minimal 3-stop chain; a caller trusting this field to distinguish a real dwell from a spread-out area gets a false sense of precision. |
| 5 | **medium** | `test_merge_nearby_stops_runaway_chain_span.py::test_runaway_chain_radius_m_understates_true_spread` | yes, trivial (6-stop chain) | correctness / silently-wrong-answer | same root cause as #3/#4, but the error compounds with chain length (38% understatement at 6 stops vs. ~20% at 3) — same defect, larger and unbounded blast radius as chains grow, but it's a magnitude-of-existing-bug finding rather than a new code path. |
| 6 | **medium** | `test_merge_nearby_stops_runaway_chain_span.py::test_runaway_chain_span_vastly_exceeds_merge_radius_with_no_guard` | yes, contrived (needs a hand-built 6-stop co-linear chain each just within `merge_radius_m`; possible but unlikely from raw GPS noise, more plausible from slow-walk/GPS-drift tracks) | correctness / silently-wrong-answer (not availability — nothing crashes or hangs) | a single reported "stop" can represent a ~175m physical span (3.9x the configured `merge_radius_m`) with no field in the response signalling that the transitive merge stretched this far — downstream consumers (e.g. "was the vehicle parked here") have no way to detect this from the API surface alone. |

## Critical / high detail

**#1 — quadratic `detect_stops` (DoS).** `detect_stops` (`bhulan/analytics/stops.py`,
lines 74-111) grows a candidate-stop window one sample at a time and, on
*every* growth step, recomputes the cluster radius over the *entire current
window* from scratch (`_cluster_radius_m`, lines 41-45: fresh `np.mean` +
full distance pass). For one long dwell of size k this is O(k²), not the
O(n log n) the module docstring claims. Concretely: 20,000 clustered points
(a single parked-vehicle GPS stream, one sample/second for ~5.5 hours — a
completely ordinary workload, not an adversarial shape) already crosses the
0.5s bar in-process; the test file's own measurements show ~1.9s at 30k,
~4.5s at 50k, and ~15.5s at 100k (the service's own advertised
`MAX_PUBLIC_POINTS` cap). An unauthenticated caller can tie up a worker for
double-digit seconds with one in-spec request, and `RATE_LIMIT_INSIGHTS =
"30/minute"` does not prevent a burst of such requests before the limiter
engages. This is the only finding in the set that threatens availability
rather than correctness, which is why it ranks above the others despite
being "just" a performance bug.

**#2 — polar dwell false negative (silently wrong answer).** `latlon_to_xy_m`
(`bhulan/analytics/geodesy.py`) projects longitude linearly using a single
global `meters_per_deg_lon = 111_320 * cos(lat0)`, which is only valid when
nearby points also have nearby longitudes — false near the poles, where
longitude is nearly degenerate (points a few meters apart can differ by up
to 180° of longitude). A synthetic but physically realistic case — 30
samples alternating between two points ~22.2m apart (true haversine
distance) near lat=89.9999 — gets projected to a spread of ~17.5m, above the
requested `stop_radius_m=15`, so `detect_stops` reports zero stops for a
dwell that is, in true physical terms, well within the radius. This returns
HTTP 200 with a wrong answer (a missing stop), not an error — the kind of
failure a caller has no way to detect without independently verifying the
input. Real-world exposure is narrow (polar research/tourism GPS traces)
but the mechanism is a straightforward consequence of the existing
tangent-plane projection, already flagged as an open risk in this cycle's
own spec.

**#3/#4 — chain-merge centroid and radius wrong on the exact cycle-7 fix
scenario.** `merge_nearby_stops` (`bhulan/analytics/stops.py`) tests
membership against `prev_original` (correct, per ADR 0007's cycle-7 fix) but
recentres and sizes `radius_m` against the running `blob`, reusing
`centroid_dist_m = dist(prev_original, s)` for the recentring-displacement
term instead of the geometrically relevant `dist(blob, s)`. On the minimal
3-stop A/B/C chain (40m apart, `merge_radius_m=45`) that this cycle's own
acceptance test (`test_merge_nearby_stops_chain_drift_undermerges.py`) uses
to confirm the count is now correct, the *centroid* comes out 50m from A —
outside the configured 45m radius, a direct violation of spec AC2's
"centroid lies within ~merge_radius_m of each member" — and the reported
`radius_m` (40m) understates the true 50m spread by ~20%. This is
high-severity specifically because it's silent and hits the cycle's own
headline acceptance scenario: any caller relying on ADR 0007's documented
guarantee that AC2 holds is currently getting incorrect data back with no
error, warning, or out-of-tolerance signal.
