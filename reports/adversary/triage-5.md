# Adversary triage — cycle 5

`poetry run pytest tests/adversary/ -q`: **3 failed, 30 passed**. Each failing
test below is one surviving finding, ranked most severe first.

| rank | severity | finding (test file::test) | unauth? | blast radius | why it matters |
|---|---|---|---|---|---|
| 1 | **critical** | `test_kml_point_timestamp_quadratic_blowup.py::test_kml_dated_points_parse_in_reasonable_time` | yes — `POST /v1/parse/file`, no `Depends` at all | availability / DoS | A single, ordinary-looking KML upload (well under the 25MB cap) can pin the async worker's event loop in O(n²) parsing for hours, since parsing runs synchronously inline rather than in a threadpool. |
| 2 | high | `test_merge_nearby_stops_radius_m_wrong.py::test_merged_stop_radius_m_reflects_true_spread` | yes — `POST /v1/insights`, `current_user_optional` (no auth required) | correctness / silently-wrong-answer | Merged stops report `radius_m` up to 4x tighter than their true sample spread, so downstream geofence/proximity logic built on the API's own numbers gets a false sense of precision. |
| 3 | medium | `test_merge_nearby_stops_chain_drift_undermerges.py::test_chain_of_pairwise_close_stops_fully_merges` | yes — `POST /v1/insights`, `current_user_optional` (no auth required) | correctness / silently-wrong-answer | `stop_count` and reported stop centroids depend on scan order/chain length rather than on pairwise distances, contradicting the documented merge contract, for a realistic input (a walk along a building edge). |

## Critical: KML parsing quadratic blowup

`bhulan/analytics/file_parsers.py::parse_kml_bytes` calls `_nearest_timestamp`
per `<Point>`, and that helper rescans the whole document from the root on
every call (`bhulan/analytics/file_parsers.py:141`), making the parse O(n²)
in the number of dated points. The test measures 1500 dated points taking
3.15s against a 0.3s budget. `POST /v1/parse/file`
(`bhulan/api/routes/insights.py:204`) has **no auth dependency at all** —
unlike `/v1/insights`, it doesn't even take `current_user_optional` — and
only checks total byte size against `settings.MAX_UPLOAD_BYTES` (25MB,
`bhulan/config/settings.py:53`), not point count. A 202KB file with 1500
points already costs 3.15s; scaling density up to the 25MB cap (~190k dated
points at the same bytes/point ratio) projects to on the order of *hours* of
CPU time for one request, spent entirely inside the async request handler —
the route's own docstring states parsing is "fast enough that pushing it to
a threadpool isn't worth it," which is exactly backwards for this code path.
`RATE_LIMIT_PLOT` (60/minute) does nothing to stop a single request from
tying up a worker indefinitely once it's admitted. This is a trivially
reachable, unauthenticated, single-request DoS against the public upload
surface.

## High: `merge_nearby_stops` reports a wrong `radius_m` after a legitimate merge

`bhulan/analytics/stops.py:237-245`: when two nearby stops merge, the new
centroid is recomputed as the unweighted midpoint (`lat=(prev.lat+s.lat)/2.0`
etc.), but the reported spread is `radius_m=max(prev.radius_m, s.radius_m)`
— the spread around the two *discarded* pre-merge centroids, not around the
new midpoint. The test drives two tight (~0m internal spread) clusters ~40m
apart through `/v1/insights` with `merge_radius_m=60`, a legitimate
jitter-merge case per spec AC3: they merge into one stop whose `radius_m`
reports `0.0`, while every underlying sample actually sits ~20m from the
reported centroid — a stop detected with `stop_radius_m=5` ends up claiming
a tighter footprint than the detector's own tolerance. Reachable with
ordinary, unauthenticated `points` input to `/v1/insights` — no crafted edge
case needed, just two real-world stops close enough to legitimately merge.
Any caller trusting the API's own `radius_m` for downstream
geofence/proximity decisions gets a confidently wrong answer.

## Not critical/high (medium, described in table above)

`merge_nearby_stops` compares each incoming stop against
`merged[-1]` (the running merged blob's drifted centroid) rather than the
original preceding stop, so a chain of stops each within `merge_radius_m` of
their immediate neighbor can silently fail to fully merge once earlier
merges have shifted the comparison centroid — see
`bhulan/analytics/stops.py:224,231`. Same unauthenticated reachability as
the `radius_m` bug, but the failure mode (wrong `stop_count`/segmentation
for chained-input tracks) is a narrower blast radius than a mis-stated
per-stop radius, so it ranks below it.
