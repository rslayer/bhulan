# Adversary triage — cycle 6

`poetry run pytest tests/adversary/ -q`: **3 failed, 31 passed**. Each failing
test below is one surviving finding, ranked most severe first.

| rank | severity | finding (test file::test) | unauth? | blast radius | why it matters |
|---|---|---|---|---|---|
| 1 | **critical** | `test_kml_nested_placemark_quadratic_blowup.py::test_kml_nested_placemarks_parse_in_reasonable_time` | yes — `POST /v1/parse/file`, no `Depends` at all | availability / DoS | A single, well-formed, ~150KB KML upload with Russian-doll-nested `<Placemark>` elements (identical point count to a flat file) reintroduces the exact O(n²) parse cost cycle 3's fix was meant to remove, tying up an async worker for hours if scaled to the upload cap. |
| 2 | high | `test_merge_nearby_stops_radius_m_wrong.py::test_merged_stop_radius_m_reflects_true_spread` | yes — `POST /v1/insights`, `current_user_optional` (no auth required) | correctness / silently-wrong-answer | Merged stops report `radius_m` up to 4x tighter than their true sample spread, so downstream geofence/proximity logic built on the API's own numbers gets a false sense of precision. |
| 3 | medium | `test_merge_nearby_stops_chain_drift_undermerges.py::test_chain_of_pairwise_close_stops_fully_merges` | yes — `POST /v1/insights`, `current_user_optional` (no auth required) | correctness / silently-wrong-answer | `stop_count` and reported stop centroids depend on scan order/chain length rather than on pairwise distances, contradicting the documented merge contract, for a realistic input (a walk along a building edge). |

## Critical: KML parsing goes quadratic again for nested (not flat) Placemarks

`bhulan/analytics/file_parsers.py::_build_point_timestamps` (the cycle-3 "single
pass" fix) iterates every `<Placemark>` once, but for *each* one calls
`_iter_elems(pm, "Point")` — a `pm.findall(".//Point")` walk of that
Placemark's **entire subtree**. For flat sibling Placemarks that subtree is
O(1)-sized, so the function is genuinely O(n), which is exactly what the
cycle-3 acceptance test (`test_kml_point_timestamp_quadratic_blowup.py`)
checks — and it still passes. But nothing in the KML schema requires
Placemarks to be flat: a "Russian doll" of `n` Placemarks nested one inside
the next (Placemark₀ ⊃ Placemark₁ ⊃ … ⊃ Placemarkₙ₋₁, each with its own
`TimeStamp`+`Point`) is well-formed KML that some third-party export/re-save
tools produce when preserving folder-like grouping via nesting instead of
siblings. For outer Placemarkₖ, the subtree walk visits all `n − k` Points
nested inside it, so summed over all Placemarks the cost is
`n + (n−1) + … + 1 = O(n²)` — the identical blowup shape the fix targeted,
just moved one level down. The test measures 1,000 nested points taking
1.12s against a 0.15s budget (~7.5x over), on a 150,768-byte file. Scaling
density to `settings.MAX_UPLOAD_BYTES` (25MB, ~165k points at the same
bytes/point ratio) and applying the O(n²) growth rate projects to on the
order of **hours** of CPU time for a single request. `POST /v1/parse/file`
(`bhulan/api/routes/insights.py:204`) takes no auth dependency at all and
only checks total byte size, not structural shape or point count — a
trivially reachable, unauthenticated, single-request DoS against the public
upload surface, using the same attack class the last cycle's fix was
supposed to close.

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
geofence/proximity decisions gets a confidently wrong answer. (This is the
same bug and the same test as flagged in cycle 5's triage; cycle 6's fix
work targeted the chain-merge gap-awareness issue instead, so this one is
still open.)

## Not critical/high (medium, described in table above)

`merge_nearby_stops` compares each incoming stop against `merged[-1]` (the
running merged blob's drifted centroid) rather than the original preceding
stop, so a chain of stops each within `merge_radius_m` of their immediate
neighbor can silently fail to fully merge once earlier merges have shifted
the comparison centroid — see `bhulan/analytics/stops.py:224,231`. Same
unauthenticated reachability as the `radius_m` bug, but the failure mode
(wrong `stop_count`/segmentation for chained-input tracks) is a narrower
blast radius than a mis-stated per-stop radius, so it ranks below it.
