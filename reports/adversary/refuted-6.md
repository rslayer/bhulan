# Refuted findings — cycle 6

None. All 3 failing adversary tests were investigated and kept as real defects.

## Findings reviewed and KEPT (not refuted)

### `tests/adversary/test_kml_nested_placemark_quadratic_blowup.py::test_kml_nested_placemarks_parse_in_reasonable_time`
Reproduced directly: ran the test in isolation, 1,000-point Russian-doll-nested
KML took ~1.12s against a 0.15s budget (vs. the flat-shape acceptance test's
~0.02s for the same point count). Verified the root cause by reading
`bhulan/analytics/file_parsers.py::_build_point_timestamps` (lines 230-250):
it iterates every `<Placemark>` once via `_iter_elems(root, "Placemark")`
(genuinely O(n) for that outer loop), but for *each* Placemark it calls
`_iter_elems(pm, "Point")`, which is `pm.findall(".//Point")` — a full walk of
that Placemark's own subtree, re-scoped to `pm` on every call (each call
allocates a fresh `seen` set, so there is no cross-call memoization). For flat
sibling Placemarks that subtree is O(1)-sized, matching the cycle-3 acceptance
test's shape. Nothing in the KML format or this code requires Placemarks to be
flat siblings — nesting them (Placemark₀ ⊃ Placemark₁ ⊃ … ⊃ Placemarkₙ₋₁,
one Point each) is well-formed XML that some re-export tools produce via
folder-like grouping. For that shape, Placemarkₖ's subtree walk visits all
`n-k` Points nested inside it, so total work is `n + (n-1) + … + 1` = O(n²) —
the exact complexity class cycle-3's fix was meant to eliminate, reintroduced
one level down the same code path. `spec/spec.md` §4 explicitly flags "deeply
nested KML `<Folder>` trees" as a known trap to probe next; nested Placemarks
are the same structural class of input. This is a real, reachable,
unauthenticated DoS via `POST /v1/parse/file` on ordinary well-formed KML, not
a fixture artifact or a preference about performance. Kept untouched.

### `tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py::test_chain_of_pairwise_close_stops_fully_merges`
This test is a pre-existing cycle-5 finding (`c34593b`), not new this cycle;
`bhulan/analytics/stops.py` is unmodified since then (this cycle's diff only
touched `file_parsers.py`), so cycle 5's analysis (`reports/adversary/refuted-5.md`)
still applies verbatim. Re-verified independently rather than taking that on
faith: read `merge_nearby_stops` (`bhulan/analytics/stops.py:219-251`) — the
loop sets `prev = merged[-1]`, comparing each incoming stop against the
*running merged blob's* recomputed (unweighted-average) centroid, not against
the original stop that immediately preceded it in the input. Independently
recomputed the geometry (`haversine_m`): A-B = 40.03m, B-C = 40.03m, both
within `merge_radius_m=45.0` — matches the test's own assertions exactly. With
A and B merged first, the blob's centroid sits ~20m from both, so C (truly
40.03m from B) reads as ~60m from the drifted centroid and is left unmerged —
producing 2 stops instead of the 1 that "every adjacent pair within
merge_radius_m" should plainly yield per the function's own docstring
("Merge consecutive stops whose centroids are within `merge_radius_m`").
Real, order-dependent silently-wrong segmentation, reachable unauthenticated
via `POST /v1/insights`. Kept untouched.

### `tests/adversary/test_merge_nearby_stops_radius_m_wrong.py::test_merged_stop_radius_m_reflects_true_spread`
Also a pre-existing cycle-5 finding on unmodified code this cycle; re-verified
independently. Read `merge_nearby_stops` line 245:
`radius_m=max(prev.radius_m, s.radius_m)`. The merged centroid is the
unweighted average of the two original centroids (`lat=(prev.lat+s.lat)/2.0`),
so after a legitimate merge (two clusters ~40m apart, within
`merge_radius_m=60` and the time-gap threshold — exactly the jitter-cleanup
case the option exists for) every original sample sits ~20m from the *new*
centroid, while `radius_m` only reports the spread around the two *discarded*
pre-merge centroids. Independently recomputed the test's geometry: A-B =
40.03m, so `min_true_radius_m` = 20.015m — matches the assertion failure's
own printed value exactly. Ran the test in isolation and confirmed the merged
stop reports `radius_m=0.0` (max of two ~0 pre-merge radii) despite each
sample actually sitting ~20m from the reported centroid — 4x looser than the
`stop_radius_m=5.0` the stop was detected with. This is exactly the trap
`spec/spec.md` names for the adversary to probe ("whether `merge_nearby_stops`
recomputes `radius_m`/`sample_count` correctly after a legitimate merge"), and
no cycle's fix has touched this line. A real silently-wrong-answer defect,
reachable unauthenticated via `POST /v1/insights`. Kept untouched.
