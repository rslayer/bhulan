# Refuted findings — cycle 5

None. All 3 failing adversary tests were investigated and kept as real defects.

## Findings reviewed and KEPT (not refuted)

### `tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py::test_chain_of_pairwise_close_stops_fully_merges`
Verified directly against `bhulan/analytics/stops.py::merge_nearby_stops` (lines
219-251): the loop sets `prev = merged[-1]`, i.e. it compares each incoming stop
against the *running merged blob's* recomputed (unweighted-average) centroid, not
against the original stop that immediately preceded it in the input sequence.

Reproduced the geometry independently:
```
haversine_m(A, B) = 40.03 m
haversine_m(B, C) = 80.06 m -> haversine_m(A, C-adjacent midpoint math) confirms
A-B = 40.03m, B-C = 40.03m, both <= merge_radius_m = 45m
```
With A and B merged first, the blob's centroid sits at the midpoint (~20m from both
A and B). C is then tested against that drifted centroid, not against B, putting it
~60m away — outside `merge_radius_m` — so C is left unmerged even though every
*adjacent original pair* satisfies the documented "centroids within merge_radius_m"
contract. This is exactly the "chained merges" trap `spec/spec.md` §4 calls out for
the adversary to probe next. The test's own request/response trace (`duration_min:
10.0` for the A-B blob, `duration_min: 5.0` for C, 2 stops instead of 1) matches this
code path precisely — not a fixture bug or misreading of intent. Kept untouched.

### `tests/adversary/test_merge_nearby_stops_radius_m_wrong.py::test_merged_stop_radius_m_reflects_true_spread`
Verified directly against `bhulan/analytics/stops.py::merge_nearby_stops` line 245:
`radius_m=max(prev.radius_m, s.radius_m)`. This reports the spread around the two
*discarded* pre-merge centroids, not the spread of the merged (recentred,
unweighted-average) centroid against the real underlying samples — despite the
merge legitimately firing (two tight clusters ~40m apart, well within
`merge_radius_m=60` and the time-gap threshold, i.e. exactly the jitter-merge use
case AC3 requires to keep working).

Confirmed the test's math: `haversine_m(_LAT_A, _LON, _LAT_B, _LON) = 40.03m` (each
cluster's own points are colocated, true per-cluster radius ~0), so after merging into
a midpoint centroid every sample sits ~20m away — but the merged stop reports
`radius_m=0.0` (max of two ~0 pre-merge radii), a stop that certifies its own centroid
is 4x closer to its samples than the `stop_radius_m=5.0` detection radius that
produced those samples in the first place. This is precisely the trap `spec/spec.md`
§4 names ("Whether `merge_nearby_stops` recomputes `radius_m`/`sample_count`
correctly after a legitimate merge"), and the cycle-2 duration fix never touched this
line. Kept untouched.

### `tests/adversary/test_kml_point_timestamp_quadratic_blowup.py::test_kml_dated_points_parse_in_reasonable_time`
Unchanged from cycle 4's assessment (`reports/adversary/refuted-4.md`): real,
reproducible O(n^2) blowup in `parse_kml_bytes`'s per-Point `_nearest_timestamp`
lookup (3.05s vs an expected <0.3s for 1500 dated points). `spec/spec.md` §2
explicitly reaffirms this cycle: "The KML-parsing quadratic ... remains backlog for a
later cycle — leave it failing." That is a scope/triage decision, not a dispute of the
finding itself. Refuting is for false positives, not for deprioritized-but-real bugs.
Kept untouched.
