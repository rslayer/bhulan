# Refuted findings — cycle 4

None. All 4 failing adversary tests were investigated and kept as real defects.

## Findings reviewed and KEPT (not refuted)

### `tests/adversary/test_kml_point_timestamp_quadratic_blowup.py::test_kml_dated_points_parse_in_reasonable_time`
Real, reproducible O(n^2) blowup in `parse_kml_bytes`'s per-Point `_nearest_timestamp`
lookup. `spec/spec.md` §2 explicitly says this is "backlog for a later cycle — leave it
failing; it is not this cycle's job." That is a scope/triage decision, not a claim that
the defect is false — the spec never disputes the finding itself. Refuting is for
false positives, not for deprioritized-but-real bugs; scope triage is the human's job,
not the refuter's. Kept untouched.

### `tests/adversary/test_merge_nearby_stops_reintroduces_time_gap_bug.py` (3 tests)
- `test_insights_merge_stops_within_m_reintroduces_week_long_stop`
- `test_insights_merge_stops_within_m_stop_duration_disagrees_with_hotspot_in_same_report`
- `test_compare_per_track_merge_stops_within_m_reintroduces_gap_bug`

Verified directly against `bhulan/analytics/stops.py`: `detect_stops` is correctly
gap-aware (splits two 9-minute visits a week apart into two 540s-duration stops), but
`merge_nearby_stops` (used whenever `InsightsOptions.merge_stops_within_m` is set, a
real, documented, pre-existing option "merge consecutive stops whose centroids are
within this radius") has no time-gap check at all — it recombines duration as
`(combined_end - combined_start).total_seconds()`, i.e. the full calendar span.

Manually reproduced outside of pytest:
```
raw stops (s):    [540.0, 540.0]
merged stops (s): [605940.0]   # ~7 days, the exact gap bug the cycle-1 fix targeted
```

This directly contradicts `spec/spec.md` AC1 ("two visits ... produce two stops ...
not one stop/visit spanning the gap") through a code path (`merge_nearby_stops`) that
the cycle-1 fix did not touch. It is a real, in-scope regression of this cycle's stated
outcome, reachable through a real caller-facing option, not a contrived or unreachable
setting. All three tests kept untouched.
