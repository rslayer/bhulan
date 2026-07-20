# Adversary cycle 4 — probing the cycle-1 time-gap-aware stops/hotspots fix

Target: this cycle's single outcome — `detect_stops` / `detect_hotspots`
becoming gap-aware (`spec/spec.md`, `spec/adrs/0001-gap-aware-stops-and-hotspots.md`).
Same in-process `TestClient(app)` pattern as prior runs, no MongoDB. Per the
brief: the already-fixed NaN/overflow/type-coercion classes were not
re-walked; the KML quadratic (`test_kml_point_timestamp_quadratic_blowup.py`)
is confirmed still failing/backlog and untouched. Coverage-guided: started
from `reports/adversary/coverage-gaps.md`, which flagged `stops.py` lines
199-213/225-226 as never reached by any adversary test — that's exactly
where the defect below lives.

## Finding: `merge_stops_within_m` reintroduces the exact bug this cycle fixed

**1 new defect, 3 reproducing failing tests**, all under
`tests/adversary/test_merge_nearby_stops_reintroduces_time_gap_bug.py`.

`bhulan/analytics/insights.py::compute_insights` wires the new gap-aware
`detect_stops` correctly:

```python
raw_stops = detect_stops(prepared, ..., split_gap_s=opts.trip_split_gap_minutes * 60.0)
stops = merge_nearby_stops(raw_stops, merge_radius_m=opts.merge_stops_within_m)
```

`detect_stops` now correctly splits two visits to the same spot a week
apart into two ~9-minute stops. But the very next line, `merge_nearby_stops`
(`bhulan/analytics/stops.py:187-226`), was not touched by the cycle-1 fix
and has **no time-gap awareness of its own**:

```python
if haversine_m(prev.lat, prev.lon, s.lat, s.lon) <= merge_radius_m:
    combined_start = prev.start_ts
    combined_end = s.end_ts
    duration = (combined_end - combined_start).total_seconds()
    ...
```

It merges any two *spatially* close consecutive stops regardless of how
much real time (or how many missing samples) separate them. Two stops at
the same centroid have distance 0, so *any* positive
`merge_stops_within_m` merges them — and `duration` is recomputed as
`combined_end - combined_start`, the full calendar span, gap included.

`merge_stops_within_m` is not a contrived/obscure setting — it's a
documented `InsightsOptions` field that exists specifically "to merge
consecutive stops whose centroids are within this radius" (e.g. to clean up
GPS-jitter-split stops). Any caller who turns it on to fix jitter silently
loses this cycle's headline gap-awareness fix.

Reproduction (`POST /v1/insights`, office visit / 7-day absence / office
visit, `options.merge_stops_within_m: 100.0`):

```
stops:    [{"duration_min": 10099.0, "sample_count": 20, ...}]   # one stop, spans the whole week
hotspots: [{"visit_count": 2, "time_spent_min": 18.0, ...}]      # correctly gap-aware
```

Within the **same** `/v1/insights` response, `stops[].duration_min` and
`hotspots[].time_spent_min` describe the same physical dwell at the same
centroid in the same track and disagree by ~561x, because hotspots have no
analogous merge step and stayed gap-aware while stops did not.

Confirmed to reproduce identically through `/v1/compare`'s per-track
reports (same `compute_insights` → `merge_nearby_stops` path per track).

3 tests, all failing on current code:
- `test_insights_merge_stops_within_m_reintroduces_week_long_stop`
- `test_insights_merge_stops_within_m_stop_duration_disagrees_with_hotspot_in_same_report`
- `test_compare_per_track_merge_stops_within_m_reintroduces_gap_bug`

## Known traps checked, not found to be defects

- **Off-by-one at the exact gap threshold.** Verified via direct
  `detect_stops`/`detect_hotspots` calls and through `/v1/insights` with
  `trip_split_gap_minutes` set to a clean value and the sample gap swept
  across the boundary (`59.999999s` / `60.0s` / `60.000001s` of *actual*
  elapsed time, confirmed via `total_seconds()` on the literal timestamps
  used, not assumed): a gap `< threshold` never splits, a gap `>= threshold`
  always splits, consistently between `stops.py` and `hotspots.py`. Matches
  the ADR's documented `>=` semantics exactly. (An earlier version of this
  probe had an off-by-one in the *test harness's* own gap arithmetic, not
  the product code — resolved and re-verified against the literal
  timestamps before concluding "clean".)
- **Reordering metamorphic property.** Submitting the two-visits-a-week-apart
  fixture in shuffled order to `/v1/insights` produces byte-identical
  `stops`/`hotspots` output to the chronologically-ordered submission —
  `prepare_track`'s global time-sort is applied consistently before
  `detect_stops`/`detect_hotspots` in every caller (`insights.py`,
  `compare.py`), so no caller accidentally relies on input order.
- **`/v1/compare` pooling double-counting.** Pooled samples across tracks
  are globally time-sorted via `prepare_track` before hotspot detection, so
  interleaved-in-time samples from different tracks at the same location
  are treated the same way genuinely-continuous same-track samples would be
  (arguably correct: overlapping-time visits to the same place are one
  continuous window regardless of which physical track recorded them).
  Exact-duplicate points (same lat/lon-to-7-decimals and same timestamp)
  across two tracks do collapse to one during `prepare_track`'s dedup, but
  this is the same dedup contract every other caller of `prepare_track`
  already relies on, not something new from the gap-awareness fix.

## Coverage note

`stops.py` adversary coverage went from 83% to 97% this cycle (the
`merge_nearby_stops` body, previously fully unprobed, is now exercised).
Remaining unprobed lines (116, 147, 225) are: the "exact recompute finds a
genuine spatial break" branch of the pre-existing (cycle-2) quadratic-safe
`_cluster_end`, the `n < 2` early return, and `merge_nearby_stops`'
non-merge `else` branch — none reached in this cycle's probing; flagging
for the next coverage-gaps pass rather than a defect claim, since no wrong
answer was observed there.
