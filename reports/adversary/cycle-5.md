# Adversary cycle 5 — `merge_nearby_stops` gap-aware fix

Scope: attack this cycle's fix (spec/spec.md, "Make `merge_nearby_stops`
gap-aware") — probe the spec's "known traps" first, then edge inputs around
the fix. Product code under `bhulan/` treated as read-only.

## Verified as fixed / not a bug

Ran the existing acceptance tests
(`test_merge_nearby_stops_reintroduces_time_gap_bug.py`,
`test_stop_and_hotspot_ignore_time_gaps.py`) — both green. Manually verified
the specific "known trap" about **temporal** chained merges (spec section 4,
first bullet): three stops A-B-C where A-B are close in time (merge) but B-C
span a real gap. Confirmed via direct calls to `merge_nearby_stops` and via
`/v1/insights` that the gap between B and C is correctly detected even
though B was already folded into a merged blob — `prev.end_ts` is updated to
the last real sample's timestamp on every merge, so the gap check against
the next stop stays correct. **No defect found here.**

## New defects found (failing tests added)

### 1. `merge_nearby_stops` reports a wrong `radius_m` after a legitimate merge

`tests/adversary/test_merge_nearby_stops_radius_m_wrong.py`

`bhulan/analytics/stops.py::merge_nearby_stops` recentres a merged stop to
the unweighted midpoint of the two original centroids
(`lat=(prev.lat+s.lat)/2.0`) but reports
`radius_m=max(prev.radius_m, s.radius_m)` — the spread *around the
discarded, pre-merge centroids*, not around the new one. Two tight clusters
(true radius ~0) sitting ~40m apart, close enough in time and space to
legitimately merge (`merge_stops_within_m=60`), merge into one stop whose
reported `radius_m` is `0.0` — while every underlying sample is actually
~20m from the reported centroid. The stop was originally detected with
`stop_radius_m=5`, so the merged report claims a radius 4x tighter than the
true spread of its own samples. This is exactly the trap named in
spec/spec.md section 4: "Whether `merge_nearby_stops` recomputes
`radius_m`/`sample_count` correctly after a legitimate merge (not just
duration)." — `radius_m` does not; `sample_count` does (verified correct,
simple sum). This code path predates this cycle's diff (the `radius_m` line
is untouched by the gap-awareness fix) but was never exercised by any
adversary test before now.

### 2. Chained merges silently under-merge due to centroid drift

`tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py`

`merge_nearby_stops`'s docstring says it merges "consecutive stops whose
centroids are within `merge_radius_m`." The loop, however, only ever
compares an incoming stop against `merged[-1]` — the *running merged
blob's* recomputed (unweighted-average) centroid — not against the
original stop that immediately preceded it. For a chain of 3+ stops each
~40m from their immediate neighbour (A-B = 40m, B-C = 40m,
`merge_radius_m=45`, no time gaps anywhere), A and B merge into a blob
centred 20m from each; C is then compared against that *drifted* 20m-off
centroid, putting it 60m away — outside `merge_radius_m` — even though the
real B-C distance (40m) is well within it. Result: 2 stops instead of the
1 a plain reading of the docstring promises, and the outcome depends on
processing order/chain length rather than on the pairwise distances
between the actual stops. Realistic for a walk along a building's edge:
some genuinely-jitter-split stops merge, others silently don't, purely as
an artifact of which end of the chain the algorithm started scanning from.
Also predates this cycle's diff; newly exercised.

## Coverage-guided notes

Both new findings live in `bhulan/analytics/stops.py`, previously reached
at only ~95% by the existing suite (lines 199-213 flagged unprobed in an
earlier report) — the merge path specifically was under-tested before this
cycle's own acceptance tests started exercising it. Did not find additional
defects in `bhulan/analytics/hotspots.py`'s `split_gap_s` threading, in
`/v1/compare`'s `shared_hotspots` pooling (uses raw pooled samples +
`detect_hotspots` directly, unaffected by the stops-merge bugs above since
hotspots have no merge step), or in `trips.py`'s consumption of merged
stops (trip splitting uses `stop.end_index`/`stop.duration_s`, both correct
post-fix; `_segment_kmh_window` computes duration from real per-sample
deltas within the trip's own index range, not from a stop's span).

## Test files added

- `tests/adversary/test_merge_nearby_stops_radius_m_wrong.py` (failing)
- `tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py` (failing)

Full adversary suite: 3 failed (2 new + the pre-existing backlog KML
quadratic test, which was already failing and out of scope for this cycle),
30 passed. No existing test was weakened, rewritten, or deleted.
