# Refuted findings — cycle 9

Reviewed all tests failing under `poetry run pytest tests/adversary/ -q` at
the start of this pass: 2 failing, 41 passing.

## Refuted (deleted)

None. Both failures were independently re-verified as real, already-known
defects and left untouched.

## Kept (not refuted)

### `tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py::test_chain_of_pairwise_close_stops_fully_merges`

Confirmed real. `merge_nearby_stops` (`bhulan/analytics/stops.py`) compares
each incoming stop against the *previous merge result's drifted centroid*
(`prev = merged[-1]`) rather than the original preceding stop. For A/B/C each
40m from their immediate neighbour with `merge_radius_m=45`, A merges with B
into a blob centred 20m from both, then C is compared against that drifted
blob (60m away, outside the radius) instead of against B (40m away, inside
the radius) — producing 2 stops instead of 1 even though every adjacent
original pair satisfies the documented `merge_radius_m` contract. This is the
same defect independently verified in `reports/adversary/refuted-5.md` and
`reports/adversary/refuted-7.md`, explicitly named as an open backlog item in
`spec/spec.md` §4 ("merge_nearby_stops chain-drift"), and still unfixed —
`stops.py`'s merge loop is unchanged from those prior assessments.

### `tests/adversary/test_pole_dwell_stop_false_negative.py::test_polar_dwell_within_true_radius_is_detected_as_a_stop`

Confirmed real. Independently recomputed: `haversine_m(89.9999, 0, 89.9999,
180)` = 22.24m true separation (11.12m true cluster radius) vs.
`latlon_to_xy_m`'s (`bhulan/analytics/geodesy.py`) projected spread of
~17.49m for the same points — the linear `x = dlon * (111_320 *
cos(lat0))` tangent-plane projection breaks down near the poles because
longitude is nearly degenerate there (two points a few meters apart can
differ by up to 180° of longitude). The inflated projected radius exceeds
the test's `stop_radius_m=15`, so `detect_stops` reports 0 stops for a dwell
that is, in true physical terms, well within the requested radius. Same
defect verified in `reports/adversary/refuted-7.md`, explicitly named in
`spec/spec.md` §4 ("Polar dwell false-negative") as a known trap not yet
addressed — `latlon_to_xy_m` is unchanged from that assessment.

Both items are pre-existing, spec-acknowledged backlog defects rather than
false positives introduced or newly probed this cycle; nothing here
warranted refutation.
