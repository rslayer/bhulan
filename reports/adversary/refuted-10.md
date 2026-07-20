# Refuted findings — cycle 10

Scope: the two new adversary test files added this cycle (confirmed via
`git status` — everything else under `tests/adversary/` was already
committed prior to this pass and is out of scope for this refuter run):

- `tests/adversary/test_merge_nearby_stops_chain_centroid_radius_wrong.py`
  (2 cases)
- `tests/adversary/test_merge_nearby_stops_runaway_chain_span.py` (2 cases)

All 4 cases were independently re-verified against `bhulan/analytics/stops.py`
(`merge_nearby_stops`), spec/spec.md, and ADR 0007. **None refuted — all 4
kept as real defects.**

## Verification

`merge_nearby_stops` decides *membership* against `prev_original` (the
immediately preceding original stop — this is ADR 0007's cycle-7 fix, and it
is correct), but *recentres* and sizes `radius_m` against the running blob:

```python
lat = (blob.lat + s.lat) / 2.0                                  # unweighted midpoint of blob, s
...
radius_m=centroid_dist_m / 2.0 + max(blob.radius_m, s.radius_m) # centroid_dist_m = dist(prev_original, s)
```

For a 3rd+ merge in a chain, `blob` has already drifted away from
`prev_original`'s position, so `centroid_dist_m` (`dist(prev_original, s)`)
is no longer the distance the recentring step actually moves — `dist(blob,
s)` is. I hand-traced the exact 3-stop A/B/C scenario from
`test_merge_nearby_stops_chain_centroid_radius_wrong.py` (A=0m, B=40m,
C=80m along a meridian, `merge_radius_m=45`):

- Merge 1 (A,B): blob centroid = 20m from A, `radius_m = dist(A,B)/2 = 20m`.
- Merge 2 (blob,C): `centroid_dist_m = dist(B,C) = 40m` (used for both the
  membership test *and*, wrongly, the recentring-displacement term). New
  centroid = midpoint(blob@20m, C@80m) = **50m** from A. Reported
  `radius_m = 40/2 + max(20,~0) = 40m`.
- True distances from the reported (50m) centroid: A=50m, B=10m, C=30m.

This matches the tests' observed output exactly (`A=50.0m B=10.0m C=30.0m`,
`radius_m=40.03` vs. true max 50.04m). Two independent, spec-relevant
violations fall out of this:

1. **AC2 violation** (spec/spec.md §3, explicitly "non-negotiable" for this
   cycle): "its centroid lies within the cluster (within ~`merge_radius_m`
   of each member)." A is 50m from the reported centroid against a
   configured `merge_radius_m` of 45m — outside. ADR 0007's own
   "Consequences" section *claims* AC2 is satisfied by the cycle-7 fix, but
   that claim was never actually checked — the sole acceptance test
   (`test_merge_nearby_stops_chain_drift_undermerges.py`) asserts only
   `len(stops) == 1`, never the centroid/radius clause. The claim is false
   for any chain of 3+ merges; the new tests are the first to check it.
2. **`radius_m` no longer bounds true spread.** The field exists (per the
   cycle-6/7 fix history referenced in `stops.py`'s own comments) precisely
   so a caller can sanity-check how spread out a reported stop really is.
   Reported 40.0m vs. true 50.0m understates the spread by ~20% here, and
   `test_runaway_chain_radius_m_understates_true_spread`'s 6-stop chain
   shows the error compounding with chain length (87.4m reported vs. 140.9m
   true — a 38% understatement). Root cause is identical in both cases:
   sizing the displacement term from `dist(prev_original, s)` instead of
   `dist(blob, s)`.

`test_runaway_chain_span_vastly_exceeds_merge_radius_with_no_guard` is
independently grounded in spec/spec.md §4's own backlog item ("Runaway chain
over-merge... does an unbounded chain... collapse into one implausibly wide
'stop'? Probe whether a span cap or total-extent guard is warranted") and
ADR 0007's "Follow-up (backlog)" section, both written by this project's own
spec authors as an open question to probe next cycle — not settled,
acceptable behavior. The test's fixture reproduces exactly that scenario (6
co-linear stops, each pairwise within `merge_radius_m=45`, no time gap) and
the measured 174.8m true end-to-end span against a 45m configured radius is
a real, reproducible fact about the current code (confirmed by direct
re-run), not a fixture error. The `<= merge_radius_m * 2` threshold in the
assertion is the test author's chosen bar for "a caller could reasonably
infer this from `merge_stops_within_m` alone," not a number pulled from
spec — but the underlying defect it's built on (no span/extent guard exists
at all, chain length is unbounded) is undisputed and directly on-topic for
the exact question the spec asks the adversary to probe. Kept per the
calibration rule: in doubt, keep it — severity/threshold-tuning is triage's
job, not the refuter's.

## Disposition

No tests deleted this pass. All 4 cases in both new files left untouched in
`tests/adversary/`.
