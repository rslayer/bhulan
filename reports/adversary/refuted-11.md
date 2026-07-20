# Refuted findings — cycle 11

Scope: the two new adversary test files added this cycle (confirmed via
`git status` — everything else under `tests/adversary/` was already
committed prior to this pass and is out of scope for this refuter run):

- `tests/adversary/test_merge_nearby_stops_weighted_lat_unweighted_lon_breaks_ac1.py`
- `tests/adversary/test_merge_nearby_stops_chain_quadratic_blowup.py`

(The other two currently-failing tests in `tests/adversary/` —
`test_stop_detection_quadratic_blowup.py` and
`test_pole_dwell_stop_false_negative.py` — are pre-existing findings from
earlier cycles, unrelated to this cycle's merge-geometry fix, and out of
scope for this pass.)

Both new cases were independently re-verified against
`bhulan/analytics/stops.py`, `spec/spec.md` (§2, §3), and by direct
reproduction. **None refuted — both kept as real defects.**

## 1. `test_merge_nearby_stops_weighted_lat_unweighted_lon_breaks_ac1.py`

Independently recomputed the A/B/C pairwise haversine distances from the
test's own fixture coordinates using `bhulan.analytics.geodesy.haversine_m`:
A-B = 44.87m, B-C = 44.85m — both legitimately `<= merge_radius_m=45.0`, so
the merge-decision premise (cycle 7's single-linkage against
`prev_original`) is satisfied honestly by the fixture, not smuggled in via
an out-of-range setup.

Re-ran the test directly: `report["stops"]` has exactly one merged stop
(AC3 intact), and the reported centroid is 37.0m from A, 7.9m from B, and
48.8m from C — outside `merge_radius_m=45.0` for member C. This is a
direct violation of AC1 (spec/spec.md §3: "for a merged chain A,B,C, the
reported centroid is within `merge_radius_m` ... of every member A, B, and
C"), which the spec explicitly calls a "non-negotiable acceptance
criterion," not a nice-to-have.

The root cause traces cleanly to `_merge_members`
(`bhulan/analytics/stops.py:266-304`): `lat` is the `sample_count`-weighted
mean of member lats, but `lon` is the *unweighted* `circular_mean_lon` —
exactly the combination spec/spec.md §2 explicitly permits ("weight by
`sample_count` if practical, otherwise an unweighted circular mean is
acceptable" for `lon`). The acceptance test cycle 8 shipped with only
exercises three *equal*-weight members, where weighted-lat and
unweighted-lon happen to agree closely enough that AC1 holds by
coincidence, not by construction — it never varies `sample_count` across
members. This new test is the first to vary member weights, and it shows
the spec's own permitted implementation choice can violate the spec's own
non-negotiable AC1 for an entirely ordinary input (a long dense dwell
sandwiched between two brief stops — nothing contrived about the sample
counts or spacing). Kept: this is a genuine internal inconsistency between
spec/spec.md §2's permitted `lon` handling and §3's AC1, manifesting as a
real, reproducible wrong-answer in the shipped code, not a test preference
or an unreachable input.

## 2. `test_merge_nearby_stops_chain_quadratic_blowup.py`

Confirmed by direct code inspection that `merge_nearby_stops`
(`bhulan/analytics/stops.py:187-263`) calls `_merge_members(groups[-1])` on
*every* stop folded into a growing blob, and `_merge_members` (lines
266-304) does a `sum(...)`, a `circular_mean_lon(...)`, and a `max(...)`
generator over *all* members accumulated so far — each O(k) for a blob of
k members. For a chain of n stops that all merge into one blob, total work
is `1 + 2 + ... + n` = O(n^2), a genuine algorithmic regression from cycle
7's O(1)-per-step pairwise-midpoint update (which this cycle's fix
deliberately replaced per spec/spec.md §2's "track the member stops ...
and compute, when emitting the merged stop" instruction — the spec
mandates member-accumulation but doesn't address doing so incrementally).

Re-ran the test directly: a 2,000-stop drift chain (well under
`MAX_POINTS=100,000`, using the documented public `merge_stops_within_m`
option) took ~8.7s end-to-end through `/v1/insights`, ~17x the 0.5s bound,
consistent with the quadratic growth the test's docstring measures
independently (1,000→0.44s, 2,000→1.72s, 4,000→6.78s, 8,000→26.8s against
`merge_nearby_stops` directly — each doubling roughly quadruples runtime,
the textbook O(n^2) signature). This targets a different function
(`merge_nearby_stops`/`_merge_members`) than the pre-existing
`test_stop_detection_quadratic_blowup.py` (`detect_stops`/`_cluster_end`,
which already has an explicit O(n) running-bound optimization documented
in the module docstring) — not a duplicate finding. Kept: a real,
independently-reproduced CPU-exhaustion regression newly introduced by
this cycle's `_merge_members` accumulator, reachable unauthenticated with
an ordinary-looking payload.

## Disposition

No tests deleted this pass. Both cases in both new files left untouched in
`tests/adversary/`.
