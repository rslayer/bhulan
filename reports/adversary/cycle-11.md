# Adversary cycle 11 — truthful merged-stop centroid + radius_m (cycle 8 fix)

Scope: attack cycle 8's fix (spec/spec.md, "Make a merged stop's reported
centroid and `radius_m` truthful for a chain of 3+ stops") — probe the
spec's "known traps" first, then edge inputs around the fix. Product code
under `bhulan/` treated as read-only.

Ran the two acceptance tests
(`test_merge_nearby_stops_chain_centroid_radius_wrong.py`,
`test_merge_nearby_stops_chain_drift_undermerges.py`) first — both green,
confirming cycle 8's headline fix (member-accumulated centroid + enclosing
`max(dist + member.radius)` radius) does what it claims for the *specific*
equal-weight, 6-samples-each A/B/C scenario the spec's own example uses.
Then attacked the two levers cycle 8 introduces that the acceptance test
doesn't vary: (1) `sample_count`-weighted `lat` mixed with *unweighted*
`lon`, and (2) the new `_merge_members` recompute-on-every-append pattern
inside the merge loop.

## New defects found (failing tests added)

### 1. Weighted-lat / unweighted-lon mismatch breaks AC1 for realistic unequal-weight chains

`tests/adversary/test_merge_nearby_stops_weighted_lat_unweighted_lon_breaks_ac1.py`

Spec section 2 explicitly permits `lon` to stay an *unweighted*
`circular_mean_lon` while `lat` becomes the `sample_count`-weighted mean
("weight by `sample_count` if practical, otherwise an unweighted circular
mean is acceptable"). The acceptance test only exercises three *equal*-
weight members (6 samples each), where weighted-lat and unweighted-lon
happen to agree closely enough that AC1 ("the reported centroid is within
`merge_radius_m` of every member A, B, and C") holds. It was never
exercised with unequal member weights — an entirely ordinary situation: a
dense, long dwell (many GPS samples over several minutes, e.g. a phone
reporting once a second while parked) sandwiched between two brief stops
(a couple of samples each), all still pairwise within
`merge_stops_within_m` of their immediate predecessor, so cycle 7's
merge-decision logic folds them into one stop exactly as it's supposed to.

Concretely: A (2 samples) → B (300 samples, ~40m from A) → C (2 samples,
~40m from B), `merge_stops_within_m=45`. The chain correctly merges into
one stop (AC3 holds — decision unchanged), but the weighted `lat` mean
snaps close to B's latitude while the *unweighted* `lon` mean stays near
the simple average of A/B/C's longitudes, dragging the reported centroid
off the true A-B-C line. Verified end-to-end via `POST /v1/insights`: the
reported centroid lands **~48.8m from C** — outside `merge_radius_m=45` —
even though every consecutive pairwise merge-decision distance was inside
the 45m radius, and this is exactly the 3-member A,B,C case AC1 is written
for.

(AC2 is *not* violated by this same input: `radius_m` is derived via
`max(haversine(centroid, member) + member.radius_m)`, which bounds the true
spread by the triangle inequality regardless of how the centroid itself
drifts — a genuinely sound piece of the cycle-8 fix, confirmed by direct
inspection and by every test run in this cycle. This finding is specifically
an AC1 defect: the reported *location* itself walks outside the merge
radius of a member it claims to summarize, not an understated radius.)

### 2. `_merge_members`'s per-append recompute makes long merge chains O(n^2)

`tests/adversary/test_merge_nearby_stops_chain_quadratic_blowup.py`

Cycle 8's fix replaces the old O(1)-per-step pairwise-midpoint recentring
with member-accumulation: on every stop folded into a blob, the merge loop
does `groups[-1].append(s); merged[-1] = _merge_members(groups[-1])`, and
`_merge_members` iterates *every* member accumulated so far (`sum(...)` for
the weighted lat, `circular_mean_lon(...)`, and a `max(...)` generator for
`radius_m` — each O(k) for a blob of k members). For a chain of n
consecutive stops that all merge into one blob (an ordinary slowly-drifting
GPS trail: each detected stop lands just outside `stop_radius_m` of the
last but well inside `merge_stops_within_m`), total work across the chain
is `1 + 2 + ... + n` = O(n^2), not O(n).

Measured directly against `merge_nearby_stops` (no HTTP/`detect_stops`
overhead): 1,000 chained stops ~0.44s, 2,000 ~1.72s, 4,000 ~6.78s, 8,000
~26.8s — each doubling of n roughly quadruples the time, the textbook
signature of O(n^2). End-to-end through `POST /v1/insights` reproduces the
same growth on an ordinary payload: 500 merging stops ~0.17s, 1,000 ~0.54s,
2,000 ~1.9s (well under the service's own `MAX_POINTS=100,000` cap, and
using the documented, public `merge_stops_within_m` option). This is a new
regression introduced by this cycle's fix, not a re-walk of the existing
`detect_stops` quadratic-blowup finding (`test_stop_detection_quadratic_blowup.py`
targets a different function with a different, already-fixed-then-reopened
root cause) — an unauthenticated caller can tie up a worker process for
seconds with a single ordinary-looking request.

## Verified as fixed / not a bug

- **AC1/AC2/AC3/AC4** for the acceptance test's own equal-weight,
  6-samples-each A/B/C scenario: confirmed green.
- **AC2 in general**: re-derived independently — `radius_m =
  max(haversine_m(centroid, m) + m.radius_m for m in members)` is correct
  by the triangle inequality for *any* centroid, so no amount of centroid
  drift (including the AC1 bug above, or synthetic extreme weight ratios up
  to 2000:1:1, or antimeridian-straddling chains) was able to make
  `radius_m` understate the true spread in any trial run.
- **Two-stop merge (AC4)**: full unit + adversary suite green
  (`test_merge_nearby_stops_radius_m_wrong.py` and friends), confirming the
  equal-weight two-member path is unchanged.
- **Antimeridian-straddling merge chains**: spot-checked a chain crossing
  ±180° with unequal weights (2/2000/2 samples) — `circular_mean_lon`
  folds correctly and AC2 held; the only defect class found is #1 above
  (weighted-lat/unweighted-lon divergence), not anything antimeridian-
  specific.
- **Known trap: transitive-merge span cap (spec section 4)** — deliberately
  out of scope this cycle per the spec ("left for the owner to decide");
  not re-litigated as a bug here since the spec explicitly defers it and
  AC1's literal wording only requires the exact 3-member A,B,C case,
  a case now genuinely broken by defect #1 above (which does not
  need a longer chain or a span-cap decision to reproduce).
- **Known trap: polar dwell false-negative** — pre-existing, already
  tracked by `test_pole_dwell_stop_false_negative.py` (cycle 7, unrelated
  to this cycle's fix); still red, not re-investigated.
- **Known traps: deeply-nested-JSON recursion, non-finite `speed_mps`,
  datetime overflow** — all already covered by existing adversary tests,
  not re-walked per the task's instruction not to re-probe already-fixed
  NaN/overflow/type-coercion territory.

## Full adversary + unit suite after this cycle's additions

`182 passed, 3 failed` (`test_pole_dwell_stop_false_negative.py` — pre-
existing, unrelated to this cycle; the two new tests above). No existing
test was weakened or modified.

## Not investigated further (time-boxed out)

Per the coverage-guided targeting note, `bhulan/analytics/parsers.py`,
`bhulan/ingestion/*`, `bhulan/analytics/geocoding.py`, and
`bhulan/auth/*` remain at low/0% adversary coverage, but are unrelated to
this cycle's merge-geometry feature and were left for a future cycle scoped
to those subsystems.
