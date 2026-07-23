# Refuted findings — cycle 16

Scope: the single failing test under `poetry run pytest tests/adversary/ -q`
at the start of this pass —
`tests/adversary/test_merge_nearby_stops_anchor_cap_radius_still_balloons.py::test_merged_stop_radius_m_balloons_far_beyond_stop_radius_m`.
Re-verified in isolation, hand-checked the weighted-centroid arithmetic, and
read `merge_nearby_stops`/`_merge_members` in `bhulan/analytics/stops.py`,
`spec/adrs/0013-stop-radius-caps-the-merge.md`, and `spec/spec.md` §§1–3.

**0 of 1 failing case refuted. 1 kept as a real defect.**

## Kept

### `test_merge_nearby_stops_anchor_cap_radius_still_balloons.py` (1 case)

Claims the cycle-16 anchor cap in `merge_nearby_stops`
(`bhulan/analytics/stops.py:415-423`) bounds each member's distance to the
group's *first* member ("the anchor") but not the distance from the
*reported centroid* to each member, so an asymmetric group (ten dwells ~49m
east of the anchor, one dwell ~49m west, each individually admissible under
the ≤`stop_radius_m` anchor check) still reports a merged `radius_m` far past
`stop_radius_m`.

Confirmed real, not a false positive:

- Read the implementation directly: the cap at line ~419 is
  `haversine_m(anchor.lat, anchor.lon, s.lat, s.lon) <= stop_radius_m` —
  anchor-to-member only. `_merge_members` (line ~452) then computes
  `radius_m` from the *sample_count-weighted centroid*, an entirely different
  reference point. Nothing in the loop constrains centroid-to-member
  distance; the geometry is only guaranteed to fit inside the anchor's
  `stop_radius_m` disk, whose diameter is `2*stop_radius_m` — the "genuine
  cluster" claim was never actually enforced for asymmetric weight
  distributions.
- Hand-verified the arithmetic: 10 members at +49m (weight 7 each), 1 at 0m
  (weight 7), 1 at -49m (weight 7) → weighted mean at
  `(10*49 + 0 - 49)/12 = 36.75m` east of the anchor; distance from that
  centroid to the -49m member is `36.75 + 49 = 85.75m` (haversine gives
  85.65m on the actual ellipsoidal-ish offsets used) — matches the observed
  failure (`radius_m=85.65` vs `stop_radius_m=50`, 71% over) to the meter.
  Not a fixture bug or a flaky assertion.
- Matches spec's own non-negotiable **AC1** (`spec/spec.md` line 76-77)
  verbatim: "each reported centroid lies within `stop_radius_m` of its own
  members." 85.65m is not within 50m of a member 49m from the anchor on the
  far side. AC1 is violated on disk today, not just the ADR's softer "small
  numeric margin" language (test already grants 20% slack over the ADR's
  wording; the actual overshoot is 71%, nowhere near "small").
  ADR 0013's own "Consequences" section makes the identical claim: "Every
  reported merged stop is a genuine cluster: `radius_m ≤ stop_radius_m`
  (allowing a small numeric margin)."
- The scenario is ordinary and reachable exactly as the test describes: a
  frequently-visited anchor spot plus lopsided repeat-visit counts to two
  jitter clusters on opposite sides of it is not a contrived adversarial
  shape, it's what real "one popular spot dominates a merged group" GPS
  tracks look like. Reachable unauthenticated via one documented
  `POST /v1/insights` call using only public `InsightsOptions` fields
  (`stop_radius_m`, `merge_stops_within_m`). Each dwell is 7 samples over
  360s (over the 300s default `min_stop_minutes`), so `detect_stops` reports
  each as its own tight, real stop before merging — nothing here depends on
  a detect_stops quirk.
- Note for triage, not grounds to kill: spec.md's own "Hard constraints"
  section (the same doc that states AC1) mandates the O(1) anchor-only check
  for performance reasons and explicitly says it "bounds the group span to ≤
  `2*stop_radius_m`" — i.e., the spec's own implementation directive and its
  own AC1 are in tension (an anchor-radius disk of diameter `2*stop_radius_m`
  cannot generally guarantee a weighted centroid stays within `stop_radius_m`
  of every member). That tension is a design/spec question for a human to
  resolve (e.g., relax AC1's wording, or reject/clip outlier members, or
  recompute against a running centroid at the cost of the O(n) guarantee) —
  it does not make the test's factual claim about current behavior wrong.
  The code today does not deliver what its own docstring, its own ADR, and
  spec's own AC1 all claim it delivers. Left untouched.

## Disposition

- No tests deleted this pass.
- The one failing test in `tests/adversary/` is left untouched.
