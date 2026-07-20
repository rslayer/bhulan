# ADR 0006 — Largest-gap minimal bounding box

**Status:** Accepted
**Cycle:** 6
**Date:** 2026-07-20
**Supersedes:** the two-candidate `bounding_box` longitude heuristic in
[ADR 0004](0004-antimeridian-projection-and-bbox.md) §2. (ADR 0004's
`latlon_to_xy_m` projection decision is unchanged.)

## Context

ADR 0004 made `geodesy.py::bounding_box` antimeridian-aware with a
**two-candidate** longitude heuristic: it compared the raw `[min, max]` framing
(a cut at ±180°) against a single `[0, 360)`-shifted framing (a cut at 0°) and
kept the tighter. That correctly tightens a single cluster straddling ±180°, but
it only ever inspects **two** of the infinitely many circular cuts.

The true minimal-longitude-span box is the complement of the **single largest
angular gap** between adjacent longitudes, and that gap can fall anywhere on the
circle — not just near 0° or ±180°. For a track visiting 3+ genuinely
spread-out longitudes whose largest gap sits elsewhere, both of the heuristic's
candidate cuts miss it and it reports a box far wider than minimal, while its
docstring still promises "the minimal box."

Concretely, six points at longitudes `-170, -100, -30, 0, 60, 170`: the true
minimal box cuts at the largest gap (between 60 and 170, a 110° gap) and spans
250° from 170 east to 60. The two-candidate heuristic instead reported a 330°
box running from 0 to −30 the long way through ±180° — ~80° (~8,900 km at the
equator) wider than minimal, and mislabelled the crossing location.

Found by `tests/adversary/test_bounding_box_not_minimal_multi_cluster.py`.

## Decision

Replace the two-candidate longitude heuristic with the **largest-gap**
algorithm, at the root in `geodesy.py::bounding_box`:

1. Latitude is unaffected — plain `min`/`max`.
2. Sort the unique longitudes ascending.
3. Compute every interior gap (`diff` of adjacent longitudes) and the wraparound
   gap that closes the circle from the max back to the min: `min + 360 − max`.
4. The **largest** gap is the arc the track does not occupy. The minimal box is
   its complement:
   - **Wraparound gap largest (or tied):** the box is the naive `[min, max]`
     with `min_lon <= max_lon`.
   - **An interior gap (between `uniq[i]` and `uniq[i+1]`) largest:** the box
     runs east from `uniq[i+1]`, over ±180°, back to `uniq[i]`, encoded as
     `min_lon = uniq[i+1]`, `max_lon = uniq[i]`. Because `uniq[i+1] > uniq[i]`,
     this yields `min_lon > max_lon` — the ADR 0004 antimeridian-crossing
     encoding, reused unchanged.

Ties resolve to the wraparound gap (see byte-identity below). A single distinct
longitude falls through to the naive framing.

The `min_lon > max_lon` = crosses-±180° **consumer contract** from ADR 0004 is
kept verbatim; only the set of cut positions considered is generalised.

## Consequences

- **AC1** — for 3+ separated longitude clusters, `bounding_box` returns the
  minimal-span box (the complement of the largest angular gap), not a wider
  superset.
- **AC2** — `/v1/insights` `summary.bbox` reflects the same minimal box (it
  serialises `bounding_box`'s output directly).
- **AC3** — the cycle-4 single-cluster straddle is subsumed: two points across
  ±180° have exactly one interior gap and a tiny wrap gap, so the interior cut
  wins and the tight crossing box is reported as before.
- **AC4 — byte-identity.** For any track inside a <180° longitude window that
  does not cross ±180°, the wraparound gap is `360 − span > 180°` while every
  interior gap is `< span < 180°`, so the wraparound gap strictly dominates and
  the naive `min/max` (the exact same floats as a plain `np.min`/`np.max`) is
  returned. Non-wraparound output is byte-identical to before.
- Backend/analytics only: no new dependencies, no DB/schema change, no API field
  change. `haversine_m`, `haversine_vec_m`, `latlon_to_xy_m`, and
  `circular_mean_lon` are untouched.

## Alternatives considered

- **Keep the two-candidate heuristic** (ADR 0004). Rejected: it inspects only
  two of the circle's cuts, so it is not minimal for 3+ spread-out longitudes —
  the defect this cycle fixes.
- **Brute-force every rotation.** Unnecessary: the minimal box is always the
  complement of *one* gap — the largest — so a single sort + `diff` + `argmax`
  is exact and `O(n log n)`.
