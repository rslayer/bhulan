# ADR 0013 — The stop radius caps the merge: a merged stop is one place

**Status:** accepted (cockpit decision, owner-confirmed)
**Date:** 2026-07-21
**Supersedes/extends:** [[0007]] (transitive merge), [[0008]] (merged geometry),
[[0011]] (linear merge + weighted longitude)

## Context

A *stop* is one place — points clustered around a common centre, all within
`stop_radius_m` of it. That is exactly how `detect_stops` decides a stop.

Cycles 7–9 made `merge_nearby_stops` transitive (single-linkage over consecutive
stops) to reunite GPS-jitter fragments of one dwell, but the merge was
**unbounded**: a chain of stops each within `merge_stops_within_m` of the next
would collapse into one stop no matter how far the chain wandered end to end
(e.g. 6 stops 40 m apart → a ~175 m "stop"). That is a slow walk or drift, not a
dwell. The `sample_count`-weighted centroid then sat far from the chain's ends
and `radius_m` ballooned — a single reported "stop" that is really movement.

Two adversary findings (`heavy_dwell…`, `runaway_chain…`) demonstrated this, but
they asserted `len(stops) == 1` **and** a bounded centroid simultaneously —
impossible for a chain spanning more than ~2× the radius. They were design flags,
not greenable tests.

## Decision

**The stop radius bounds the merge.** A stop may join the current merged group
only while it stays within `stop_radius_m` of the group's fixed **anchor** (its
first member's centroid). When the next stop would fall beyond one stop's radius
of the anchor — the chain has walked past one place — it starts a **new group**
instead of extending an ever-widening blob.

- The cap is tied to **`stop_radius_m`**, the caller's own definition of how big
  one stop is (threaded from `InsightsOptions.stop_radius_m`, the same value
  `detect_stops` uses) — not to a multiple of `merge_stops_within_m`. A
  `merge_stops_within_m` looser than `stop_radius_m` therefore cannot produce a
  stop wider than one stop: **the stop radius wins.** `merge_stops_within_m`
  still gates whether two neighbours are close enough to consider at all (and
  whether merging is enabled).
- The anchor is the group's **first** member and never moves, so "every member
  within `stop_radius_m` of the anchor" is an exact O(1)-per-stop guarantee — no
  per-member recompute against a drifting centroid, which would reintroduce the
  cycle-9 O(n²) blow-up. The check is a single `haversine_m` comparison.
- The reported geometry is unchanged from [[0008]]/[[0011]]: the
  `sample_count`-weighted, circular-in-longitude centroid of the members
  actually in the (now bounded) group, and the true enclosing `radius_m`.

## Consequences

- A progressive walk/drive no longer masquerades as one wide stop; it splits into
  bounded stops (or, once `detect_stops` gains progressive-movement rejection —
  the next cycle — is not reported as stops at all).
- Every reported merged stop is a genuine cluster: `radius_m ≤ stop_radius_m`
  (allowing a small numeric margin), so a downstream "was the vehicle parked
  here" consumer can trust the location and spread.
- **Byte-identical when the cap never trips.** For the common case (jitter
  fragments all within `stop_radius_m` of the anchor) the added condition is a
  provable no-op; verified 0-diff over 2000 random within-radius merges.
- **Behaviour change for `merge_stops_within_m > stop_radius_m`.** Stops farther
  apart than one stop radius no longer merge even if within the merge distance.
  Adversary tests that previously set `stop_radius_m = 5` with
  `merge_stops_within_m = 45` and asserted an 80–175 m chain fully merges were
  re-targeted: the pure-geometry tests now use a `stop_radius_m` that legitimately
  contains their chain (so they still verify single-linkage / centroid / radius),
  and `heavy_dwell` was reframed to assert the cap keeps every stop bounded.
