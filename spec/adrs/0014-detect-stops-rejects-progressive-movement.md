# ADR 0014 — A stop is a dwell, not a moving window: reject progressive translation

**Status:** accepted (cockpit decision, owner-requested)
**Date:** 2026-07-21
**Related:** [[0013]] (the merge cap — a *merged* stop is one place)

## Context

`detect_stops` grows a cluster while its centroid spread stays within
`stop_radius_m`, then reports it as a stop if it lasts `min_duration_s`. That
tests *how spread out* the points are, but not *whether they are centred on a
common spot*. A slow walk or crawl that lasts longer than the minimum duration
is therefore chopped into `stop_radius_m`-sized chunks and **each chunk is
reported as a phantom stop** — a steady ~190 m walk becomes 2 "stops". A stop
should be one *place* (points clustered around a common centre), and continuous
progressive movement is a walk or drive, not a stop.

(The fast-movement case was already filtered — a quick drive doesn't last
`min_duration_s` within one radius — so only *slow, sustained* movement leaks
through.)

## Decision

A cluster that passes the spread + duration gates is reported as a stop **only if
it is not progressively translating.** Movement is detected by the drift between
the centroid of the cluster's first (in time) half and that of its second half:

- A genuine **dwell**'s halves coincide — random GPS jitter averages out, so the
  two half-centroids sit on top of each other (drift → 0).
- A **walk/drive**'s halves drift apart by ~the distance travelled between them;
  a cluster that has directionally filled its `stop_radius_m` has a half-to-half
  drift of ≈ `stop_radius_m`.

A cluster is rejected as movement when that drift exceeds
`_PROGRESSIVE_DRIFT_FRACTION` (**0.5**) of `stop_radius_m`. Clusters of fewer
than four samples are too short to judge direction and are treated as dwells (so
brief legitimate stops are never dropped).

The check is O(cluster size) and does not change the O(n)-amortised scan;
verified O(n) on a 16 000-sample walk.

## Consequences

- A slow walk/drive reports **zero** stops instead of phantom chunks; a jittery
  dwell (no net direction) is still reported as one stop.
- `_PROGRESSIVE_DRIFT_FRACTION = 0.5` is a **tunable** threshold. 0.5 cleanly
  separates a directional fill of the radius (drift ≈ radius) from random jitter
  (drift → 0). A lower value rejects more aggressively (risking dropping a dwell
  that slowly shifts within its radius); a higher value is more permissive. Left
  as a module constant for now; could become an `InsightsOptions` knob if callers
  need per-request control.
- **Interaction with the merge cap ([[0013]]).** Progressive-rejection is the
  *primary* defence — a walk never becomes stops, so it never reaches the merge.
  The cap remains correct as defence-in-depth for genuine nearby dwells, but is
  rarely the binding constraint once walks are rejected at detection. Cap tests
  that fed a walk *as a sequence of stops* through `/v1/insights` should instead
  exercise `merge_nearby_stops` directly (unit level), so `detect_stops`'
  progressive filter does not pre-empt them.
- Edge cases for the adversary to probe next: a curved (non-straight) walk; a
  dwell that legitimately drifts within its radius (e.g. a boat swinging at
  anchor); very-few-sample clusters near the 4-sample floor.
