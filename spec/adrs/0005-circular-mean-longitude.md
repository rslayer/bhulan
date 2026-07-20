# ADR 0005 — Antimeridian-aware centroid longitude (circular mean)

**Status:** Accepted
**Cycle:** 5
**Date:** 2026-07-20

## Context

Cycle 4 (ADR 0004) fixed the *projection* so a dwell straddling ±180° is now
correctly **clustered** into a single stop/hotspot. That un-masked a second,
pre-existing bug one layer up: the cluster's reported **centroid longitude** was
still a plain, non-wraparound `numpy.mean` over the raw longitude values.
Averaging `+179.9999` and `-179.9999` the naive way gives `~0.0` — the
**antipode**, ~20,000 km away on the opposite side of the planet. The response
now contains a confident, specific stop (real duration, real sample count)
pinned in the wrong ocean — arguably worse than the pre-cycle-4 empty result,
because nothing signals the error.

Three call sites shared the one root cause (a non-wraparound longitude average):

- `bhulan/analytics/stops.py::detect_stops` — `lon_c = mean([p.lon ...])`
- `bhulan/analytics/hotspots.py::detect_hotspots` — the cluster centroid longitude
- `bhulan/analytics/stops.py::merge_nearby_stops` — the `(prev.lon + s.lon) / 2.0`
  midpoint

All are reachable unauthenticated via `/v1/insights` and `/v1/compare`.

Found by `tests/adversary/test_antimeridian_centroid_reports_wrong_location.py`.

## Decision

Fix the root once: add a single shared helper
`bhulan/analytics/geodesy.py::circular_mean_lon(lons)` and reuse it at all three
call sites rather than patching each. Latitude is unaffected (no wraparound at
±90 for an average) and keeps its plain `numpy.mean`. `haversine_m` is already
correct and is left untouched as the cross-check.

`circular_mean_lon` is gated on the straddle exactly like `latlon_to_xy_m` /
`bounding_box`:

- **Raw longitude span ≤ 180°** (the overwhelming majority): return the plain
  arithmetic mean `float(np.mean(lons))`, so non-antimeridian output is
  **byte-identical** to before. Gating on the *raw* span — not a shifted span —
  matters: the `+360` shift reintroduces ~1e-13° of float noise, so
  unconditionally taking the shifted mean would perturb ordinary centroids.
- **Raw span > 180°** (straddles ±180°): shift longitudes into `[0, 360)`
  (`lon < 0 → lon + 360`) so points near ±180° become contiguous, take the mean,
  then fold back to `(-180, 180]` via `((mean + 180) % 360) - 180`. The mean now
  lands *inside* the real cluster.

The resulting centroid lands within `radius_m` of every sample when measured with
`haversine_m`, identically to a cluster anywhere else on Earth.

## Consequences

- A tight dwell straddling ±180° is reported at its true location (near ±180),
  not the antipode — for `detect_stops`, `detect_hotspots`, the `/v1/compare`
  pooled `shared_hotspots`, and `merge_nearby_stops` (AC1–AC3).
- Every non-antimeridian track produces byte-identical stops, hotspots, merged
  stops, and bbox — the wraparound branch is unreachable for raw span ≤ 180°
  (AC4).
- Backend/analytics only: no new dependencies, no DB/schema change, no API field
  change. `haversine_m` and cycle 4's projection/bbox are untouched (AC5).

## Alternatives considered

- **Circular mean via `atan2(mean sin, mean cos)` unconditionally.** Rejected:
  it differs from the arithmetic mean in the last ULP for ordinary clusters,
  breaking byte-identity (AC4). Gating on the >180° raw span keeps the common
  path exact — the same trade-off ADR 0004 made for `latlon_to_xy_m`.
- **Patch each of the three call sites independently.** Rejected: three copies of
  the same wraparound logic drift; the spec mandates one shared helper.
- **Fold to `(-180, 180]` differently per call site.** Rejected: consistency with
  `bounding_box`'s existing `((x + 180) % 360) - 180` fold keeps the whole
  geodesy surface uniform.
