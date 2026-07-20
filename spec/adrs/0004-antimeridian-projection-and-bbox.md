# ADR 0004 — Antimeridian-aware projection and bounding box

**Status:** Accepted
**Cycle:** 4
**Date:** 2026-07-20

## Context

`bhulan/analytics/geodesy.py::latlon_to_xy_m` projects lat/lon onto a local
tangent plane by scaling a **linear** longitude difference to metres
(`x = (lons - lon0) * meters_per_deg_lon`, `lon0 = mean(lons)`). It has no
wraparound handling at ±180°. Two samples at `lon = 179.9999` and
`lon = -179.9999` are ~22 m apart in reality (per the trig-based, already-correct
`haversine_m`), but the linear difference reads ~359.9998°, so they project
~40 000 km apart. Both `detect_stops` and `detect_hotspots` cluster on this
projection, so a genuine tight dwell straddling the antimeridian (Fiji, Tonga,
Kiribati, the Aleutians, Chukotka) never clusters into a stop — `stops: []`.

`geodesy.py::bounding_box` had the identical blindness: naive `min/max` over raw
longitudes reported `min_lon = -179.9999, max_lon = 179.9999` — a ~360° span
implying the whole globe — for a sub-100 m dwell. Because `total_distance_km`
(via `haversine_vec_m`) wraps correctly, the same `/v1/insights` response was
internally self-contradictory: a correct tiny distance beside a zero stop count
and a whole-world bbox.

Found by sweep 5
(`tests/adversary/test_antimeridian_projection_breaks_stops_and_bbox.py`).

## Decision

Fix the root — the projection and the bbox — once each, so every consumer
(`detect_stops`, `detect_hotspots`, `/v1/compare` pooling, `InsightsSummary.bbox`)
is corrected without per-caller changes. `haversine_m` / `haversine_vec_m` are
already correct and are left untouched as the cross-check.

### 1. `latlon_to_xy_m`

A track is treated as straddling the antimeridian when its raw longitude span
(`max - min`) exceeds 180°. Only then:

- **Reference longitude** — `lon0` is the mean of longitudes shifted into
  `[0, 360)` (`lon < 0 → lon + 360`), so the reference lands *inside* the real
  cluster rather than on its antipode. (The plain arithmetic mean of `179.99` and
  `-179.99` is `0` — 180° from every point, which is exactly why normalising the
  difference alone would not have fixed it.)
- **Longitude difference** — normalised to `(-180, 180]` via
  `((lons - lon0 + 180) % 360) - 180` before scaling to metres, so each point
  takes the short way round to `lon0`.

For every non-straddling track (raw span ≤ 180° — the overwhelming majority) the
code takes the original branch verbatim: `lon0 = mean(lons)`,
`dlon = lons - lon0`. The modulo is a mathematical no-op there, so it is skipped
entirely to guarantee **byte-identical** output (avoiding last-ULP drift from the
`% 360` round-trip).

### 2. `bounding_box` — wraparound convention

The box is the **minimal-longitude-span** box.

- Non-straddling points: the naive `min/max`, with `min_lon <= max_lon` as
  before (byte-identical).
- Straddling points: longitudes are shifted into `[0, 360)`; if that span is
  strictly smaller than the raw span, the box crosses ±180° and is reported by
  folding the shifted edges back to `(-180, 180]`. The result has
  **`min_lon > max_lon`**, and is defined to run **east from `min_lon`, over
  +180°/−180°, to `max_lon`** — the short way round. E.g. a dwell at ±180°
  yields `min_lon = 179.9999, max_lon = -179.9999` (a ~0.0002° box), not
  `-179.9999 … 179.9999` (~360°).

**Consumer contract:** a bbox with `min_lon > max_lon` is an
antimeridian-crossing box, not an empty or inverted one. The API field set is
unchanged (`BBox{min_lat, min_lon, max_lat, max_lon}`); only the interpretation
of the crossing case is newly documented, which the spec permits.

## Consequences

- A tight dwell straddling ±180° is now detected as a stop and a hotspot,
  identically to the same dwell anywhere else on Earth (AC1).
- `bbox` for such a track reports its true sub-degree extent, not ~360° (AC2).
- `stops` / `bbox` / `total_distance_km` in one response are now mutually
  consistent (AC3).
- Every non-antimeridian track produces byte-identical projection, stops,
  hotspots, and bbox — the antimeridian branch is unreachable for raw span
  ≤ 180° (AC4).
- Backend/analytics only: no new dependencies, no DB/schema change, no API field
  change (AC5).

## Alternatives considered

- **Normalise the longitude difference against the arithmetic mean only** (the
  spec's headline formula, without a wraparound-aware `lon0`). Rejected on its
  own: for a symmetric straddle the arithmetic mean is the antipode, so both
  deltas stay just under ±180°, the modulo changes nothing, and the projection
  stays broken. The wraparound-aware reference longitude is what makes the
  normalisation bite.
- **Circular mean (`atan2(mean sin, mean cos)`) as `lon0` unconditionally.**
  Rejected: it differs from the arithmetic mean in the last ULP for ordinary
  tracks, breaking the byte-identical requirement (AC4). Gating on the >180°
  span keeps the common path exact.
- **Report the straddling bbox as `-180 … 180` (a full-width box).** Rejected: it
  is the very "whole globe" answer the bug produces; a map fitting it zooms out
  to the planet instead of the dwell.
