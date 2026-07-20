# Adversary cycle 7 — antimeridian-aware projection + bounding box

Scope: attack this cycle's fix (spec/spec.md, "Make the local-tangent-plane
projection and the bounding box antimeridian-aware") — probe the spec's
"known traps" first, then edge inputs around the fix. Product code under
`bhulan/` treated as read-only.

Ran the existing acceptance test
(`test_antimeridian_projection_breaks_stops_and_bbox.py`) first — green,
confirming AC1/AC3 (stop detection, distance-consistency) hold for the
originally-reported bug. Then worked through spec section 4's named traps
in order: poles, legitimately-wide-vs-jitter longitude spans, `/v1/compare`
pooling, and bbox consumers.

## New defects found (failing tests added)

### 1. Stop/hotspot centroid at the antimeridian is reported at the antipode

`tests/adversary/test_antimeridian_centroid_reports_wrong_location.py`
(4 tests)

This is the headline finding. Cycle 4's fix correctly makes
`latlon_to_xy_m` *cluster* a dwell that straddles +/-180 — but three
separate call sites still compute that cluster's reported **lat/lon
centroid** with a plain, non-wraparound average over the raw longitude
values:

- `detect_stops` (`bhulan/analytics/stops.py`): `lon_c =
  float(np.mean([...]))`
- `detect_hotspots` (`bhulan/analytics/hotspots.py`): `centroid_lon =
  float(np.mean([lons[i] for i in idxs]))`
- `merge_nearby_stops` (`bhulan/analytics/stops.py`): `lon = (prev.lon +
  s.lon) / 2.0`

Averaging `+179.9999` and `-179.9999` the naive way gives `~0.0` — the
**antipode** of the true location, roughly 20,000 km (half the Earth's
circumference) away from every sample in the cluster. Verified end-to-end:
a dwell at lat=10, jittering between lon=179.9999/-179.9999, is now
correctly detected as one stop and one hotspot (cycle 4's fix working as
intended) but both are reported at `lon: 0.0`. Same result pooled through
`/v1/compare`'s `shared_hotspots`, and through `merge_nearby_stops`'s
midpoint recentring.

Before cycle 4, this bug was invisible: a dwell at the antimeridian was
never clustered at all (`stops: []`), so no centroid was ever computed for
it. The projection fix makes the *fact* of the stop/hotspot correct, which
un-masks this independent, and arguably worse, defect: a client now gets a
confident-looking pin on the map that is on the literal opposite side of
the planet from the truth, instead of an empty list it might have thought
to double-check. Reachable via ordinary `points` input to `/v1/insights`
and `/v1/compare` — any real dwell near Fiji, Tonga, Kiribati, the
Aleutians, or Chukotka.

### 2. `bounding_box` is not actually the "minimal" box it documents itself as

`tests/adversary/test_bounding_box_not_minimal_multi_cluster.py` (2 tests)

This is exactly the trap spec/spec.md section 4 calls out: "A track that
legitimately spans a *wide* longitude range ... vs. a jitter across
+/-180 — the fix must not mistake one for the other." Cycle 4's fix adds
exactly one extra candidate framing (longitudes shifted into `[0, 360)`,
i.e. a cut at 0 degrees) alongside the naive framing (a cut at +/-180
degrees), and picks whichever of those two is tighter. That correctly
handles a two-cluster split across +/-180 (this cycle's target case), but
the true minimal-longitude-span box requires cutting at the single
*largest* gap between consecutive points around the circle, which can sit
anywhere — not just at 0 or +/-180.

Six points at longitudes -170, -100, -30, 0, 60, 170 (same latitude, so
only longitude framing is in play): the true minimal box cuts at the
largest gap (between 60 and 170, a 110-degree gap) and spans 250 degrees.
`bounding_box` instead reports a box running from 0 to -30 "the long way"
(through +/-180), spanning 330 degrees — 80 degrees (~8,900 km at the
equator) wider than necessary — while still asserting (via its own
`min_lon > max_lon` convention) that this is the minimal antimeridian-
crossing box. Verified both at the `bounding_box()` unit level and through
`POST /v1/insights`'s `summary.bbox`. A client zooming a map to "fit" this
bbox zooms out much further than the data requires; more importantly, the
function's contract ("the minimal-longitude-span box") is simply false for
3+ spread-out points.

### 3. Tangent-plane projection near the poles inflates cluster spread, causing false-negative stop detection

`tests/adversary/test_pole_dwell_stop_false_negative.py`

Also directly named in spec section 4: "The poles (lat +/-90) — does the
tangent-plane projection degrade there too?" Near a pole, longitude is
near-degenerate: two points a few meters apart can differ by up to 180
degrees of longitude. `latlon_to_xy_m` scales longitude linearly by a
single global `meters_per_deg_lon = 111_320 * cos(lat0)`, which is only a
valid local-plane approximation when nearby points also have nearby
longitudes — false near a pole regardless of the antimeridian fix (the
raw longitude span here, exactly 180 degrees, doesn't even trigger cycle
4's new wraparound branch; this is the plain `else` path, so the same
distortion would already have existed pre-fix — the spec explicitly asked
that this be checked while the projection is in focus).

Concretely: 30 samples alternating between (lat=89.9999, lon=0) and
(lat=89.9999, lon=180). True separation (per the correct `haversine_m`) is
~22.2m, i.e. a true cluster radius of ~11.1m — comfortably inside any
`stop_radius_m` of 15m by ordinary standards. `latlon_to_xy_m` instead
projects this pair to points ~17.5m apart from the cluster centroid
(overstating the true spread by ~57%), so `detect_stops` with
`stop_radius_m=15` reports zero stops for a dwell that is, in true
physical terms, well within the requested radius. Realistic for any GPS
trace recorded within ~15-20m of true north/south pole.

## Verified as fixed / not a bug

- **AC1/AC3** (jitter-across-+/-180 clustering and total-distance
  consistency): confirmed via the existing acceptance test — green.
- **`/v1/compare` pooling of two *separately located* antimeridian
  dwells** (spec trap 3, taken as "does pooling merge or lose distinct
  antimeridian hotspots"): pooled two tracks, one at lat=-18 (Fiji-like)
  and one at lat=52 (Aleutian-like), both jittering across +/-180 at
  different times. `shared_hotspots` correctly returned 2 distinct
  hotspots (not merged, not lost) — the clustering side of the fix
  generalizes correctly across pooled multi-track input. (Their *reported
  lon* is wrong per defect #1 above, but the clustering/pooling logic
  itself is sound.)
- **`/v1/plot/validate` bbox rendering** (spec trap 4): `PlotResponse`
  does not include a `bbox` field at all (`accepted`/`rejected`/`issues`/
  `points` only) — there is no antimeridian-bbox-consumer code path on
  this endpoint to probe.
- **`haversine_m`/`haversine_vec_m`**: untouched per the spec's
  instruction, and independently re-verified correct at the antimeridian
  and near the poles (used as the ground truth in all three defects
  above).

## Not investigated further (time-boxed out)

Per the coverage-guided targeting note, `bhulan/analytics/parsers.py`,
`bhulan/ingestion/*`, and `bhulan/analytics/geocoding.py` remain at 0-45%
adversary coverage, but are unrelated to this cycle's antimeridian feature
and were left for a future cycle scoped to those subsystems.
