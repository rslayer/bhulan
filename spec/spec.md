# Product Spec — bhulan

> Living spec. The cockpit writes it; the loop reads it, builds, iterates
> (eval → adversarial → robustness → refute → triage), then opens a PR.
> The loop NEVER merges to master and NEVER deploys — you do. bhulan is a public
> demo; keep it PR-gated.

**Status:** cycle 4
**Cycle of last revision:** 4

---

## 1. This cycle's single outcome

**Make the local-tangent-plane projection and the bounding box
antimeridian-aware**, so a track near ±180° longitude is no longer projected as
if it spans the globe.

Found by sweep 5 (a silently-wrong-answer geo-correctness bug, the class prior
runs never covered). Root cause is one function:
`bhulan/analytics/geodesy.py::latlon_to_xy_m` projects with a **linear**
longitude difference (`x = (lons - lon0) * meters_per_deg_lon`), no wraparound.
Two points at lon `179.9999` and `-179.9999` (~22 m apart in reality) project
~40 000 km apart. Because both `detect_stops` and `detect_hotspots` cluster on
this projection, a real tight dwell at the antimeridian yields **`stops: []`**;
and `geodesy.py::bounding_box` has the same blindness (naive min/max over raw
longitude), so `InsightsSummary.bbox` spans ~360° for a sub-meter dwell. The
same request's `total_distance_km` is *correct* (`haversine_m` wraps properly),
so the response is internally self-contradictory. Reachable via ordinary
unauthenticated `/v1/insights` input — any track near Fiji, Tonga, Kiribati,
the Aleutians, or Chukotka.

(`tests/adversary/test_antimeridian_projection_breaks_stops_and_bbox.py`)

## 2. Hard constraints

- **The named test is the acceptance test** — it already exists and is failing.
  Fix the product code; do NOT weaken it.
- **Fix at the root, once.** The fix belongs in `latlon_to_xy_m` (and
  `bounding_box`) in `geodesy.py`, not per-caller in stops/hotspots — fixing the
  projection there fixes every consumer. Normalise the longitude difference to
  the range `(-180, 180]` (i.e. `((lons - lon0 + 180) % 360) - 180`) before
  scaling to metres, so the shortest signed longitude delta is used.
- **`bounding_box` must handle wraparound** — for points clustered near ±180°,
  report the minimal-span box (which may cross the antimeridian, e.g.
  `lon_min=179.99, lon_max=-179.99` interpreted as the short way round), not the
  naive `-179.99 … 179.99` that implies the whole globe. Document the convention
  chosen in the return value / ADR.
- **`haversine_m` is already correct — do NOT touch it.** It is the cross-check.
- **Output must be unchanged for all non-antimeridian tracks** — the vast
  majority of inputs. The projection for points far from ±180° must be
  numerically identical to today (the normalisation is a no-op there).
- **Preserve every existing passing test** — geodesy unit tests, and cycles 1–3
  (gap-aware stops/hotspots/merge, KML O(n)) must stay green.
- Backend/analytics only. No new dependencies, no API-shape changes (adding a
  documented bbox-crosses-antimeridian convention is fine; changing the field
  set is not), no DB/schema changes.

## 3. Non-negotiable acceptance criteria

- **AC1:** a tight dwell straddling ±180° is detected as a stop (and a hotspot),
  same as the identical dwell placed anywhere else on Earth.
- **AC2:** `bbox` for an antimeridian-straddling track reports a sub-degree span
  (the true small extent), not ~360°.
- **AC3:** `stops`/`bbox` and `total_distance_km` in the same response are now
  mutually consistent for such a track.
- **AC4:** every non-antimeridian track produces byte-identical projection,
  stops, hotspots, and bbox to before (no regression).
- **AC5:** `poetry run pytest tests/unit/ tests/adversary/test_antimeridian_projection_breaks_stops_and_bbox.py -q` is green.

## 4. Known traps for the adversary to probe next

- The poles (lat ±90) — does the tangent-plane projection degrade there too?
- A track that legitimately spans a *wide* longitude range (a real
  transcontinental trip) vs. a jitter across ±180 — the fix must not mistake one
  for the other.
- `/v1/compare` `shared_hotspots` pooling across two antimeridian tracks.
- Bbox convention consumers: does `/v1/plot` render a crosses-antimeridian bbox
  correctly?
- Remaining backlog: GPX/FIT parsing complexity, XML entity-expansion DoS,
  memory growth (from this sweep's other probed areas if any survived triage).

## 5. Definition of done for this cycle

- AC1–AC5 pass. The projection and bbox wrap correctly at ±180°; haversine
  untouched; non-antimeridian output unchanged.
- ADR recorded in `spec/adrs/` (document the bbox wraparound convention).
- A PR is opened for review. **No merge to `master` without your review.**

## 6. Deploy target

None from the loop. The loop opens a PR; you review and merge. bhulan is a
public demo — production stays gated.
