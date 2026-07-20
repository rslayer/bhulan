# Robustness run 4

A follow-up adversarial pass over the public, stateless `/v1` surface
(`POST /v1/insights`, `POST /v1/plot/validate`, `POST /v1/compare`,
`POST /v1/parse/file`, `GET /v1/healthz`), using the same in-process
`TestClient(app)` pattern as prior runs — no MongoDB, no network.

Per the brief for this run, categories 2 and 3 (NaN/Infinity/overflow,
wrong-type coercion) were **not** re-walked — they were closed in run 2 and
re-verified fixed in a quick regression spot-check below, alongside a spot
check of every other run-2/run-3 defect. The bulk of this run's effort went
where prior runs looked least: **metamorphic correctness** (does the same
underlying track produce the same/predictably-transformed answer under
reordering, duplication, translation, unit changes?), **algorithmic
complexity beyond the already-fixed `detect_stops` blowup** (trip
segmentation, hotspot clustering, dedup, and — new this run — file
parsing), and **file-format robustness** on `POST /v1/parse/file`.

**2 new defect classes found**, both **silently-wrong-answer / DoS**, not
crashes:

1. A **new O(n²) blowup in KML parsing** (category 15) — a shape run-3's
   file-upload probe didn't try (`<Placemark><TimeStamp>+<Point>`, the
   standard way Google My Maps exports dated waypoints).
2. **`detect_stops` and `detect_hotspots` are time-gap-blind** (category
   14/16) — a metamorphic property this run specifically went looking for.
   Two real, separate visits to the same location, arbitrarily far apart in
   calendar time, silently collapse into one "stop"/"hotspot" whose
   reported duration spans the entire gap. Trips (which do check gaps) are
   unaffected — this is specific to `stops.py` and `hotspots.py`.

Reproducing pytest tests are under `tests/adversary/`
(`test_kml_point_timestamp_quadratic_blowup.py`,
`test_stop_and_hotspot_ignore_time_gaps.py`, 4 failing tests total). The
rest of the probed surface — including a deliberate, sustained attempt to
defeat the run-2 `detect_stops` fix with a purpose-built adversarial
construction — held up. Results for all 10 mandated categories, plus the
extra-focus areas, are below.

---

## Category 1 — Out-of-range coordinates (lat > 90 / < -90, lon > 180 / < -180)

**Verdict: CLEAN** (re-verified, unchanged since run 2)

Spot-checked `lat: 91` against `/v1/insights` → `422`
(`less_than_equal`), matching run 2's full sweep across all three
structured endpoints. `PointIn.lat`/`lon` still carry `ge`/`le`
constraints (`bhulan/analytics/insights.py:57-58`); no regression.

---

## Category 2 — NaN / Infinity / -Infinity / overflowing numerals (1e400)

**Verdict: CLEAN** (fixed in run 2, re-verified fixed here)

Sent `{"points":[{"lat": NaN, "lon": 1.0}]}` (raw body) → `422`, not the
`500` from run 2's original finding. Also re-sent the run-3 successor
defect (`speed_mps: 1e400` reaching a *successful* response) →
`200` with `max_speed_kmh: 0.0` (the value is now dropped as implausible
telemetry per `PointIn._drop_implausible_speed`, `insights.py:68-93`, not
reflected as `inf`). Both fixes hold.

---

## Category 3 — Wrong types (null / string / bool / array where a number is expected)

**Verdict: CLEAN** (fixed in run 2, not re-walked per this run's brief —
the malformed-GeoJSON-coordinates 500 from run 2 is a parser-level defect
unrelated to this run's focus areas and wasn't touched by any change made
here)

---

## Category 4 — Empty coordinate list; single point; two identical points

**Verdict: CLEAN**

Re-confirmed via this run's own metamorphic probes (see Category 16
below): an exact duplicate point inserted mid-track is silently deduped
by `prepare_track` and produces byte-identical `summary`/`stops`/`trips`
output (`accepted_point_count` unchanged, `quality.issues` correctly
reports `"Removed 1 duplicate points"`). Inserting the *same* duplicate
10 times in a row produces an identical result to inserting it once.

---

## Category 5 — Timestamps: non-monotonic, duplicate, far-future, pre-epoch, missing

**Verdict: CLEAN** (the run-2 `OverflowError` on extreme datetime + offset
is fixed and re-verified here)

Re-sent `{"points":[{"lat":1.0,"lon":1.0,"ts_utc":"0001-01-01T00:00:00+14:00"}]}`
→ `200` (was `500` in run 2). `mobility._to_utc`'s `except (OverflowError,
OSError)` fallback (`mobility.py:48-56`) is present and working.

New this run: timestamps that are valid individually but separated by a
**huge, sample-free gap** are the subject of Category 14/16's defect
below — that's a distinct finding from "one extreme timestamp," so it's
written up separately rather than folded in here.

---

## Category 6 — Enormous payloads (100k+ points) — latency / memory / DoS

**Verdict: CLEAN at the cap; see Category 14 for the complexity sub-finding**

- `101,001` points → `413` (unchanged from run 2, the cap itself works).
- The run-2 `detect_stops` O(n²) blowup is fixed — see Category 14 for
  the adversarial re-verification performed this run.
- **New sub-finding**: the size cap is checked *after* parsing for
  `/v1/parse/file` (`_enforce_point_cap` runs on the parser's return
  value), so a file that is small in bytes but pathological in *shape*
  can burn CPU before any cap has a chance to reject it. See Category 15.

---

## Category 7 — Swapped lat/lon, mixed units, precision extremes (1e-15)

**Verdict: CLEAN**, plus a new unit-conversion correctness check this run

Sent the same physical speed (10 m/s) via three different CSV unit
columns to `/v1/plot/validate`:
- `speed` (assumed m/s): `10` → `speed_mps: 10.0`
- `speed_kmh`: `36` → `speed_mps: 10.0`
- `speed_mph`: `22.3694` → `speed_mps: 10.000016576`

All three converge to the same `speed_mps` (mph carries expected
floating-point rounding noise at the 6th decimal, not a bug). Unit
conversion in `parsers.py::_dict_to_point` (lines 110-122) is correct.

---

## Category 8 — Unicode / control chars / very long strings in name/string fields

**Verdict: CLEAN** (unchanged since run 2 — no string-handling code path
touched this run)

---

## Category 9 — Malformed transport (wrong content-type, truncated JSON, deeply nested JSON, extra/unknown fields, missing required fields)

**Verdict: CLEAN** (the run-2 deeply-nested-JSON `RecursionError` → `500`
is fixed and re-verified here)

Re-sent `{"points": [[[...[1]...]]]}` with 2000 levels of nesting →
`422` (was `500` in run 2).

---

## Category 10 — Injection-looking values in string fields

**Verdict: CLEAN** (unchanged since run 2 — no string-handling / sink code
path touched this run)

---

## Category 14 — Algorithmic complexity: `detect_stops` re-attack, trips, hotspots, dedup

**Verdict: CLEAN.** `detect_stops`'s run-2 fix holds up under a dedicated
attempt to defeat it; trip segmentation, hotspot clustering, and dedup are
all genuinely linear at the sizes tested.

### `detect_stops`: adversarial re-verification of the run-2 fix

The run-2 quadratic blowup was fixed by maintaining a running upper bound
on the cluster spread (`stops.py::_cluster_end`, a triangle-inequality
argument: the centroid can move by at most `shift` per added point, so the
bound only needs an exact recompute when it *might* cross `radius_m`).
This run specifically tried to construct inputs that defeat that bound —
i.e., force it to be loose (an old point's true distance grows less than
the shift assumed) so that exact recomputes keep firing on an
ever-growing single window, restoring O(n²).

Constructions tried (all via direct calls to `detect_stops`, instrumented
to count calls to the exact-recompute function `_cluster_radius_m`):

1. **Boundary-hugging via bisection** (new point placed, per step, at the
   farthest feasible distance from the current centroid that keeps the
   *exact* cluster radius just under `radius_m`, both via a
   continuously-varying golden-angle direction and a fixed antipodal
   alternation): the newly added point is always the new extremal point,
   so `d_new` — an exact per-step measurement, not the loose `shift`-based
   term — dominates the bound. Result: **1 exact recompute total** for a
   600-800-point single cluster; no blowup.
2. **Anchor + orthogonal "wobble" points** (an explicit old point held
   right at `radius_m − ε`, with a stream of new points added near the
   opposite side of the cluster to accumulate `shift` without directly
   threatening the anchor): analytically, the algorithm's own
   centroid-update math forces `shift ≈ d_new / m` — any wobble large
   enough to push the running bound over threshold is (up to a factor of
   `m`) already large enough that the wobble point's own `d_new` would
   trigger the same recompute anyway, so the "loose" `shift`-only pathway
   never dominates independently. A version of this construction that
   *did* reach steady per-step recomputation broke the cluster (the wobble
   itself became the new extremum) rather than sustaining O(n²) growth —
   confirmed via `top_size` staying pinned at the pre-wobble window size
   rather than growing with `n`.
3. Random-direction / random-magnitude perturbation inside a disk near
   `radius_m`: clusters broke early (window size ~3-10) rather than
   sustaining growth, since misaligned points routinely pushed the exact
   radius over threshold.

No construction reproduced superlinear scaling. Measured wall time for
the "many points, one long stop" shape (the run-2 counter-example) at
`n = 10,000 / 20,000 / 40,000 / 80,000 / 100,000` scaled linearly
(~2× time per 2× n) throughout. This is a **CLEAN re-verification**, not
just an absence-of-evidence — the specific mathematical property that
made the fix hard to defeat (`shift ≈ d_new / m`, so the loose bound
pathway can't dominate without the tight one also firing) is documented
above for future passes.

### Trip segmentation, hotspot clustering, dedup

Direct n / 2n / 4n / 8n / 16n timing (bypassing HTTP, calling the
analytics functions directly) on a realistic "always slowly moving, lots
of short direction changes" track (worst case for repeated
window-boundary and grid-cell computation):

| n | `detect_stops` | `detect_trips` | `detect_hotspots` |
|---|---|---|---|
| 5,000 | 0.184s | 0.004s | 0.006s |
| 10,000 | 0.359s | 0.008s | 0.012s |
| 20,000 | 0.713s | 0.015s | 0.023s |
| 40,000 | 1.453s | 0.031s | 0.048s |
| 80,000 | 2.853s | 0.063s | 0.100s |

All three scale linearly (~2× per doubling). `prepare_track`'s
dedup/sort was timed separately up to 100,000 points (0.417s at 100k,
also linear). No quadratic behavior found in any of these.

---

## Category 15 — File parsing (`/v1/parse/file`): a new O(n²) in KML timestamp lookup

**Verdict: DEFECT** — `tests/adversary/test_kml_point_timestamp_quadratic_blowup.py`

Run 3 probed `/v1/parse/file` with billion-laughs KML, XXE, malformed/
truncated XML, and non-UTF-8 binary garbage, and found it clean — but
never tried a KML shape built from many `<Placemark>` elements, each
containing one `<TimeStamp>` and one `<Point>` (as opposed to a single
`<LineString>` or `<gx:Track>`). That shape is exactly what Google My
Maps / Earth produce for "save location as a dated point" — e.g. a
"starred places" or "visited spots" export.

Sent KML files of `n` such Placemarks (`n = 500, 1000, 1500, 2000`,
70 KB – 284 KB):

| n | file size | wall time |
|---|---|---|
| 500 | 70 KB | 0.12s |
| 1,000 | 142 KB | 0.39s |
| 1,500 | 213 KB | 0.87s |
| 2,000 | 284 KB | 1.51s |

Time roughly quadruples per doubling of `n` — the O(n²) signature.
Extrapolating the measured coefficient, a file well under 5 MB (a
fraction of `MAX_UPLOAD_BYTES` = 25 MB) would tie up a worker for tens of
minutes.

Expected: parse time roughly linear in the number of points, similar to
the `<LineString>` and `<gx:Track>` shapes, which this run confirmed
*are* linear (80,000 points in 0.24s and 0.44s respectively — see
Category 14's table for the comparison methodology).

Actual: O(n²), confirmed both via direct function timing and through the
live `POST /v1/parse/file` endpoint.

Root cause: `parse_kml_bytes` (`bhulan/analytics/file_parsers.py:134-142`)
calls `_nearest_timestamp(root, elem)` for every `<Point>` element found.
`_nearest_timestamp` (lines 224-244) walks *every* `<Placemark>` in the
whole document from the root and, for each one, does a full subtree scan
(`_contains`, lines 247-251) checking whether it contains the target
`<Point>`. The `elem.getparent() if hasattr(elem, "getparent") else None`
guard at line 140 is meant to skip this expensive root-walk when a
cheaper parent pointer is available, but stdlib `xml.etree.ElementTree`
(used here, not `lxml`) never exposes `getparent` — so the guard is
always `None` and the expensive path always executes, for every point,
in every KML file. `n` points × O(n) placemark/subtree scan each =
O(n²).

Compounding factor (Category 6 tie-in): `/v1/parse/file`'s own point-cap
check (`_enforce_point_cap`) runs *after* `parse_file_bytes` returns, so
none of the service's own size guards can prevent the O(n²) cost from
being paid first.

Impact: an unauthenticated caller can submit an ordinary, sub-megabyte
KML export and tie up a worker process for a very long time — a
straightforward DoS vector, and unlike the fixed `detect_stops` blowup,
this one is triggered by unremarkable everyday input (a small "saved
places" export), not a specially-shaped track.

Other file-parsing checks performed this run, all clean, no crash:
- Truncated GPX (`<trkpt lat="1" lon="1">` with no closing tags) → `400`.
- Non-XML garbage named `.gpx` → `400`.
- Truncated binary `.fit` (7 bytes) → `400` (`"file truncated?"`).
- Empty `.fit` → `400` (`"Uploaded file is empty"`).
- GPX content uploaded with a `.txt` extension (wrong-extension case) →
  `200`, 0 points, correctly falls through to the text parsers which
  don't understand raw XML — no crash, no misparse.
- Uppercase extension (`DATA.GPX`) → `200`, parsed correctly (extension
  matching is case-insensitive via `.lower()`).
- Missing filename entirely → `422` (FastAPI's own `UploadFile`
  validation).
- **Zip-bomb attempt**: a 50 MB-of-zeros payload compressed to a 51 KB
  `.zip`, uploaded as both `bomb.kml` and `bomb.zip` → `400` in both
  cases (`"Invalid KML/XML"` / `"Unknown file type"`). None of the three
  parsers (`gpxpy`, stdlib XML, `fitdecode`) perform any decompression —
  a zip file is simply invalid input to all of them, so there is no
  decompression-bomb surface on this endpoint at all.

---

## Category 16 — Metamorphic properties: reordering, duplication, translation, units

**Verdict: 1 DEFECT (documented under Category 14's write-up as the
`detect_stops`/`detect_hotspots` gap-blindness finding —
`tests/adversary/test_stop_and_hotspot_ignore_time_gaps.py`), rest CLEAN**

This was the primary new focus area for this run: does resubmitting the
*same underlying track* in a different but semantically-equivalent form
produce the same (or predictably transformed) answer?

### Clean sub-cases
- **Reordering**: a realistic drive-stop-drive track (66 points) submitted
  in chronological order vs. randomly shuffled order → byte-identical
  `summary`, `stops`, and `trips` in the response. `prepare_track`'s
  internal re-sort by `ts_utc` makes submission order irrelevant, as
  designed.
- **Exact duplication**: inserting one exact copy of an existing point
  (same lat/lon/ts) mid-track, or inserting the same duplicate 10 times
  in a row, → identical `stops`/`trips`/`summary` (apart from the
  expected `point_count`/`quality.issues` bookkeeping correctly
  reflecting the extra raw input rows). `prepare_track`'s dedup `set`
  handles this correctly regardless of duplicate multiplicity.
- **Constant coordinate shift**: translating every point in the 66-point
  track by `+0.01°` lat/lon → `total_distance_km` 4.1318 → 4.1315,
  `avg_moving_speed_kmh` 23.21 → 23.209, `max_speed_kmh` 25.213 → 25.211.
  These tiny (~0.01-0.03%) differences are the *expected*, physically
  correct consequence of the haversine/local-tangent-plane projection's
  `cos(lat)` dependence on absolute latitude — not a bug. Stop count and
  moving time were exactly unchanged.
- **Unit changes**: see Category 7 above (mps/kmh/mph all converge to the
  same `speed_mps`).

### Defect sub-case: time-gap-blind stops and hotspots

Constructed a track representing two real, clearly-separate visits to the
same location: 10 samples (1/minute) parked at an office, then a **7-day
gap with zero samples at all** (device fully off, not just a data gap the
device tried to report), then 10 more samples (1/minute) parked at the
*same* spot:

```
POST /v1/insights
{"points": [
  {"lat":40.0,"lon":-73.0,"ts_utc":"2024-01-01T00:00:00Z"}, ... (10 samples, 60s apart)
  {"lat":40.0,"lon":-73.0,"ts_utc":"2024-01-08T00:00:00Z"}, ... (10 samples, 60s apart)
]}
```

Expected: two trips (✓ correctly reported — `detect_trips`'s
gap-based split works), two stops of ~9 minutes each, and a hotspot with
`visit_count: 2` and `time_spent_min` around 18-20 minutes total.

Actual:
- **One** stop, `duration_min: 10099.0` (≈ 7 days) — the entire calendar
  gap reported as continuous dwell time at the location.
- **One** hotspot, `visit_count: 1`, `time_spent_min: 10099.0` — same
  issue.
- Re-verified with realistic GPS jitter (±~5m random noise per sample
  instead of an exact repeated coordinate) — same result
  (`duration_min: 10099.0`, `radius_m: 6.88`), so this isn't an artifact
  of submitting bit-identical coordinates.
- Re-verified through `POST /v1/compare`, submitting the two visits as
  **two separate tracks** recorded a week apart (an even more obviously
  "two visits" shape than a single track with a gap) →
  `shared_hotspots[0].visit_count: 1` regardless of which track is listed
  first in the request (order-independence itself is fine — the bug is
  that neither order produces the correct count).

Root cause — two independent gap-blind implementations:
1. `bhulan/analytics/stops.py::detect_stops` / `_cluster_end` only checks
   spatial spread (`radius_m`) when growing a window; it never inspects
   the time gap between consecutive samples. `trips.py::_trip_bounds`
   (lines 83-89) explicitly splits on `trip_split_gap_seconds` for
   exactly this reason — stops never received the equivalent check.
2. `bhulan/analytics/hotspots.py::_visit_count` / `_time_spent_s`
   (lines 107-148) define a "visit" as a run of consecutive *array
   indices* within the same grid cell, post-`prepare_track`-sort. Two
   revisits with nothing recorded in between become index-adjacent and
   are indistinguishable from one continuous dwell; `_time_spent_s` sums
   every inter-index `dt` inside the run, including the multi-day gap.

Impact: any client reading `stops[].duration_min` or
`hotspots[].time_spent_min` / `visit_count` — the exact fields the
service's own `hotspots.py` module docstring says are the primary way to
answer "how long did the user spend at this place" and "how many times
did they visit," especially for the multi-track/`compare` recurring-place
use case — gets a silently wrong answer that grows unboundedly with the
calendar gap between visits. No crash, no rejection, no signal that
anything is wrong.
