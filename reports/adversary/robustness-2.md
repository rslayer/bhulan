# Robustness run 2

Adversarial audit of the public, stateless `/v1` surface (`POST
/v1/insights`, `POST /v1/plot/validate`, `POST /v1/compare`, `GET
/v1/healthz`) using the in-process `TestClient(app)` pattern from
`tests/integration/test_insights_api.py` — no MongoDB, no network.

**5 defect(s) found**, covering categories 2, 3, 5, 6, and 9. Categories
1, 4, 7, 8, and 10 were probed thoroughly and found clean. Reproducing
pytest tests are under `tests/adversary/` (one file per defect class,
18 failing tests total); this file is the narrative companion.

Note on run 1: an earlier adversary pass (`robustness/run-1`, commit
`105ccab`) found the same NaN/Infinity/overflow defect documented under
category 2 below. That branch was never merged to `master`, so the fix
is still absent here — this run reproduces it fresh (with an added
"NaN in a non-lat/lon bounded field" case) rather than assuming it's
covered.

---

## Category 1 — Out-of-range coordinates (lat > 90 / < -90, lon > 180 / < -180)

**Verdict: CLEAN**

`PointIn.lat`/`PointIn.lon` (`bhulan/analytics/insights.py:45-46`) carry
`ge`/`le` pydantic constraints. Every endpoint returns a clean `422`
with a precise `ctx.le`/`ctx.ge` error for `lat=91`, `lat=-91`,
`lon=181`, `lon=-181`, tested against `/v1/insights`, `/v1/plot/validate`,
and `/v1/compare`.

Sent: `{"points":[{"lat":91,"lon":0}]}` (and the 3 sibling boundary
violations) to all three endpoints.

Result: `422` in every case, e.g.
`{"detail":[{"type":"less_than_equal","loc":["body","points",0,"lat"],"msg":"Input should be less than or equal to 90", ...}]}`.

---

## Category 2 — NaN / Infinity / -Infinity / overflowing numerals (1e400)

**Verdict: DEFECT** — `tests/adversary/test_nan_infinity_floats_crash.py`

Sent (raw body, `content-type: application/json`, bypassing the JSON
client helper so the literal tokens reach the wire):
- `{"points":[{"lat": NaN, "lon": 1.0}]}`
- `{"points":[{"lat": Infinity, "lon": 1.0}]}`
- `{"points":[{"lat": -Infinity, "lon": 1.0}]}`
- `{"points":[{"lat": 1e400, "lon": 1.0}]}` (overflows to `inf` during
  stdlib JSON parsing, no exotic literal needed)
- `{"points":[{"lat":1.0,"lon":1.0}],"options":{"stop_radius_m": NaN}}`
  (any bounded numeric field, not just lat/lon)

Expected: `422` — pydantic's `ge`/`le` constraints correctly reject
`NaN`/`inf` (comparisons against `NaN` are always `False`).

Actual: `500 Internal Server Error` on `/v1/insights`,
`/v1/plot/validate`, and `/v1/compare`.

Root cause: Python's stdlib `json` module accepts the non-standard
`NaN`/`Infinity`/`-Infinity` tokens when *parsing* the request body.
Pydantic does correctly raise a `RequestValidationError` for the
out-of-range value — but FastAPI's default validation-error handler
echoes the rejected value back in the error detail's `"input"` field,
and Starlette's `JSONResponse` renders with strict RFC 8259 JSON
(`allow_nan=False`). Building the 422 body itself then raises
`ValueError: Out of range float values are not JSON compliant` inside
Starlette, which is uncaught and surfaces as a 500. The crash happens
while rendering the *rejection*, not the acceptance.

Same underlying pattern as the deeply-nested-JSON defect below
(category 9): the input is correctly identified as invalid, but the act
of reporting that back to the client is what actually fails.

---

## Category 3 — Wrong types (null / string / bool / array where a number is expected)

**Verdict: DEFECT** — `tests/adversary/test_malformed_geojson_coordinates_crash.py`

### Clean sub-cases (pydantic-level structured `points`)
Sent to `/v1/insights` as structured `points`:
- `lat: null` → `422 float_type`
- `lat: "abc"` → `422 float_parsing`
- `lat: [1,2]` → `422 float_type`
- `lat: {"a":1}` → `422 float_type`
- `points: "not a list"` → `422 list_type`
- `points: {"lat":1,"lon":1}` → `422 list_type`
- `points: ["notadict"]` / `points: [null]` → `422 model_attributes_type`
- `ts_utc: true` / `ts_utc: [1,2,3]` → `422 datetime_type`

All clean. Two leniency notes (not defects, standard pydantic v2 lax
mode, documented for completeness):
- `lat: "12.5"` (numeric string) is silently coerced to `12.5` and
  accepted (`200`) — expected pydantic behavior, not a bug.
- `lat: true` is silently coerced to `1.0` and accepted (`200`) —
  `bool` is a Python `int` subclass, so pydantic's lax float coercion
  accepts it. Worth knowing about (a buggy client sending booleans by
  mistake would not be caught), but this is pydantic's documented
  default across virtually every field in the codebase, not specific
  to `bhulan`; not treated as a reproducing-test-worthy defect here.

### Defect sub-case: malformed GeoJSON `coordinates`
Sent as free-text GeoJSON via the `text` field (a supported input mode
per the `parsers.py` module docstring), to all three endpoints:
- `{"type":"Point","coordinates":[1]}` (too few elements)
- `{"type":"Point","coordinates":"ab"}` (string instead of a list)
- `{"type":"Point","coordinates":5}` (number instead of a list)
- `{"type":"LineString","coordinates":[[1],[2]]}` (malformed pairs)

Expected: `400`/`422` (the sibling case of unparseable text already
returns `400` elsewhere in this codebase).

Actual: `500 Internal Server Error`.

Root cause: `bhulan/analytics/parsers.py::_coords_from_geojson_geom`
(~line 199) does `float(coords[1]), float(coords[0])` (or iterates
`coords` for `LineString`/`MultiPoint`) with no type/length guard.
`parse_any` (`bhulan/analytics/parsers.py:281-304`) only catches
`(json.JSONDecodeError, ParseError)` around the GeoJSON path — a bare
`IndexError`, `ValueError`, or `TypeError` from the malformed
coordinates is none of those and propagates through
`_materialize_points` (`bhulan/api/routes/insights.py`, which only
wraps the call in `except ParseError`) straight out of the endpoint.

---

## Category 4 — Empty coordinate list; single point; two identical points

**Verdict: CLEAN**

- `{"points": []}` on `/v1/insights` and `/v1/plot/validate` → `400`
  `"Request must include either 'points' or non-empty 'text'"` (an
  empty list is falsy in Python, so it correctly falls through to the
  "need points or text" check rather than being treated as 0 valid
  points).
- `{"points": [{"lat":1,"lon":1}]}` (single point) → `200`, sane
  all-zero distance/speed summary, no stops/segments crash.
- Two identical points → `200`, `accepted_point_count: 1` (exact
  duplicate correctly deduped by `prepare_track`'s `seen` set).
- `/v1/compare` with one track's `points: []` → `400` scoped to that
  track's label, correctly falls through to the text-required check
  (same falsy-empty-list behavior as above — verified this doesn't get
  silently treated as "0 valid points, proceed anyway").
- `/v1/compare` with only 1 track → `422`
  `"Provide at least 2 tracks to compare"` (the `_min_tracks` validator).

---

## Category 5 — Timestamps: non-monotonic, duplicate, far-future, pre-epoch, missing

**Verdict: DEFECT** — `tests/adversary/test_extreme_datetime_offset_overflow_crash.py`

### Clean sub-cases
- Non-monotonic timestamps (`[t=10s, t=0s]` in submission order) →
  `200`, correctly re-sorted by `prepare_track` before distance/speed
  math.
- Duplicate timestamps on distinct coordinates → `200`, division-by-zero
  in speed calculation correctly guarded (`moving_time_min: 0.0`, no
  crash).
- Far-future (`9999-12-31T23:59:59+14:00`) → `200`.
- Pre-epoch (`1901-01-01T00:00:00Z`) → `200`.
- Missing `ts_utc` mixed with present timestamps → `200`.

### Defect sub-case: extreme datetime + non-UTC offset
Sent (single point, no second point required):
- `{"points":[{"lat":1.0,"lon":1.0,"ts_utc":"0001-01-01T00:00:00+14:00"}]}`
- `{"points":[{"lat":1.0,"lon":1.0,"ts_utc":"9999-12-31T23:59:59-14:00"}]}`

Expected: `200` (as with the far-future/pre-epoch cases above, which
succeed) or a clean `4xx` if the service wants to reject implausible
dates.

Actual: `500 Internal Server Error` on `/v1/insights` and
`/v1/compare`. (`/v1/plot/validate` is unaffected — it never calls
`prepare_track`.)

Root cause: `bhulan/analytics/mobility.py::_to_utc` (line 48) calls
`ts.astimezone(timezone.utc)` unconditionally. A timestamp already at
the extreme edge of Python's representable `datetime` range, combined
with a UTC offset that pushes the *converted* value past that edge,
raises `OverflowError: date value out of range` (e.g.
`0001-01-01T00:00:00+14:00` needs to subtract 14 hours to normalize to
UTC, which underflows past `datetime.min`). `PointIn.ts_utc`
(`bhulan/analytics/insights.py`) has no plausibility bounds — unlike
the ingestion-only `TrackPoint.validate_timestamp`
(`bhulan/models/canonical.py`), which rejects pre-1970/post-now+2days
timestamps but does not apply to this public surface. The
`OverflowError` is uncaught between `_to_utc` and the route handler.

---

## Category 6 — Enormous payloads (100k+ points) — latency / memory / DoS

**Verdict: DEFECT** — `tests/adversary/test_stop_detection_quadratic_blowup.py`

### Clean sub-case: the size cap itself
`101,001` points → `413`
`"Request has too many points: 100001 > 100000."`, fast (~0.2s), as
expected from `_enforce_point_cap`.

### Defect sub-case: algorithmic complexity within the cap
Sent: a single track of tightly-clustered points (5 distinct
coordinates within ~1m of each other, one sample per second — i.e. one
long "parked" stop, not a contrived adversarial shape) at increasing
sizes, all well within `MAX_PUBLIC_POINTS`/`MAX_POINTS` (100,000):

| points | wall time |
|---|---|
| 12,000 | 0.44s |
| 15,000 | 0.59s |
| 20,000 | 0.94–1.24s |
| 30,000 | 1.87s |
| 50,000 | 4.46s |
| 100,000 (the service's own cap) | 15.56s |

Expected: `bhulan/analytics/stops.py`'s own module docstring claims the
implementation "brings the worst case down from O(n*m) (the legacy
algorithm) to O(n log n) in the common case" via "a sliding window plus
a KD-tree" — under that claim, 100,000 points should take a small
constant multiple of the 12,000-point time (a few seconds at most), not
35x longer.

Actual: response is still `200` (no crash), but scaling is clearly
quadratic (4x the points ≈ 4x² the time), and no KD-tree is used
anywhere in the file despite the docstring.

Root cause: `bhulan/analytics/stops.py::detect_stops` (lines 74-111)
uses a double `while` loop. For each starting index `i` it grows window
`j` one sample at a time, and on *every* growth step calls
`_cluster_radius_m` (lines 41-45), which recomputes the centroid and
max-distance over the *entire current window* from scratch. Growing one
window from size 1 to size k costs `1 + 2 + ... + k` = `O(k^2)` — for a
track that is mostly one long stop (a very ordinary real-world shape:
"car parked for an hour, one GPS fix per second" is exactly `k ≈ 3600`
consecutive same-cluster samples), this is quadratic in the stop's
sample count, not the claimed `O(n log n)`.

Impact: an unauthenticated caller can tie up a worker for 15+ seconds
with one request that is within the service's own published limits.
The per-IP rate limit (`RATE_LIMIT_INSIGHTS = "30/minute"`) does not
prevent this — even a few such requests before the limiter engages
already produces tens of seconds of sustained CPU load on a stateless,
unauthenticated endpoint.

---

## Category 7 — Swapped lat/lon, mixed units, precision extremes (1e-15)

**Verdict: CLEAN**

- Swapped lat/lon (both valid ranges, e.g. `lat=45,lon=30` then
  `lat=30,lon=45`) → `200`, computed as two structurally valid but
  geographically different points; no crash, no silently-wrong
  arithmetic (this is a semantic ambiguity the API cannot detect, not a
  bug).
- Precision extreme (`lat=1e-15, lon=1e-15`) → `200`, correctly rounds
  to a duplicate against a second `2e-15` point at the 7-decimal dedup
  granularity used by `prepare_track`; no crash.
- Antipodal points (`(90,180)` vs `(-90,-180)`) → `200`,
  `total_distance_km: 20015.09` (correct half-circumference), no
  `math domain error`. The vectorized `haversine_vec_m`
  (`bhulan/analytics/geodesy.py:29-50`) correctly `np.clip`s the
  intermediate value to `[0, 1]` before `arcsin(sqrt(...))`. (The
  *scalar* `haversine_m`, used only by `merge_nearby_stops`, clamps the
  upper bound via `min(1.0, a)` but not a lower bound of `0.0` — tested
  antipodal-with-`merge_stops_within_m` directly and did not reproduce
  a domain error in practice, since `a` is a sum of squares and cannot
  go meaningfully negative in float64 for these inputs; noting this as
  an theoretically-fragile-but-not-reproducing area for a future pass
  rather than a confirmed defect.)
- Device-reported `speed_mps: 1e300` → `200`, accepted and reflected
  unclamped into `max_speed_kmh: 3.6e+300`. `PointIn.speed_mps`
  (`bhulan/analytics/insights.py:50`) only has a `ge=0` floor, no
  ceiling (unlike the ingestion-only `TrackPoint.speed_mps`, capped at
  120 m/s in `bhulan/models/canonical.py`). This does not crash or
  produce mathematically wrong output — it's a policy gap (no physical
  plausibility check on this surface), not a defect meeting this
  audit's bar; flagged here as an observation for a future hardening
  pass rather than a reproducing test.

---

## Category 8 — Unicode / control chars / very long strings in name/string fields

**Verdict: CLEAN**

Sent to the only free-text fields on this surface (`label` on
`/v1/insights` and `CompareTrack.label`, plus the `text` parse-input
field):
- `label` containing a null byte, raw control chars (`\x01\x02\x03`),
  ANSI escape sequences, RTL override + emoji + `<script>` → `200` in
  every case, value accepted and echoed/ignored per the anonymous-user
  history path (no crash, no mangling observed in the response).
- `label` over 120 chars (both `/v1/insights` and `/v1/compare`) →
  `422 string_too_long`, `max_length=120` correctly enforced.
- `text` field with null/control bytes in CSV cells → `200`, row
  correctly rejected via the quality channel
  (`"No coordinates could be parsed..."`), not a crash.
- A 2000-char single line with no delimiters as `text` → `200`, treated
  as zero parseable rows, not a crash.

---

## Category 9 — Malformed transport (wrong content-type, truncated JSON, deeply nested JSON, extra/unknown fields, missing required fields)

**Verdict: DEFECT** — `tests/adversary/test_deeply_nested_json_recursion_crash.py`

### Clean sub-cases
- Truncated JSON body → `422 json_invalid`.
- Wrong `content-type: text/plain` with a JSON-shaped body → `422
  model_attributes_type` (FastAPI doesn't parse it as JSON at all, body
  arrives as a raw string, cleanly rejected against the expected
  object shape).
- Non-JSON body with `content-type: application/json` → `422
  json_invalid`.
- Extra/unknown top-level fields (`{"points":[...], "unknown_field":
  "hello"}`) and extra/unknown per-point fields
  (`{"lat":1,"lon":1,"extra_field":"x","__proto__":"y"}`) → `200`,
  silently ignored per pydantic's default (non-strict) extra-field
  policy — no crash, no unexpected leakage of the extra keys into the
  response.
- Missing `points`/`text` entirely → `400` (custom check in
  `_materialize_points`).
- Missing `lat`/`lon` on a point → `422 missing`.
- Missing `tracks` on `/v1/compare` → `422 missing`.
- Empty request body → `422 missing`.

### Defect sub-case: deeply nested JSON
Sent: `{"points": [[[[...[1]...]]]]}` with 2000 levels of array
nesting (a few KB payload) to all three endpoints.

Expected: `422` (pydantic should reject a deeply-nested list as an
invalid `PointIn`, same as any other malformed `points` element).

Actual: `500 Internal Server Error`.

Root cause: this is the same failure pattern as the NaN/Infinity defect
in category 2 — pydantic *does* correctly reject the value, but
rendering the rejection crashes. `fastapi.encoders.jsonable_encoder`
recurses once per nesting level while serializing the
`RequestValidationError`'s echoed `"input"` field for the 422 body.
Past ~1000 levels (Python's default recursion limit), this raises
`RecursionError: maximum recursion depth exceeded` while building the
error response, surfacing as an unhandled 500. Confirmed via direct
traceback capture (`raise_server_exceptions=True`): the recursion is
entirely inside `fastapi/encoders.py:jsonable_encoder`, not in any
`bhulan` code — the vulnerability is in how the framework renders
*any* rejected deeply-nested input, and `bhulan`'s `points` field
(`List[PointIn]`, no depth limit at the pydantic level) is simply a
reachable path to it from an unauthenticated caller.

---

## Category 10 — Injection-looking values in string fields

**Verdict: CLEAN**

Sent as `label` (and one CSV `text` cell) on `/v1/insights` and
`/v1/plot/validate`:
- SQL: `'; DROP TABLE users; --`
- NoSQL: `{"$where": "1==1"}`
- Script: `<script>alert(1)</script>`
- Template/SSTI-shaped: `{{7*7}}${7*7}#{7*7}`
- Path traversal: `../../../../etc/passwd`
- Shell/command: `$(rm -rf /); \`id\``
- CSV-formula injection: `=cmd|'/c calc'!A1`

All `200`, value accepted as an opaque string with no evaluation,
execution, or query construction observed. Expected — these endpoints
are stateless (no SQL/NoSQL query building, no shell execution, no
template rendering of user input) per the `insights.py` module
docstring, so there is no injection sink reachable from this surface.
