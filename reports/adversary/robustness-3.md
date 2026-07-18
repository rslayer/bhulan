# Robustness run 3

A follow-up adversarial pass over the public `/v1` surface, run after the run-2
defects (categories 2, 3, 5, 6, 9) were all fixed. This pass deliberately
targeted the surface run-2 covered least: the **file-upload endpoint**
(`/v1/parse/file`), **non-finite floats reaching a *successful* response** (as
opposed to a rejected one), and the **auth/history** endpoints.

**1 defect found.** Its reproducing test is under `tests/adversary/`
(`test_nonfinite_speed_mps_response_crash.py`). The rest of the probed surface
held up; notable clean results and one non-blocking observation are recorded
below so a future pass does not re-tread them.

---

## Category 11 — Non-finite float in a *successful* response (`speed_mps`)

**Verdict: DEFECT** — `tests/adversary/test_nonfinite_speed_mps_response_crash.py`

Sent a single point carrying a pathological `speed_mps`:

- `{"points":[{"lat":1.0,"lon":1.0,"speed_mps":1e400}]}` — `1e400` parses to
  `inf`.
- `{"points":[{"lat":1.0,"lon":1.0,"speed_mps":1e308}]}` — **finite** on input,
  but overflows to `inf` during the analytics.
- The same via the `text` parse path: `"lat,lon,ts,speed\n1.0,1.0,,1e400\n"`.

Expected: a clean `4xx` (structured) or a dropped row (parsed text/file).

Actual: `500 Internal Server Error` on `/v1/insights`, `/v1/plot/validate`, and
`/v1/compare`, plus the `text` path. Confirmed via direct traceback capture:

```
ValueError: Out of range float values are not JSON compliant: inf
  starlette/responses.py -> json.encoder (allow_nan=False)
```

Root cause: `PointIn.speed_mps` (`bhulan/analytics/insights.py`) had only a
`ge=0` floor — no ceiling and no finiteness guard. `inf >= 0` is True, so `inf`
was accepted; and a finite value near `1e308` overflows to `inf` when the
summary's `max_speed_mps` is multiplied by `MS_TO_KMH` (3.6) to report
`max_speed_kmh` (`compute_insights` line ~431, `build_trip` line ~264). The
non-finite float then reaches Starlette's `JSONResponse`, which serializes with
`allow_nan=False` and raises while *building the 200 body*, surfacing as an
unhandled 500.

This is the same "non-finite float reaches the JSON encoder" class as the run-2
category-2 NaN/Infinity defect, but reached through a **successful** response
rather than a rejected one — so the `RequestValidationError` scrubber in
`bhulan/api/app.py` (which only runs on the 422 path) does not cover it.
Category 7 of run-2 noted `speed_mps` had "no ceiling" and flagged it as a
policy gap for a future pass, but tested only `1e300` (whose `* 3.6` stays
finite → `200`) and so did not surface the `inf` crash.

Fix: an implausible `speed_mps` is treated as **missing** rather than rejected.
A `mode="before"` field validator on `PointIn.speed_mps` drops any value that is
non-finite or above `MAX_SPEED_MPS` (1e7 m/s, ~36 million km/h — orders of
magnitude beyond any real GPS-tracked object) to `None`, before it can reach the
`ge=0` constraint or the analytics. `speed_mps` is the only unbounded
caller-supplied float feeding `max_speed_mps`, so this single guard closes every
downstream overflow site.

Rationale for ignoring rather than rejecting: a physically impossible
device-reported speed is bad telemetry, not a malformed request. Failing the
whole point (or the whole batch) over one bad field is disproportionate, and the
pipeline already derives speed from distance/time for points that carry no
`speed_mps` at all — so dropping the value degrades gracefully instead of losing
the reading. Verified: a two-point track whose reported speed is `1e400` still
returns `200` with a distance/time-derived `max_speed_kmh`.

Values that are merely *invalid* rather than implausible — a negative speed, a
non-numeric string — are passed through untouched and still produce the usual
`422`.

**Behavior note:** run-2's `1e300` example (previously accepted with `200` and
reflected as `max_speed_kmh: 3.6e+300`) is now silently ignored, so the response
no longer carries a meaningless magnitude. The request still succeeds.

---

## Category 12 — File upload (`/v1/parse/file`): malformed GPX / KML / FIT, XML attacks

**Verdict: CLEAN**

`/v1/parse/file` dispatches by extension to `gpxpy` (.gpx), stdlib
`xml.etree` (.kml), `fitdecode` (.fit), or the text parsers, and wraps each in a
`ParseError` → `400` funnel. Probed:

- **Billion-laughs** entity-expansion `.kml` → `200` with zero points (Python's
  `xml.etree` does not expand the nested internal entities into content here);
  no hang, no memory blow-up observed at the tested depth.
- **XXE** `.kml` with `<!ENTITY xxe SYSTEM "file:///etc/passwd">` → `400`
  `Invalid KML/XML` — stdlib `xml.etree` does not resolve external entities; no
  file read.
- Malformed / truncated XML `.kml`, `.gpx` → `400`.
- Non-UTF-8 `.gpx`, binary garbage / truncated `.fit` → `400` (broad
  `except Exception → ParseError` in `parse_fit_bytes` / `parse_gpx_bytes`).
- `.kml` `<coordinates>nan,nan</coordinates>` / `1e400,1e400` → `200`, dropped
  by `_safe_point`'s range check (all comparisons with `nan`/`inf` are False).
- Unknown extension with non-UTF-8 bytes → `400`; empty file → `400`; missing
  filename → `422`.

No unhandled exception reachable through this endpoint.

## Category 13 — Auth / history endpoints with the feature disabled

**Verdict: CLEAN (one observation)**

With `BHULAN_AUTH_ENABLED=false` (the default), `/v1/auth/verify`,
`/v1/history`, and `/v1/history/{id}` return a handled
`503 "Authentication is not enabled on this server"` for every input tried
(oversized email, path-traversal-shaped token, 5000-char id). No crash, no DB
access, no leakage.

Observation (not a defect, not fixed here): `503 Service Unavailable` implies a
*temporary* condition, whereas "auth is not enabled on this instance" is a
permanent configuration state — `501 Not Implemented` or `404` would be a more
accurate signal. This is an API-semantics nit, not a robustness defect, and
changing a status code is a product decision left to the maintainers.
