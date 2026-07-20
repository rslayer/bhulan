# ADR 0003 — Single-pass KML Point→timestamp resolution

**Status:** Accepted
**Cycle:** 3
**Date:** 2026-07-20

## Context

`bhulan/analytics/file_parsers.py::parse_kml_bytes` handled the standard
`<Placemark><TimeStamp>…</TimeStamp><Point>…</Point></Placemark>` shape — how
Google My Maps / Earth and most GPS-export tools represent a *dated* waypoint —
by looking up each `<Point>`'s enclosing-Placemark timestamp with a per-Point
call to `_nearest_timestamp(root, elem)`.

The parser uses stdlib `xml.etree.ElementTree`, which has no parent pointers, so
the `elem.getparent()` guard was *always* `None` for every element (only `lxml`
exposes `getparent`). Each `<Point>` therefore triggered a **full walk from the
document root**: `_nearest_timestamp` iterated *every* `<Placemark>` in the whole
document and, for each, `_contains` walked that Placemark's entire subtree
checking identity against the target Point. For `n` Placemarks with one Point
each — the ordinary "n dated waypoints" export — that is **O(n²)**.

Because `POST /v1/parse/file` is unauthenticated and `_enforce_point_cap` only
runs *after* `parse_file_bytes` returns, the full quadratic cost is paid before
any size guard applies. A sub-megabyte, ordinary-looking KML (well under the
25 MB `MAX_UPLOAD_BYTES`) could tie up a worker for minutes — a straightforward
DoS. Found by robustness run 4 (`reports/adversary/robustness-4.md`); acceptance
test `tests/adversary/test_kml_point_timestamp_quadratic_blowup.py`.

## Decision

Resolve all Point timestamps in a **single pass**, then do each per-Point lookup
in O(1).

1. **`_build_point_timestamps(root)`** iterates every `<Placemark>` exactly once.
   For each, it reads the Placemark's timestamp once
   (`_placemark_timestamp`) and applies it to every `<Point>` the Placemark
   contains, recording `id(point) → timestamp` in a dict. Building the whole map
   costs O(total nodes) = O(n); the map is built once per parse, not per Point.
2. **`_placemark_timestamp(pm)`** is the timestamp half of the old
   `_nearest_timestamp` — first `<TimeStamp><when>` else first `<TimeSpan><begin>`
   else `None` — preserving the exact value the old code produced.
3. The Point loop now reads `ts_by_point.get(id(elem))` instead of walking the
   document. Iteration order (driven by `_iter_elems(root, "Point")`) is
   unchanged, so output is byte-for-byte identical for valid KML.
4. `_nearest_timestamp` and `_contains` (the per-Point root walk and subtree
   membership check) are removed — nothing else referenced them.

### Ordering / correctness parity

- **First Placemark wins.** `setdefault` keeps the first-in-document-order
  Placemark's timestamp when a Point is (pathologically) nested in more than one
  Placemark — matching the old `_nearest_timestamp`, which returned the first
  enclosing Placemark found in `_iter_elems` order.
- **No timestamp / not in a Placemark.** A Point with no Placemark timestamp, or
  outside any Placemark, is absent from the map, so `.get(...)` returns `None` —
  identical to the old behaviour (Point kept, timestamp `None`).
- **Multiple Points per Placemark** all receive the same Placemark timestamp, as
  before.

## Consequences

- KML parsing of the `<Placemark><TimeStamp>+<Point>` shape is O(n): the
  acceptance test's growth/wall-clock bound at n/2n/4n/8n passes (AC1). A
  thousands-of-placemarks legitimate file parses well under the worker timeout
  (AC3).
- No output regression: same points/timestamps/order for valid KML; the
  `<gx:Track>` path, the no-timestamp path, and multi-Point Placemarks are
  untouched (AC2, AC4). GPX and FIT parsing are not touched.
- No size cap added, no `lxml` dependency, no new request field, no API-shape or
  schema change — stdlib `ElementTree` only.
- `stops.py` / `hotspots.py` / `trips.py` are untouched. All parser unit tests
  and cycles 1–2's gap-aware analytics tests stay green (AC5).

## Alternatives considered

- **Switch the parser to `lxml` for `getparent()`.** Rejected by the spec — a new
  dependency and a larger change than needed; a single-pass stdlib map is the
  intended fix.
- **Cap input size / point count before parsing.** Rejected — it changes
  behaviour for legitimate large files; the goal is that a legitimate large KML
  parses *fast*, not that it is rejected.
- **Memoize `_nearest_timestamp` per-Placemark.** Still pays one full root walk
  per Point to find the Placemark; does not remove the quadratic. A direct
  Placemark→Point map is both simpler and genuinely O(n).
