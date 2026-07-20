# ADR 0012 — Depth-guarded loader for untrusted JSON

**Status:** Accepted
**Cycle:** 10
**Date:** 2026-07-20

## Context

`bhulan/analytics/parsers.py` calls stdlib `json.loads` directly on
untrusted text at three sites (`parse_geojson`, `parse_json`, and
`estimate_input_rows`). These are reachable unauthenticated through:

* `POST /v1/parse/file` — any upload whose name does not end in
  `.gpx`/`.kml`/`.fit` is UTF-8-decoded and handed to `parse_any`.
* the free-text `text` field of `POST /v1/insights`, `POST /v1/plot/validate`,
  and each track's `text` field on `POST /v1/compare`.

A tiny, cheap-to-generate payload — `depth` nested single-element JSON arrays,
`[[[…1…]]]`, costing only `O(depth)` bytes — makes CPython's C JSON decoder
raise `RecursionError` once nesting is deep enough (empirically ~10 000 levels,
well under any file-size or point-count limit the service enforces).
`RecursionError` is **not** a `ParseError`, so none of the route-level
`except ParseError` handlers catch it and it surfaces as an unauthenticated
**HTTP 500** — an availability defect.

This is distinct from the already-handled *structured* `points`-array crash
(`test_deeply_nested_json_recursion_crash.py`), where pydantic rejects the value
and `jsonable_encoder` recurses while rendering the 422 body; that path is
covered by the `RecursionError` fallback already present in the
`RequestValidationError` handler in `bhulan/api/app.py`. The free-text path
crashes *during decoding*, entirely outside that handler's reach.

## Decision

Add a single shared guarded loader in `parsers.py` and use it at **every**
untrusted `json.loads` site:

```python
MAX_JSON_NESTING_DEPTH = 200

def _loads_untrusted(text, max_depth=MAX_JSON_NESTING_DEPTH):
    if _max_json_nesting_depth(text) > max_depth:
        raise ParseError(...)          # ValueError → route maps to clean 4xx
    try:
        return json.loads(text)
    except RecursionError as exc:       # belt-and-braces fallback
        raise ParseError(...) from exc
```

`_max_json_nesting_depth` does a single linear O(len) scan of the raw
characters, tracking string state (so `[`/`{` inside JSON string literals are
not counted) and returning the deepest bracket/brace nesting. Because the guard
runs **before** `json.loads`, the decoder never drives its recursion on a
pathological payload; the `RecursionError` catch is only a fallback and is not
relied upon.

### Chosen cap: 200 levels

A real GPS/track document nests only a handful of levels deep — a flat array of
point dicts is depth 2; a GeoJSON `FeatureCollection` of `LineString` features
is ~5. 200 is orders of magnitude beyond anything legitimate yet comfortably
below CPython's ~1000 default recursion limit and the C decoder's own ceiling,
so it rejects the attack while leaving every valid and ordinary-malformed input
untouched.

`_loads_untrusted` raises `ParseError` (a `ValueError`), which the existing
route handlers already convert to a clean **400** (`/v1/parse/file`,
`/v1/insights`, `/v1/plot/validate`, `/v1/compare` text paths). No response
shape changes for valid input or ordinary malformed input.

## Consequences

* All four endpoints return a clean 4xx (never 500, never a hang) for the
  deep-nesting free-text payload.
* Valid GPX/KML/FIT and ordinary shallow `points`/text inputs are unaffected.
* No new dependencies, no DB/schema changes; the fix is localized to
  `parsers.py`.
* Trade-off: a legitimate document nested beyond 200 levels would be rejected.
  No real GPS/track format approaches this, so the trade-off is safe.
