# ADR 0015 — Uniform 422 for a too-deeply-nested request body

**Status:** accepted (cockpit decision, owner-confirmed)
**Date:** 2026-07-21
**Related:** [[0012]] (depth-guarded untrusted JSON in the *parse* path)

## Context

A tiny deeply-nested JSON body — `{"points": [[[…1…]]]}`, `O(depth)` bytes —
sent to a structured endpoint (`/v1/insights`, `/v1/plot/validate`,
`/v1/compare`) originally 500'd: the stdlib json decoder raised `RecursionError`
while parsing the request body, or `jsonable_encoder` recursed while rendering
the 422 error body. Those crashes were since contained, but *inconsistently* by
depth:

- depth ≤ ~200 → normal pydantic 422 (invalid points),
- depth ~500–900 → 422 `too_deeply_nested` (the `RequestValidationError`
  handler's `RecursionError` catch, [[0012]] era),
- depth ≥ ~1000 → **400** "There was an error parsing the body" (Starlette
  catches the parse-level `RecursionError`).

Same class of malformed input, three different responses. [[0012]] fixed the
*untrusted-text parse* path (`parsers._loads_untrusted`) but not the *structured
request body*, which FastAPI parses itself.

## Decision

A small ASGI middleware (`_JSONBodyDepthGuardMiddleware`) rejects a JSON request
body nested past `MAX_JSON_NESTING_DEPTH` (200) with a uniform **422
`too_deeply_nested`** *before* the body reaches the JSON parser.

- It scans only the first **64 KB** of the raw body for bracket nesting, using
  the early-exiting `parsers.json_nesting_exceeds`. A deep-nesting attack payload
  is a few KB (depth 200 ≈ 400 bytes), so it is caught in full; a legitimate
  large body is shallow, so scanning its 64 KB prefix (≈ 3 ms, measured ~0.8 ms
  on the reject path) finds low depth and passes it through untouched. Scanning
  the whole of a 6.9 MB legitimate body would cost ~280 ms — hence the bound.
- It is registered **inside** the CORS layer (added before CORS, which
  Starlette therefore runs outermost) so the 422 still carries CORS headers a
  browser client needs.
- A pathological body that pads shallow content past the 64 KB scan window
  before nesting deep is **not** flagged here, but still fails safely
  downstream — the parser's own `RecursionError` surfaces as a clean 4xx, never
  a 500. The middleware is for *consistency*, not crash-prevention (the crash is
  already prevented).

## Consequences

- Every over-deep body — structured or free-text, any depth — now returns the
  same clean 422 `too_deeply_nested`. Predictable for API consumers.
- Negligible per-request cost (~0.8 ms) on JSON POST/PUT/PATCH; other requests
  and non-JSON bodies (multipart uploads to `/v1/parse/file`) are untouched.
- The `RequestValidationError` handler's `RecursionError` catch stays as
  belt-and-braces (now rarely reached).
- **Out of scope, flagged as backlog:** a large *shallow* body is a separate DoS
  — the analytics pipeline runs ~0.7 ms/point, so a single unauthenticated
  100 000-point request (the `MAX_PUBLIC_POINTS` cap) ties up a worker for ~73 s.
  The depth guard does not address this; it needs its own cycle.
