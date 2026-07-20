# Product Spec — bhulan

> Living spec. The cockpit writes it; the loop reads it, builds, iterates
> (eval → adversarial → robustness → refute → triage), then opens a PR.
> The loop NEVER merges to master and NEVER deploys — you do. bhulan is a public
> demo; keep it PR-gated.

**Status:** cycle 10
**Cycle of last revision:** 10

---

## 1. This cycle's single outcome

**Stop deeply-nested JSON input from crashing the public API with a 500.** A
deeply-nested JSON payload (tiny — `O(depth)` bytes, e.g. `[[[…1…]]]` ~20 KB)
sent to any endpoint that parses untrusted text makes `json.loads` raise
`RecursionError`, which is uncaught and surfaces as an unauthenticated **HTTP
500** (a crash / availability defect), instead of a clean 4xx rejection.

`bhulan/analytics/parsers.py` calls `json.loads` directly on untrusted input at
three sites (lines ~238, ~273 in `parse_json`, and ~352 in `parse_any`). It is
reachable unauthenticated through `POST /v1/parse/file` (a `.json`/plain-text or
any non-`.gpx/.kml/.fit` upload), and through the free-text `points`/track fields
of `/v1/insights`, `/v1/compare`, and `/v1/plot/validate`.

Acceptance tests (already on master, currently red — 8 cases across two files):
`tests/adversary/test_parse_file_free_text_deep_nesting_recursion_crash.py` and
`tests/adversary/test_deeply_nested_json_recursion_crash.py`.

## 2. Hard constraints

- **The named tests are the acceptance tests** — they already exist and are
  failing. Fix the product code; do NOT weaken them. (Read each test to confirm
  the expected status: a clean client error, **not** 500, and not a hang.)
- **Fix at the root, once.** Add a single shared depth-guarded JSON loader (e.g.
  `parsers._loads_untrusted(text, max_depth=...)`) and use it at every
  `json.loads` site that consumes untrusted input. Prefer a **pre-scan of the
  raw text's maximum bracket/brace nesting depth** and reject before decoding
  (raise a `ValueError`/domain error the API layer already maps to 4xx), rather
  than relying on catching `RecursionError` after the fact (which can leave the
  interpreter state fragile). If `RecursionError` is caught as a belt-and-braces
  fallback, convert it to the same clean 4xx.
- **Pick a sane cap.** A nesting depth of, say, 200 is far beyond any real
  GPS/track document yet well below CPython's ~1000 recursion limit. Document
  the chosen cap. Valid inputs (real GPX/KML/FIT and ordinary `points` arrays,
  which are shallow) must be entirely unaffected.
- **Every affected endpoint returns a clean client error, never 500 and never a
  hang**, for the deep-nesting payload: `/v1/parse/file`, `/v1/insights`,
  `/v1/compare`, `/v1/plot/validate`.
- **Also cover the structured-payload variant** if it shares the path — the
  second test file targets a deeply-nested `points` array whose 422 error body
  recurses during rendering; ensure that too returns a clean bounded error.
- **Do NOT regress cycles 1–9** and all currently-passing parser/unit tests. Do
  NOT change the response shape for valid input or for ordinary invalid input
  (a normal malformed body must still return its current 4xx).
- Backend/analytics + API-error-handling only. No new dependencies, no DB/schema
  changes.

## 3. Non-negotiable acceptance criteria

- **AC1:** a deep-nesting JSON upload to `/v1/parse/file` returns a clean 4xx
  (client error), not 500 and not a hang.
- **AC2:** the deep-nesting free-text field on `/v1/insights`, `/v1/compare`, and
  `/v1/plot/validate` each return a clean 4xx, not 500.
- **AC3:** the deeply-nested structured `points` array returns a clean bounded
  4xx (no recursion crash while rendering the error body).
- **AC4:** valid inputs and ordinary malformed inputs are unchanged — real
  track documents parse as before; a normal bad body returns its current 4xx.
- **AC5:** `poetry run pytest tests/unit/ tests/adversary/test_parse_file_free_text_deep_nesting_recursion_crash.py tests/adversary/test_deeply_nested_json_recursion_crash.py -q` is green.

## 4. Known traps for the adversary to probe next (backlog / product decisions)

- **Transitive-merge span cap (PRODUCT DECISION, owner):** heavy-dwell /
  runaway-chain findings — a merged stop's centroid can't stay within
  `merge_radius_m` once the chain spans > 2× it. Owner's call whether to cap.
- **Zero-time-delta movement** (`test_zero_time_delta_movement_*`): segments with
  identical timestamps mis-handled in speed/distance.
- **Polar dwell false-negative** (`test_pole_dwell_stop_false_negative.py`).
- Non-finite reported `speed_mps`; datetime-overflow / malformed-type.
- Replace the deleted wall-clock `detect_stops` DoS test with a **structural**
  (operation-count) O(n) regression test — no timing threshold.

## 5. Definition of done for this cycle

- AC1–AC5 pass. One shared depth-guarded untrusted-JSON loader; all four
  endpoints return a clean 4xx (never 500) on deep nesting; valid/ordinary input
  unchanged; cycles 1–9 green.
- ADR recorded in `spec/adrs/` (document the depth cap and the pre-scan approach).
- A PR is opened for review.

## 6. Deploy target

None from the loop. The loop opens a PR; you review and merge. bhulan is a
public demo — production stays gated.
