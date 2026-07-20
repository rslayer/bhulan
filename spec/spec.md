# Product Spec — bhulan

> Living spec. The cockpit writes it; the loop (`.github/workflows/loop.yml`)
> reads it, builds, iterates (eval → adversarial → robustness → refute →
> triage), then opens a PR. The loop NEVER merges to master and NEVER deploys —
> you do. bhulan is a public demo; keep it PR-gated.

**Status:** cycle 3
**Cycle of last revision:** 3

---

## 1. This cycle's single outcome

**Make `parse_kml_bytes` linear (O(n)) for the `<Placemark><TimeStamp>+<Point>`
shape** — currently O(n²), an unauthenticated DoS on `/v1/parse/file` within
normal usage.

Found by robustness run 4 (`reports/adversary/robustness-4.md`). Root cause is
precise: `bhulan/analytics/file_parsers.py::parse_kml_bytes` (~lines 134-142),
for **every** `<Point>` element, calls `_nearest_timestamp(root, elem)` to find
its enclosing Placemark's timestamp. Because it parses with stdlib
`xml.etree.ElementTree` (no parent pointers), the `elem.getparent()` check is
**always None**, so each Point triggers a **full walk from the document root**:
`_nearest_timestamp` iterates every `<Placemark>` in the whole document and
`_contains` walks each Placemark's subtree. For n Placemarks (one Point each) —
exactly how Google My Maps / Earth exports dated waypoints — that is O(n²),
with no exotic input.

(`tests/adversary/test_kml_point_timestamp_quadratic_blowup.py`)

## 2. Hard constraints

- **The named test is the acceptance test** — it already exists and is failing
  (it asserts a growth bound / wall-clock ceiling at n, 2n, 4n, 8n). Fix the
  product code to make it pass; do NOT weaken it.
- **Fix the algorithm, not a symptom.** Do the timestamp lookup in a **single
  pass**: walk the document once, building a map from each `<Placemark>` (or its
  contained `<Point>`) to its timestamp, then resolve each Point in O(1) — rather
  than scanning the whole document per Point. Equivalent alternative: iterate
  Placemarks once and, for each, read its own Point(s) + timestamp together.
- **Do NOT cap the input size as the fix.** A size cap changes behaviour for
  legitimate large files; the goal is that a legitimate large KML parses fast.
- **Do NOT switch the whole parser to `lxml`** just to get `getparent()` — that
  is a new dependency and a larger change than needed. A single-pass map with
  stdlib `ElementTree` is the intended fix. (If a Placemark→child index is
  cleaner via `ElementTree`'s own iteration, use that.)
- **Output must be byte-for-byte unchanged** for a correctly-formed KML — same
  points, same timestamps, same order. GPX and FIT parsing must be untouched.
- **Preserve every existing passing test** — the parser unit tests, and cycles
  1–2's gap-aware analytics tests, must stay green.
- Backend/parsing only. No new dependencies, no API-shape changes, no DB/schema
  changes. Do not touch the analytics (`stops.py`/`hotspots.py`/`trips.py`).

## 3. Non-negotiable acceptance criteria

- **AC1:** parsing a KML with n `<Placemark><TimeStamp>+<Point>` elements is
  **sub-quadratic** — the growth assertion in the acceptance test passes at n,
  2n, 4n, 8n.
- **AC2:** a normal, correctly-formed KML still parses to exactly the same
  points/timestamps/order as before (no output regression); GPX and FIT parsing
  unchanged.
- **AC3:** a large-but-legitimate KML (thousands of placemarks) parses well
  under the worker timeout, with no size-cap rejection.
- **AC4:** edge shapes still parse correctly — a `<gx:Track>` KML, a Placemark
  with no timestamp (Point kept, timestamp None), multiple Points per Placemark.
- **AC5:** `poetry run pytest tests/unit/ tests/adversary/test_kml_point_timestamp_quadratic_blowup.py -q` is green.

## 4. Known traps for the adversary to probe next

- Other per-element full-document walks: does GPX/FIT parsing have the same
  O(n²) shape anywhere?
- XML entity expansion / billion-laughs on `/v1/parse/file` (a different DoS).
- Deeply-nested KML `<Folder>` trees, malformed/truncated KML mid-element.
- Memory growth (not just time) with document size.
- Any remaining metamorphic properties in the analytics after cycles 1–2.

## 5. Definition of done for this cycle

- AC1–AC5 pass. KML timestamp resolution is a single pass; large legitimate
  files parse fast; no size-cap band-aid; no output or GPX/FIT regression; no
  `lxml` dependency added.
- ADR recorded in `spec/adrs/`.
- A PR is opened for review. **No merge to `master` without your review.**

## 6. Deploy target

None from the loop. The loop opens a PR; you review and merge. bhulan is a
public demo — production stays gated.

---

## Change log
- cycle 1 — cockpit — time-gap-aware detect_stops/detect_hotspots.
- cycle 2 — cockpit — gap-aware merge_nearby_stops (sibling re-merge).
- cycle 3 — cockpit — make parse_kml_bytes O(n): resolve each Point's Placemark
  timestamp via a single-pass map instead of a full document walk per Point.
