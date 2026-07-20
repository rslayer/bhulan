# Adversary cycle 6 — `parse_kml_bytes` single-pass fix

Scope: attack this cycle's fix (spec/spec.md, "Make `parse_kml_bytes` O(n)")
— probe the spec's "known traps" first, then edge inputs around the fix.
Product code under `bhulan/` treated as read-only.

## Verified as fixed / not a bug

- **AC1 acceptance test** (`test_kml_point_timestamp_quadratic_blowup.py`,
  the flat "n sibling `<Placemark><TimeStamp>+<Point>`" shape) is green —
  1,500 points parse in well under the 0.3s ceiling. Directly measured
  through `/v1/parse/file`: 1,000-point flat KML ≈ 0.023s, 2,000-point flat
  KML ≈ 0.035s — comfortably linear.
- **Known trap "does GPX/FIT have the same O(n²) shape anywhere?"** — read
  `parse_gpx_bytes` and `parse_fit_bytes`: both iterate their source
  library's own point/frame stream once and append directly
  (`_append_gpx_point`, `_fit_field` per frame); no per-element walk back
  over the document/stream. No bhulan-authored quadratic code found in
  either path.
- **Known trap "XML entity expansion / billion-laughs on `/v1/parse/file`"**
  — tried a 5-level and a 9-level (`10^9`-if-fully-expanded) entity-expansion
  KML payload through `parse_kml_bytes`. Both parsed in ~0.15ms with zero
  points; CPython's stdlib `expat` backing `xml.etree.ElementTree` has had
  built-in entity-expansion amplification limits since 3.7.1/3.9, and they
  are in effect here. **No exploitable billion-laughs DoS found.**
- **Known trap "deeply-nested KML `<Folder>` trees"** — 5,000 levels of
  nested `<Folder>` wrapping a single Placemark/Point parsed in ~3.5ms, no
  `RecursionError`. `ElementTree`'s C parser doesn't recurse per XML nesting
  depth, so Python's 1000-frame recursion limit isn't at risk here.
- **Known trap "malformed/truncated KML mid-element"** — confirmed
  `fromstring` raises `XmlParseError`, caught and re-raised as `ParseError`
  (existing unit-test-covered path); no new defect.
- Multi-Point-per-Placemark and no-timestamp Points (AC4) spot-checked
  directly against `parse_kml_bytes` — correct.

## New defect found (failing test added)

### `_build_point_timestamps` is still O(n²) for nested (non-sibling) Placemarks

`tests/adversary/test_kml_nested_placemark_quadratic_blowup.py`

The cycle-3 fix (`bhulan/analytics/file_parsers.py::_build_point_timestamps`,
lines 230-250) iterates every `<Placemark>` once — genuinely O(n) *when
Placemarks are flat siblings*, which is what the acceptance test covers. But
for each Placemark it calls `_iter_elems(pm, "Point")`
(line 248 → `pm.findall(".//Point")`), which walks **that Placemark's
entire subtree**. Nothing in the KML format or this code prevents
`<Placemark>` elements from being nested inside one another (well-formed
XML; some third-party re-export tools preserve folder-like grouping via
nesting rather than flat lists). For a "Russian doll" of `n` nested
Placemarks (Placemark₀ ⊃ Placemark₁ ⊃ … ⊃ Placemarkₙ₋₁), each with its own
Point, the subtree walk from Placemarkₖ visits all `n-k` Points nested
inside it, so the total work is `n + (n-1) + … + 1 = O(n²)` — the exact
complexity class the cycle-3 fix was supposed to eliminate, reintroduced
one level down.

Measured through the public endpoint (in-process, no network), same point
count, flat vs. nested structure:

| n (points) | flat-shape wall time | nested-shape wall time |
|---|---|---|
| 1,000 | 0.023s | 0.34s (~15x slower) |
| 2,000 | 0.035s | ~1.9s |

Direct-function timing on the nested shape alone shows the quadratic
signature clearly: n=200 → 0.017s, n=400 → 0.056s (3.3x), n=800 → 0.214s
(3.8x), n=1,600 → 0.855s (4.0x) — each doubling roughly quadruples time, not
doubles.

Impact: an unauthenticated caller can still trigger the exact DoS the
cycle-3 fix targeted on `POST /v1/parse/file`, just by nesting `<Placemark>`
elements instead of listing them as flat siblings — a small structural
tweak to the same "n dated waypoints" KML export shape the fix was written
against.

**Secondary note (not a new correctness regression, but worth flagging for
scoping a future fix):** for this nested shape, `setdefault` at line 249
means the *outermost* Placemark's subtree walk visits every descendant
Point first and locks in its timestamp for all of them — every Point ends
up stamped with Placemark₀'s timestamp, not its own immediate parent's.
This matches the *old*, pre-cycle-3 `_nearest_timestamp`/`_contains`
behavior bug-for-bug (the old code's "first enclosing Placemark in document
order" also resolves to the outermost one for nested input), so it is not a
regression introduced by this cycle's diff — but it means "nested
Placemarks" was never a shape either implementation handled by
per-immediate-parent semantics, which matters if nested Placemarks are ever
brought in-scope for a future correctness (not just performance) fix.

## Coverage-guided notes

Targeted `bhulan/analytics/file_parsers.py` (40% reached per
`coverage-gaps.md`, this cycle's own file). Specifically probed the
previously-unprobed `<gx:Track>` block (lines 152-170, 0% reached before
this cycle): verified interleaved `<when>`/`<gx:coord>` children pair
correctly by document-order-within-tag-type (not by literal interleaving
position) for a 3-point track — correct, no defect found. Did not find
further defects in the LineString path (lines 130-132) or
`_extend_from_coord_string`'s malformed-token handling (lines 187, 191-192)
— both degrade gracefully (skip malformed tuples, keep valid ones).

Did not probe `bhulan/analytics/parsers.py` (CSV/JSON/GeoJSON fallback,
45% reached) or the ingestion-pipeline files (`ingestion/*.py`, mostly 0%
reached but not reachable from the public `/v1` surface under test) this
cycle — out of scope for "this cycle's feature"; flagged for a future cycle
if CSV/JSON/GeoJSON parsing becomes the spec's focus.

## Test files added

- `tests/adversary/test_kml_nested_placemark_quadratic_blowup.py` (failing)

Full adversary suite: 3 failed (1 new + 2 pre-existing backlog findings from
cycle 5 — `test_merge_nearby_stops_radius_m_wrong.py` and
`test_merge_nearby_stops_chain_drift_undermerges.py`, both already failing
before this cycle and out of scope for it), 31 passed. No existing test was
weakened, rewritten, or deleted.
