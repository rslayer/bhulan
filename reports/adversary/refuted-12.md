# Refuted findings — cycle 12

Scope: all tests currently failing under `poetry run pytest tests/adversary/
-q` at the start of this pass (11 failing cases across 6 files). Each was
re-run in isolation (and, where relevant, with `--no-cov` and via direct
unit-level benchmarking) against `bhulan/analytics/stops.py`,
`bhulan/analytics/mobility.py`, `bhulan/analytics/file_parsers.py`,
`bhulan/analytics/geodesy.py`, `spec/spec.md`, and `spec/adrs/0011-*.md`.

**2 of 11 failing cases refuted (2 files deleted). 9 kept as real defects.**

## Refuted

### `test_merge_nearby_stops_equal_weight_lon_not_byte_identical.py` (1 case)

Claimed AC4 violates spec's "byte-identical for equal-`sample_count` groups"
because cycle 9's weighted circular-mean `lon` (via
`atan2(Σsin, Σcos)`) differs from cycle 8's `circular_mean_lon` (a plain
arithmetic mean on its non-antimeridian fast path) by `1.421e-14` degrees —
about 1.6 nanometers on the ground.

Refuted: ADR 0011 (Accepted, cycle 9 — the authoritative design record for
this exact change) explicitly names and accepts this precise discrepancy in
its own "Consequences" section: *"For equal weights the weighted circular
mean equals the unweighted `atan2` circular mean (the constant weight
factors out); the tolerance-based regression tests are unaffected by the
~1e-13° float difference between the `atan2` mean and the prior arithmetic
mean in the non-antimeridian case."* That is not an oversight the ADR failed
to notice — it is the documented, deliberate scope of "byte-identical":
`lat`/`radius_m`/`duration_s`/indices/`sample_count` stay bit-for-bit, and
the acceptance test the ADR actually points to
(`tests/unit/analytics/test_stops.py::test_merge_nearby_stops_combines_close_stops`)
never asserts `lon` at all — confirmed by reading it directly. Two
mathematically-equivalent formulas for the same equal-weight mean (plain
sum-and-divide vs. sin/cos/atan2) are not required to agree at the last bit
of a `float64`; that's ordinary floating-point non-associativity, not a
behavioral regression, and no caller-visible consequence follows from a
1.4e-14° difference. The test's own docstring even quotes the ADR's carve-out
verbatim while arguing past it — treating "the spec's literal words say
byte-identical" as controlling over the design record that coined and scoped
that exact phrase. Killed as asserting a preference (bitwise float equality)
the project's own accepted design decision already considered and rejected
as the relevant bar.

### `test_stop_detection_quadratic_blowup.py` (1 case)

Claimed `detect_stops` is O(n²) (not O(n log n) as an outdated part of the
module docstring once said), timing a 20,000-point single-cluster track at
~0.52–0.53s against a 0.5s budget under the default `pytest tests/adversary/
-q` invocation (which enables `--cov=bhulan` via this project's
`pyproject.toml` `addopts`).

Refuted: the current `detect_stops`/`_cluster_end` (see the module docstring
and the `r_est` running-bound logic in `bhulan/analytics/stops.py`) already
carries the O(n) fix — and has since commit `8593f1a` ("Fix NaN->500 crash
and quadratic stop-detection DoS"), well before cycle 7. I benchmarked
`detect_stops` directly (bare Python, no HTTP, no coverage) on the test's
exact point pattern at 5,000/10,000/20,000/40,000/80,000 points:
`0.0064s / 0.0125s / 0.0248s / 0.0499s / 0.0998s` — a clean ~2x-per-doubling
curve, i.e. linear, not the ~4x-per-doubling an O(n²) algorithm would show
(80,000 points, 4x the test's own cluster size, finishes in 0.0998s total —
if the test's claimed quadratic growth were real this would take seconds,
not a tenth of a second). The full `/v1/insights` endpoint (in-process
`TestClient`, no coverage) also scales linearly: `0.074s / 0.156s / 0.265s /
0.575s` for the same sizes. Re-running the exact failing test with `--no-cov`
passes reliably (5/5 runs, ~0.3s wall for the request) — the ~0.52s consistently
observed under the default coverage-instrumented run is coverage.py's
per-line tracing overhead across the full request pipeline (JSON
parse/validate + mobility segmentation + stops + hotspots + trips, ~2,600
instrumented statements), not input-size-driven blowup in `detect_stops`.
The test's specific causal claim ("no KD-tree... `_cluster_radius_m`
recomputes over the entire window on every growth step") describes code that
predates the `r_est` optimization and does not match what's on disk today.
Since the claimed mechanism is directly disproven by controlled scaling
measurement, and the pass/fail is instead an artifact of an absolute-time
budget colliding with this project's own always-on `--cov` pytest default,
this is a broken assertion against stale root-cause reasoning, not a live
algorithmic defect. Killed.

## Kept (9 cases, real defects — left untouched)

- `test_merge_nearby_stops_heavy_dwell_drags_centroid_far_outside_merge_radius.py`
  (1 case): re-verified independently (isolated run, `--no-cov`, and by
  hand-checking the weighted-centroid math). AC2's general claim in
  spec/spec.md §2 and ADR 0011 — "the centroid... lies within
  `merge_radius_m` of every member" — is false in general; it only held for
  the symmetric scenario the cycle-9 acceptance test happens to check. A
  heavy dwell (`sample_count`≈300) followed by a 3-hop walk-away chain
  (each hop 40m, comfortably inside `merge_radius_m=45`) produces a
  `sample_count`-weighted centroid that lands ~118m from the farthest
  member — 2.6x the merge radius — using an entirely ordinary GPS pattern
  (idling, then rolling forward), not the out-of-scope "genuinely long
  drift chain" span-cap backlog item. Real, and a materially worse
  violation than the bug this cycle fixed.
- `test_parse_file_free_text_deep_nesting_recursion_crash.py` (5 cases):
  re-verified with `--no-cov` — all 5 still 500. Traced the code path:
  `parse_file_bytes` decodes non-`.gpx`/`.kml`/`.fit` uploads as text and
  hands them to `parsers.parse_any` → `parse_json`, which calls stdlib
  `json.loads` directly with no `RequestValidationError`/`ParseError`
  wrapper in that path, so CPython's `RecursionError` on deep array nesting
  propagates as an unhandled 500 on four public endpoints. Distinct root
  cause from the already-known structured-`points` recursion crash (which
  *is* caught, via the custom `RequestValidationError` handler). Matches
  spec/spec.md §4's own backlog line ("Deeply-nested-JSON recursion → 500").
  Real.
- `test_pole_dwell_stop_false_negative.py` (1 case): pre-existing
  (committed at cycle 7, not part of this cycle's new work), explicitly
  named in spec/spec.md §4 as a known, still-open backlog item. Confirmed
  the `latlon_to_xy_m` linear-longitude-projection degeneracy near the pole
  is real and unfixed. Left untouched.
- `test_zero_time_delta_movement_misclassified_as_stopped.py` (2 cases):
  re-verified with `--no-cov` — both still fail identically. Traced
  `bhulan/analytics/mobility.py`: `segment_by_motion`'s
  `np.where(step_secs > 0, step_dist / step_secs, 0.0)` and
  `speed_stats_mps`'s `if s > 0` guard both silently zero the speed for a
  zero-elapsed-time hop, while the segment's `distance_m` is summed from the
  same `step_dist` array unconditionally — producing a self-contradictory
  response (`kind: "stopped"`, `distance_km: 2.11`, `max_speed_kmh: 0.0`, no
  `quality.issues` entry). Confirmed by direct code read, matches the test's
  root-cause claim exactly. Real.

## Note on 429s in the full-suite run

Several of the above (`test_stop_detection_quadratic_blowup.py` before
deletion, both `test_zero_time_delta_movement_misclassified_as_stopped.py`
cases) showed `429 Too Many Requests` rather than their documented failure
mode when run as part of the *full* `tests/adversary/` directory — the
per-IP `slowapi` rate limiter (`30/minute`, keyed on the fixed
`TestClient`/`testclient` host) accumulates across every test module in one
pytest process, since the underlying `app`/limiter singleton is imported
once and shared. This is test-harness cross-pollution, not a product defect
in its own right, and not something a refuter should paper over: each
affected test was re-run in isolation to recover its true underlying
verdict rather than being judged on the 429.

## Disposition

- Deleted `tests/adversary/test_merge_nearby_stops_equal_weight_lon_not_byte_identical.py`
- Deleted `tests/adversary/test_stop_detection_quadratic_blowup.py`
- All other failing tests left untouched.
