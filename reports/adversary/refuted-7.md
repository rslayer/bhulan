# Refuted findings — cycle 7

Reviewed all 9 tests failing under `poetry run pytest tests/adversary/ -q`
at the start of this pass: the 3 new cycle-7 test files (7 test cases) plus
2 pre-existing failing tests inherited from earlier cycles.

## Refuted (deleted)

### `tests/adversary/test_stop_detection_quadratic_blowup.py::test_insights_clustered_track_completes_in_reasonable_time`

The test's premise — "`detect_stops` is quadratic in cluster size, not
O(n log n) as documented" — is factually false for the current code. It
describes the *pre-fix* algorithm. `bhulan/analytics/stops.py::_cluster_end`
already carries exactly the O(n)-amortized running-upper-bound guard the test's
own docstring says is missing (added in commit `8593f1a`, "Fix NaN->500 crash
and quadratic stop-detection DoS" — this same test was added in that commit
and passed at the time).

Verified directly:
- Calling `detect_stops` on a single tight cluster and scaling `n` from 5,000
  to 80,000 points gives ~linear growth (0.006s / 0.016s / 0.024s / 0.051s /
  0.098s) — a 16x increase in `n` produces a ~15x increase in time, not the
  ~256x an O(n^2) algorithm would show.
- The full `/v1/insights` endpoint, `--no-cov`, for the test's own 20,000-point
  payload: consistently 0.26-0.31s (5 runs), comfortably under the test's
  0.5s budget — not the "quadratic blowup" the test alleges.
- The same endpoint at 100,000 points (`MAX_PUBLIC_POINTS`, the size the
  test's docstring claims takes "~15.5s"): measured 1.47s. The docstring's
  performance figures are stale, carried over from before the fix.

The failure that *does* reproduce (0.53-0.54s, consistently, under this
project's default `pytest` invocation) is entirely explained by
`coverage`'s `sys.settrace` instrumentation overhead (`addopts = "--cov=bhulan
..."` in `pyproject.toml`) adding a near-constant ~0.25s to every request in
this test file, regardless of `n` — confirmed by re-running with `--no-cov`,
where the same request passes reliably with margin to spare. This is a
timing-threshold test whose asserted defect (algorithmic quadratic blowup) does
not exist in the current implementation; it fails only because an unrelated,
uniform test-harness overhead happens to push a linear-cost, already-fast
request over an arbitrarily tight 0.5s line. Not a defect in product code.
Deleted the test file.

## Kept (not refuted)

All 3 new cycle-7 test files (7 cases across
`test_antimeridian_centroid_reports_wrong_location.py`,
`test_bounding_box_not_minimal_multi_cluster.py`, and
`test_pole_dwell_stop_false_negative.py`) were independently re-verified
against product code and kept as real:

- Antimeridian centroid bug: confirmed `np.mean` over raw longitudes at
  `stops.py:166`, `hotspots.py:219-220`, and the plain midpoint
  `(prev.lon + s.lon) / 2.0` at `stops.py:237` — all three land on ~0.0 for a
  cluster split across +179.9999/-179.9999, verified by direct reproduction.
- `bounding_box` non-minimality: independently recomputed the true
  minimal-span cut for longitudes `[-170, -100, -30, 0, 60, 170]` (largest
  circular gap is 60->170, 110 degrees, leaving a 250-degree minimal box) and
  confirmed `bounding_box()` only evaluates the raw and 0-shifted framings
  (`geodesy.py:76-94`), reporting 330 degrees instead — matches the test
  exactly.
- Polar projection false negative: recomputed independently —
  `haversine_m(89.9999, 0, 89.9999, 180)` = 22.24m true separation (11.12m true
  radius) vs. `latlon_to_xy_m`'s projected cluster radius of 17.49m, which
  exceeds the test's `stop_radius_m=15` and causes the false negative exactly
  as described.

`tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py` (failing,
pre-existing from cycle 5) was left untouched: already independently verified
as a real defect in `reports/adversary/refuted-5.md` (the "sibling re-merge"
in `merge_nearby_stops` comparing against the drifted blob centroid rather
than the original preceding stop) and deliberately deprioritized as backlog,
not fixed by any cycle since. Re-read `merge_nearby_stops` (`stops.py:219-258`)
and confirmed the code is unchanged from that assessment — still a real,
reproducible defect, just not this cycle's target. Out of scope for this
refuter pass; not re-litigated further.
