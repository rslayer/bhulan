# Triage — cycle 12 surviving findings

`poetry run pytest tests/adversary/ -q` → 9 failed, 45 passed. Two of those
9 failures (`test_zero_time_delta_movement_misclassified_as_stopped.py`, both
tests) fail with `429 Rate limit exceeded` when run inside the *full*
adversary suite, because the suite's shared in-process rate-limiter state
(30 req/min per `TestClient`) is exhausted by earlier tests in the same run —
that 429 is a test-harness ordering artifact, not the defect under test. Run
in isolation (`pytest tests/adversary/test_zero_time_delta_movement_misclassified_as_stopped.py`)
both tests fail on their real assertions instead; that real failure is what's
triaged below as finding #4. 8 distinct defects survive (one test file,
`test_parse_file_free_text_deep_nesting_recursion_crash.py`, contributes 5
failing tests that share a single root cause and are triaged as one finding).

| rank | severity | finding (test file::test) | unauth? | blast radius | why it matters |
|---|---|---|---|---|---|
| 1 | critical | `test_parse_file_free_text_deep_nesting_recursion_crash.py` (5 tests: `test_parse_file_deeply_nested_text_upload_returns_500`, `test_parse_file_deeply_nested_json_extension_upload_returns_500`, `test_insights_deeply_nested_text_field_returns_500`, `test_plot_validate_deeply_nested_text_field_returns_500`, `test_compare_deeply_nested_track_text_field_returns_500`) | yes — `/v1/parse/file`, `/v1/insights`, `/v1/plot/validate`, `/v1/compare`, no auth, no special payload beyond one string field | availability / unhandled-exception crash (DoS per request) | A ~20KB string of nested JSON brackets, sent as a file upload or in any of four public endpoints' free-text `text` field, hits `json.loads` directly with no `RequestValidationError` wrapper in the path, raising an uncaught `RecursionError` that 500s the request — reachable on four distinct endpoints with a trivial, cheap-to-generate payload and no rate-limit-defeating volume needed. |
| 2 | high | `test_merge_nearby_stops_heavy_dwell_drags_centroid_far_outside_merge_radius.py::test_heavy_dwell_then_light_walk_merges_but_centroid_leaves_merge_radius` | yes — `/v1/insights` with the documented `merge_stops_within_m` option, ordinary-shaped input | correctness / silently-wrong-answer | An everyday pattern (vehicle idles, then crawls forward in traffic) makes the reported merged-stop location land ~118m from a member it claims to summarize — 2.6x the 45m merge radius and materially worse than the cycle-8 regression this cycle's fix targeted (~49m) — with no error or warning surfaced to the caller. |
| 3 | medium | `test_pole_dwell_stop_false_negative.py::test_polar_dwell_within_true_radius_is_detected_as_a_stop` | yes — `/v1/insights`, but only for coordinates within ~15-20m of true north/south pole | correctness / silently-wrong-answer (false negative) | A genuine dwell physically well inside `stop_radius_m` near the pole is reported as zero stops because the tangent-plane longitude projection degrades near-degenerate meridians there; carried over unresolved from cycle 11's triage — narrow real-world exposure (polar research/tourism traces only). |
| 4 | medium | `test_zero_time_delta_movement_misclassified_as_stopped.py` (2 tests: `test_stopped_segment_does_not_report_multi_km_distance`, `test_zero_time_delta_movement_does_not_silently_zero_max_speed`) | yes — `/v1/insights`, ordinary points with duplicate timestamps (a realistic batch-flush/rounding logging artifact) | correctness / silently-wrong-answer | Consecutive samples sharing an identical timestamp but 2.1km apart get classified as a "stopped" segment with `distance_km: 2.1127` and `max_speed_kmh: 0.0` in the same response — an internally self-contradictory answer with no `quality.issues` entry to flag it, unlike exact-duplicate points which already get a documented warning. |

## Critical/High detail

**#1 — Free-text JSON recursion crash on four public endpoints (critical).**
`bhulan/analytics/file_parsers.py::parse_file_bytes` decodes any upload whose
filename doesn't end in `.gpx`/`.kml`/`.fit` as raw text and hands it to
`bhulan.analytics.parsers.parse_any`, which for text starting with `[` or `{`
calls stdlib `json.loads` directly — outside the `RequestValidationError`
handler that already guards the *structured* `points`-array recursion case
(that handler explicitly catches `RecursionError` when scrubbing a 422 body).
`json.loads` raises an uncaught `RecursionError` at ~10,000 levels of array
nesting regardless of `sys.setrecursionlimit()`, well under any file-size or
point-count limit the service enforces, and none of the `except ParseError`
handlers in `bhulan/api/routes/insights.py` / `compare.py` catch a
`RecursionError`. This is the top finding because it is unauthenticated,
requires no special input shape, costs the attacker only `O(depth)` bytes
(~20KB) to send, and reproduces identically across `POST /v1/parse/file`
(via file upload, two content-type/extension variants), `POST /v1/insights`
(`text` field), `POST /v1/plot/validate` (`text` field), and `POST
/v1/compare` (a track's `text` field) — a single root cause with the widest
blast radius of anything surviving this cycle.

**#2 — Heavy-dwell-then-walk-away centroid breaks AC2 by 2.6x the merge
radius (high).** Cycle 9's fix to `_merge_members`
(`bhulan/analytics/stops.py`) made the merged centroid a
`sample_count`-weighted mean, and was verified only against a symmetric
scenario (brief, dense, brief, with the dense member in the middle). Once the
chain instead runs *past* the heavy member — one 300-sample dwell followed by
three brief (2-sample) stops walking away in a straight line, each hop
exactly 40m (comfortably inside `merge_stops_within_m=45`, so the single-
linkage merge decision is correct and unaffected) — the ~300:1 weight
imbalance pins the centroid within ~2m of the heavy dwell and abandons the
light members entirely: the farthest ends up ~118m away against a 45m merge
radius, and even the middle stop is already outside it at ~78m. This is a
plausible, everyday GPS shape (idle-then-crawl-in-traffic) spanning only
~120m total, not a contrived "runaway drift chain" edge case, and it silently
misreports the merged stop's location for exactly the feature (accurate stop
geocoding) that downstream consumers like dispatch or geofencing would rely
on without any way to detect the answer is untrustworthy.
