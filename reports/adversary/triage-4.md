# Adversary triage — cycle 4

`poetry run pytest tests/adversary/ -q` → **4 failed, 27 passed**. Each failing
test is one finding below, ranked most severe first.

| rank | severity | finding (test file::test) | unauth? | blast radius | why it matters |
|---|---|---|---|---|---|
| 1 | **critical** | `test_kml_point_timestamp_quadratic_blowup.py::test_kml_dated_points_parse_in_reasonable_time` | yes — `POST /v1/parse/file` takes no auth | availability / DoS | a sub-5MB, completely ordinary "export my saved places" KML file ties up a worker process for tens of minutes (measured: 1,500 points / 213KB → 3.9s, quadratic growth), and the size guard only runs *after* the O(n²) parse completes |
| 2 | **high** | `test_merge_nearby_stops_reintroduces_time_gap_bug.py::test_insights_merge_stops_within_m_reintroduces_week_long_stop` | yes — `POST /v1/insights` takes no auth, trigger is a single documented option field | correctness / silently-wrong-answer | setting the documented `merge_stops_within_m` option collapses two 9-minute visits a week apart into one reported "10099-minute" (~7-day) stop, reintroducing the exact bug this cycle's ADR claims to have fixed, through an unpatched sibling code path |
| 3 | **high** | `test_merge_nearby_stops_reintroduces_time_gap_bug.py::test_compare_per_track_merge_stops_within_m_reintroduces_gap_bug` | yes — `POST /v1/compare` takes no auth | correctness / silently-wrong-answer | same root-cause bug, reachable through a second public endpoint (`/v1/compare`'s per-track reports), proving the blast radius isn't limited to `/v1/insights` |
| 4 | **medium** | `test_merge_nearby_stops_reintroduces_time_gap_bug.py::test_insights_merge_stops_within_m_stop_duration_disagrees_with_hotspot_in_same_report` | yes — same request as #2 | correctness / internal inconsistency | within one `/v1/insights` response, `stops[].duration_min` (10099 min) and `hotspots[].time_spent_min` (18 min) describe the same physical dwell but disagree by 561×, so any downstream consumer that trusts one field over the other silently gets an answer three orders of magnitude off |

## Why the critical/high findings matter

**#1 — KML O(n²) parse (critical).** `parse_kml_bytes` (`bhulan/analytics/file_parsers.py`)
resolves each `<Point>`'s timestamp via `_nearest_timestamp(root, elem)`, which is written
assuming `lxml`-style parent pointers (`elem.getparent()`). The code actually parses with
stdlib `xml.etree.ElementTree`, where `getparent` never exists, so every single point
silently falls back to a full document walk (`_iter_elems` over every `Placemark`, each
checked with a full-subtree `_contains` scan). For a document with `n` dated points this is
O(n²), and the point-count cap (`_enforce_point_cap`) only runs *after* `parse_file_bytes`
returns — so the guard exists but is applied too late to bound the cost actually paid. Since
`POST /v1/parse/file` requires no authentication, an anonymous caller can submit one ordinary
KML export (Google My Maps "export saved places" produces exactly this shape) well under the
service's own 25MB upload limit and tie up a worker process for a very long time — a plain
CPU-exhaustion DoS against a stateless, public endpoint, no unusual input shape required.

**#2/#3 — `merge_nearby_stops` reintroduces the time-gap bug (high).** `compute_insights`
(`bhulan/analytics/insights.py`) runs `detect_stops` — which is correctly gap-aware after
this cycle's fix — and then, only when the caller sets `merge_stops_within_m`, pipes the
result through `merge_nearby_stops` (`bhulan/analytics/stops.py`), which merges any two
consecutive stops whose centroids fall within the radius with **no time-gap check of its
own**. Two visits to the same spot a week apart (distance 0) get folded into a single stop
whose duration is recomputed as `end_ts - start_ts` across the entire week-long gap. This
isn't a contrived setting: `merge_stops_within_m` is a documented option that exists
specifically to clean up GPS jitter, so any caller who turns it on to get cleaner stop
boundaries silently loses the cycle's headline correctness fix. It's reachable identically
through `/v1/insights` directly and through `/v1/compare`'s per-track reports, both
unauthenticated, both with a single trivial JSON field (`"options": {"merge_stops_within_m":
100.0}`) — no adversarial input crafting needed. `bhulan/analytics/stops.py` lines 199-213
(the merge logic) were flagged in `reports/adversary/coverage-gaps.md` as never having been
exercised by any prior adversary test, which is exactly where this defect was hiding.

## Note on #4

The internal-consistency test (`stops[].duration_min` vs. `hotspots[].time_spent_min`
disagreeing by 561× in the same response) is the same root cause as #2/#3, not an
independent defect — it's downgraded to medium here because it's a corroborating symptom
(useful for spotting the bug via response-shape sanity checks) rather than a new way to
trigger wrong output. Fixing `merge_nearby_stops`'s missing time-gap check resolves all
three of #2/#3/#4 at once.
