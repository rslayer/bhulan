# Adversary triage — cycle 8

`poetry run pytest tests/adversary/ -q` → **4 failed, 39 passed**. All four
failures are pre-existing backlog defects, independently re-verified this
cycle (see `refuted-8.md`) — none are new. Cycle 4/5's antimeridian
projection + centroid fix is fully green (all its tests pass); what's left
are three unrelated, previously-known correctness defects. No critical or
high findings survive this cycle: every remaining finding is a
silently-wrong-answer correctness bug, none crash the service, leak data,
or are trivially reachable with zero crafting the way the (now-fixed)
antimeridian centroid bug was.

| rank | severity | finding (test file::test) | unauth? | blast radius | why it matters |
|---|---|---|---|---|---|
| 1 | medium | `test_bounding_box_not_minimal_multi_cluster.py::test_insights_bbox_endpoint_is_not_minimal_for_three_plus_clusters` | yes — `POST /v1/insights`, ordinary multi-longitude `points`, no crafted jitter needed | correctness / silently-wrong-answer | any real track visiting 3+ genuinely distant longitudes (not just an antimeridian-straddling one) gets a `summary.bbox` up to ~80° (~8,900 km) wider than the true minimal box, mislabeled as "minimal" — a map client fitting to this bbox zooms out far more than the track warrants |
| 2 | medium | `test_bounding_box_not_minimal_multi_cluster.py::test_bounding_box_is_not_actually_minimal_for_three_plus_clusters` | n/a — direct unit-level call to `bounding_box()`, not through an endpoint | correctness / silently-wrong-answer | same root cause as #1 (only two of the true circular-gap cuts are ever considered), confirmed at the function level; graded below #1 only because this test doesn't independently prove endpoint reachability (that's #1's job) — the underlying code path is identical |
| 3 | medium | `test_merge_nearby_stops_chain_drift_undermerges.py::test_chain_of_pairwise_close_stops_fully_merges` | yes — `POST /v1/insights` with the documented `merge_stops_within_m` option set | correctness / silently-wrong-answer | `merge_nearby_stops` compares each stop to the *previous merge result's drifted centroid* instead of the original preceding stop, so a chain where every adjacent pair is within `merge_radius_m` (A-B 40m, B-C 40m, radius 45m) can still under-merge into 2 stops instead of 1 — order-dependent, contract-violating, but needs the non-default merge option plus a specific multi-stop chain geometry to trigger, more contrived than #1 |
| 4 | medium | `test_pole_dwell_stop_false_negative.py::test_polar_dwell_within_true_radius_is_detected_as_a_stop` | yes — `POST /v1/insights`, ordinary `points`, but only within ~15-20m of the true geographic pole | correctness / silently-wrong-answer (false negative) | `latlon_to_xy_m`'s single global `meters_per_deg_lon` linear-longitude projection overstates a genuinely tight polar dwell's spread by ~57%, so `detect_stops` silently reports zero stops for a real, physically-tight visit; ranked last because its real-world trigger surface (GPS traces within meters of the N/S pole — polar research/tourism) is by far the narrowest of the four |

## Why these rank where they do (no critical/high this cycle)

All four remaining findings are the same *class* of bug — silently
returning a plausible-looking but wrong answer rather than crashing,
leaking data, or requiring authentication to hit — so severity clusters at
medium across the board; the ranking above is driven by **exploitability**
(how ordinary the triggering input is) rather than by differing blast
radius.

- **#1/#2 (non-minimal bounding box)** are the same defect at two levels:
  the endpoint test (#1) proves it's reachable with zero adversarial
  crafting — any track that legitimately visits 3+ spread-out longitudes
  hits it, no antimeridian jitter required — which is what keeps it ahead
  of the other two. It's capped at medium rather than high because the
  defect always degrades to a *superset* of the true box: no data is
  dropped, clipped, or mislocated, just a looser and mislabeled envelope.
- **#3 (chain-drift under-merge)** needs a caller to opt into
  `merge_stops_within_m` (not the default path) and a specific chain
  geometry where merge order matters, so it's a notch more contrived than
  #1 despite also being unauth-reachable through `/v1/insights`.
- **#4 (polar projection false negative)** is capped last because,
  although it's a real, unauthenticated defect on the default `/v1/insights`
  path, its trigger condition — a dwell within meters of the true
  geographic pole — is the narrowest real-world exposure of the four by a
  wide margin.

No fixes were applied; `bhulan/` was not modified.
