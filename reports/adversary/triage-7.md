# Adversary triage — cycle 7

`poetry run pytest tests/adversary/ -q` → **8 failed, 34 passed**. Failures
group into 4 distinct root causes across 4 test files; each failing test is
one surviving finding, ranked most severe first.

| rank | severity | finding (test file::test) | unauth? | blast radius | why it matters |
|---|---|---|---|---|---|
| 1 | **high** | `test_antimeridian_centroid_reports_wrong_location.py::test_stop_centroid_at_antimeridian_reports_the_antipode` | yes — `POST /v1/insights`, `current_user_optional` | correctness / silently-wrong-answer, confidently presented | a dwell near Fiji/Tonga/the Aleutians is reported as a stop at lon≈0, ~17,791 km (the antipode) from where every sample actually is — worse than the pre-cycle-4 behavior of reporting nothing |
| 2 | **high** | `test_antimeridian_centroid_reports_wrong_location.py::test_hotspot_centroid_at_antimeridian_reports_the_antipode` | yes — `POST /v1/insights` | correctness / silently-wrong-answer | same root cause (naive `numpy.mean` over raw longitude) in `detect_hotspots`, independently reachable |
| 3 | **high** | `test_antimeridian_centroid_reports_wrong_location.py::test_shared_hotspot_pooled_across_compare_reports_the_antipode` | yes — `POST /v1/compare` | correctness / silently-wrong-answer | same bug surfaces on a second public endpoint (`shared_hotspots` pooling), proving it isn't confined to one code path |
| 4 | **high** | `test_antimeridian_centroid_reports_wrong_location.py::test_merge_nearby_stops_midpoint_at_antimeridian_reports_the_antipode` | yes — reachable via `/v1/insights` with `merge_stops_within_m` set | correctness / silently-wrong-answer | `merge_nearby_stops`'s plain `(prev.lon + s.lon) / 2.0` midpoint has the identical antipode failure for two stops split across ±180° |
| 5 | medium | `test_pole_dwell_stop_false_negative.py::test_polar_dwell_within_true_radius_is_detected_as_a_stop` | yes — `POST /v1/insights`, but needs a real dwell within ~15-20m of true N/S pole | correctness / silently-wrong-answer (false negative) | `latlon_to_xy_m`'s single global `meters_per_deg_lon` scale factor overstates a genuinely tight polar dwell's spread by ~57%, causing `detect_stops` to report zero stops for a real, physically-tight visit; narrow real-world exposure (polar research/tourism GPS traces only) |
| 6 | medium | `test_bounding_box_not_minimal_multi_cluster.py::test_bounding_box_is_not_actually_minimal_for_three_plus_clusters` | n/a — unit-level call to `bounding_box()` directly | correctness / silently-wrong-answer | for 3+ spread-out longitude clusters, `bounding_box`'s two-candidate-cut heuristic (raw framing vs. shifted-at-0 framing) misses the true largest-gap cut, reporting a box up to ~80° (~8,900 km) wider than its own docstring's "minimal box" promise |
| 7 | medium | `test_bounding_box_not_minimal_multi_cluster.py::test_insights_bbox_endpoint_is_not_minimal_for_three_plus_clusters` | yes — `POST /v1/insights`, ordinary multi-longitude `points` input, no crafted antimeridian jitter needed | correctness / silently-wrong-answer | same non-minimal bbox surfaces in `summary.bbox`; a map client "fit to bbox" zooms out ~80° further than the true track extent requires |
| 8 | medium | `test_merge_nearby_stops_chain_drift_undermerges.py::test_chain_of_pairwise_close_stops_fully_merges` | yes — `POST /v1/insights` with documented `merge_stops_within_m` option | correctness / silently-wrong-answer | `merge_nearby_stops` compares each stop against the *previous merge result's drifted centroid* instead of the original preceding stop, so a chain of pairwise-close stops (A-B 40m, B-C 40m, `merge_radius_m=45`) under-merges into 2 stops instead of 1, purely as an artifact of scan order |

## Why the high findings matter (#1-4: antimeridian centroid → antipode)

All four are the same defect wearing different code paths, so they're
grouped rather than treated as independent severities. Cycle 4 fixed
`latlon_to_xy_m` so that a dwell straddling the antimeridian (±180°
longitude) is correctly *clustered* into one stop/hotspot instead of being
split or dropped. That fix un-masked a second, pre-existing bug one layer
up: the centroid itself is still computed with a plain, non-wraparound
`numpy.mean` (`bhulan/analytics/stops.py::detect_stops`,
`bhulan/analytics/hotspots.py::detect_hotspots`) or a plain midpoint
(`bhulan/analytics/stops.py::merge_nearby_stops`, `(prev.lon + s.lon) /
2.0`). Averaging `+179.9999` and `-179.9999` the naive way gives `~0.0` —
not a nearby point, but the **antipode**, roughly 17,800-20,000 km away on
the *opposite side of the planet*. Before cycle 4's clustering fix this was
invisible (the cluster was never formed, so no centroid was ever computed);
now it's strictly worse: a client gets a specific, confident-looking pin on
the map — a stop or hotspot with a real duration and sample count — sitting
in the wrong ocean entirely, rather than an empty result that would at
least prompt investigation. All three call sites are reachable through
ordinary, unauthenticated `points` input to `/v1/insights` and
`/v1/compare` (`current_user_optional`, no auth dependency) — any real
device dwelling near Fiji, Tonga, Kiribati, the Aleutians, or Chukotka hits
this with zero adversarial crafting. This is the same class of "fix
un-masks the next bug" progression noted in prior cycles' triage reports,
and should be treated as one fix (wraparound-aware circular mean/midpoint
for longitude, reused across all three call sites) rather than three patches.

## Note on the medium findings

- **#5 (pole projection false negative)** is a real correctness bug with the
  same "tangent-plane projection breaks down under a degenerate-longitude
  regime" shape as the antimeridian family, but downgraded relative to #1-4
  because it's a false *negative* (a stop silently disappears) rather than a
  confidently-wrong location, and its real-world trigger surface (GPS
  traces within meters of the geographic pole) is far narrower than
  "anywhere near the antimeridian."
- **#6/#7 (non-minimal bounding box)** is a correctness/labeling defect, not
  a crash or availability issue, and degrades gracefully (the reported box
  is always a *superset* of the true minimal box, so no data is dropped —
  just a looser, mislabeled envelope). Reachable with ordinary multi-point
  tracks, no ±180° jitter needed, which is what keeps it out of "low."
- **#8 (chain-merge order-dependence)** is a documented-contract violation
  (the merge is supposed to be transitive over pairwise-close consecutive
  stops but isn't) with a plausible real-world trigger (a jitter trail along
  a building perimeter), but its effect is a modest one-stop-too-many split
  rather than a wildly wrong number, so it ranks alongside the other mediums
  rather than with the antipode family.
