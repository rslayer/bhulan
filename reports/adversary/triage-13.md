# Triage — cycle 13

`poetry run pytest tests/adversary/ -q` → **4 failed, 50 passed** (see
`reports/adversary/refuted-13.md` for verification detail; 0 of 4 refuted).
Two of the four failures share one root cause (the zero-time-delta
mobility bug), so there are **3 distinct findings**, ranked below.

| rank | severity | finding (test file::test) | unauth? | blast radius | why it matters |
|---|---|---|---|---|---|
| 1 | **high** | `test_pole_dwell_stop_false_negative.py::test_polar_dwell_within_true_radius_is_detected_as_a_stop` | yes — trivial JSON body | correctness / silently-wrong-answer (false negative) | A real, tight dwell (11.1m true radius, well inside `stop_radius_m=15`) near a pole is reported as **zero stops** instead of one, because the lat/lon→local-XY projection degenerates near the poles; any fleet with polar or high-latitude routes gets stop-detection that silently drops real stops with no error or quality flag. |
| 2 | **high** | `test_merge_nearby_stops_heavy_dwell_drags_centroid_far_outside_merge_radius.py::test_heavy_dwell_then_light_walk_merges_but_centroid_leaves_merge_radius` | yes — trivial JSON body | correctness / silently-wrong-answer | Merging a 300-sample dwell with a chain of 2-sample stops that each walked only 40m (inside `merge_radius_m=45`) yields a reported centroid up to **118.3m** (2.6×) from a member stop — the API reports a stop location that is not actually where the vehicle stopped, with no indication to the caller that the centroid is unreliable. |
| 3 | **high** | `test_zero_time_delta_movement_misclassified_as_stopped.py` (both cases) | yes — trivial JSON body | correctness / silently-wrong-answer | Consecutive samples sharing one timestamp but different positions produce a segment labeled `"stopped"` with `distance_km: 2.11` and `avg_speed_kmh: 0.0`, while `summary.total_distance_km` also reports the real 2.11km and `max_speed_kmh: 0.0` — two directly contradictory numbers in the same response, no `quality.issues` warning, for a realistic input shape (batch-flushed loggers stamping one wall-clock time on several fixes). |

Note on the two `429`s seen when running the whole suite together: that is
a test-harness artifact (the process-wide slowapi rate limiter accumulates
across all `/v1/insights` calls made by earlier adversary test files
sharing one fake TestClient peer address), not part of either finding —
confirmed by re-running `test_zero_time_delta_movement_misclassified_as_stopped.py`
alone, where both cases fail on their real assertions instead. Do not read
the `429`s as a fourth (rate-limiting) finding.

## Critical / High detail

### 1. Polar dwell false negative (high)

A 30-sample dwell clustered within 11.1m of its own centroid at
`lat=89.9999`, requested with `stop_radius_m=15`, is physically a textbook
stop — but comes back with **0 stops detected**. Root cause:
`latlon_to_xy_m`'s local-tangent-plane projection assumes longitude
degrees represent a roughly constant real distance, which breaks down
near the poles (a few meters of true separation can span up to 180° of
longitude there), inflating the projected cluster spread past the
threshold. Any vehicle or asset operating at high latitude — polar
research, arctic logistics, some far-northern fleets — gets stops
silently erased from its trip history with total API confidence and no
`quality` warning. This is worse than a crash: the caller has no signal
that anything went wrong.

### 2. Heavy-dwell merge centroid drifts outside merge radius (high)

`detect_stops`'s stop-merging step accepts merges purely on
consecutive-pairwise distance (each hop is 40m, under the 45m
`merge_radius_m`), then computes the merged centroid as a
`sample_count`-weighted average. A long, heavy dwell (300 samples) at one
end of the chain drags the weighted centroid so far toward itself that
later members of the chain end up 78–118m from the reported centroid —
over 2.5× the radius the caller configured and was told the merge would
respect. Any downstream consumer treating the reported stop location as
"where the vehicle actually was" (dispatch, geofencing, billing-by-stop)
gets a location error large enough to place the vehicle at the wrong
building or address, silently.

### 3. Zero-time-delta movement misclassified as stopped (high)

Two independent divide-by-zero guards in `bhulan/analytics/mobility.py`
(`segment_by_motion`'s `step_speed = np.where(step_secs > 0, ..., 0.0)`
and `speed_stats_mps`'s `if s > 0` skip) both force same-timestamp hops to
contribute zero speed, while the segment's and summary's `distance_km`
sum the real haversine distance unconditionally. The result is a response
where `segments` reports a `"stopped"` block covering 2.1km of real
travel, `summary.total_distance_km` agrees the distance happened, and yet
`summary.max_speed_kmh` / `avg_moving_speed_kmh` are `0.0` — with no entry
in `quality.issues`, unlike the analogous exact-duplicate-point case which
*is* flagged. Any client that trusts segment `kind` to mean "distance ≈ 0
here" (e.g., "how far did the vehicle travel while stopped") gets a
materially wrong answer, and the underlying anomaly (a physically
impossible zero-elapsed-time displacement, usually a clock/logging
glitch) is reported with full confidence instead of being surfaced.
