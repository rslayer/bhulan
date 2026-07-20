# Product Spec — bhulan

> Living spec. The cockpit writes it; the loop (`.github/workflows/loop.yml`)
> reads it, builds, iterates (eval → adversarial → robustness → refute →
> triage), then opens a PR. The loop NEVER merges to master and NEVER deploys —
> you do. bhulan is a public demo; keep it PR-gated.

**Status:** cycle 2
**Cycle of last revision:** 2

---

## 1. This cycle's single outcome

**Make `merge_nearby_stops` gap-aware**, so it can no longer re-merge two
distinct visits back into one stop spanning a real-world absence — silently
undoing cycle 1's fix.

Found by cycle 1's own adversary. Cycle 1 made `detect_stops` gap-aware, but
`compute_insights` immediately post-processes its output:

```
raw_stops = detect_stops(..., split_gap_s=opts.trip_split_gap_minutes * 60.0)
stops = merge_nearby_stops(raw_stops, merge_radius_m=opts.merge_stops_within_m)
```

`merge_nearby_stops` (`bhulan/analytics/stops.py`) merges any two consecutive
stops whose centroids are within `merge_radius_m` **with no elapsed-time
check** — recomputing duration as `combined_end - combined_start`, the entire
calendar span again. Two stops at the *same* spot (distance 0) a week apart are
folded into one ~10 000-minute stop. `merge_stops_within_m` is a real,
documented `InsightsOptions` field for cleaning up GPS-jitter-split stops, so
any caller who enables it silently loses cycle 1's gap-awareness — while
`hotspots[].time_spent_min` in the *same response* stays correct, so the two
fields disagree by three orders of magnitude about the same physical dwell.

(`tests/adversary/test_merge_nearby_stops_reintroduces_time_gap_bug.py`)

## 2. Hard constraints

- **The named test is the acceptance test** — it already exists and is failing.
  Fix the product code to make it pass; do NOT weaken, rewrite, or delete it.
- **Merge must respect the same gap.** `merge_nearby_stops` must NOT merge two
  stops separated by a time gap ≥ the split threshold — a gap is a real-world
  absence, exactly as cycle 1 established for `detect_stops`. Reuse the **same**
  `split_gap_s` notion cycle 1 introduced (`stops.DEFAULT_SPLIT_GAP_S` /
  `trip_split_gap`), threaded through — do NOT introduce a second divergent gap
  concept, and do NOT hard-code a different constant.
- **Merged duration must be the sum of the real dwells**, never the span
  including the gap. If two stops are legitimately merged (close in space AND
  time), the reported duration must reflect actual presence, not calendar time.
- **Preserve every existing passing test** — cycle 1's gap-aware tests, the
  jitter-merge behaviour `merge_nearby_stops` exists for (two stops close in
  space AND time still merge correctly), and the unit+integration suites.
- Backend/analytics only. Thread the gap through `compute_insights` →
  `merge_nearby_stops` as needed; no API-shape changes, no new dependencies, no
  DB/schema changes. Do not touch `trips.py`.
- The KML-parsing quadratic (`test_kml_point_timestamp_quadratic_blowup.py`)
  remains **backlog** for a later cycle — leave it failing.

## 3. Non-negotiable acceptance criteria

- **AC1:** two stops at the same location separated by a large time gap are NOT
  merged by `merge_nearby_stops`, even when `merge_stops_within_m` is set — they
  remain two stops with gap-aware durations.
- **AC2:** `stops[].duration_min` and `hotspots[].time_spent_min` in the same
  `/v1/insights` response agree (within rounding) about the same physical dwell
  — no three-orders-of-magnitude disagreement.
- **AC3:** the legitimate use case still works — two stops close in BOTH space
  and time (GPS jitter within the gap threshold) are still merged into one, with
  a duration equal to the real combined dwell.
- **AC4:** the gap threshold used by merge is the SAME one cycle 1 introduced;
  no second/divergent constant.
- **AC5:** `poetry run pytest tests/unit/ tests/integration/ tests/adversary/test_merge_nearby_stops_reintroduces_time_gap_bug.py tests/adversary/test_stop_and_hotspot_ignore_time_gaps.py -q` is green (with a Mongo service).

## 4. Known traps for the adversary to probe next

- Chained merges: three+ stops where A–B are close in time but B–C span a gap.
- Whether `merge_nearby_stops` recomputes `radius_m`/`sample_count` correctly
  after a legitimate merge (not just duration).
- Any OTHER post-processing step that recomputes duration from start/end spans.
- The `/v1/compare` `shared_hotspots` path once merge is gap-aware.
- The KML quadratic (backlog) and other complexity/metamorphic properties.

## 5. Definition of done for this cycle

- AC1–AC5 pass. `merge_nearby_stops` respects the same gap as `detect_stops`;
  the two duration fields agree; jitter-merge still works.
- ADR recorded in `spec/adrs/`.
- A PR is opened for review. **No merge to `master` without your review.**

## 6. Deploy target

None from the loop. The loop opens a PR; you review and merge. bhulan is a
public demo — production stays gated behind your explicit approval.

---

## Change log
- cycle 1 — cockpit — make detect_stops/detect_hotspots time-gap-aware (reused
  trips.py's split-on-gap pattern).
- cycle 2 — cockpit — make merge_nearby_stops gap-aware too; it was re-merging
  gap-split stops back across the absence, silently undoing cycle 1 whenever
  merge_stops_within_m is set. Found by cycle 1's own adversary.
