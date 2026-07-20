# Product Spec — bhulan

> Living spec. The cockpit writes it; the loop (`.github/workflows/loop.yml`)
> reads it, builds, iterates (eval → adversarial → robustness → refute →
> triage), then opens a PR. The loop NEVER merges to master and NEVER deploys —
> you do. bhulan is a public demo; keep it PR-gated.

**Status:** cycle 1
**Cycle of last revision:** 1

---

## 1. This cycle's single outcome

**Make `detect_stops` and `detect_hotspots` time-gap-aware**, so a real-world
absence between two visits to the same place is no longer silently reported as
one continuous stop/hotspot spanning the entire gap.

Found by the robustness sweep (run 4, `reports/adversary/robustness-4.md`) — a
metamorphic / silently-wrong-answer defect, the class prior crash-focused runs
never looked at. A device parked at a spot for 10 min, switched off for a week
(no samples), then parked at the *same* spot for 10 min is reported as **one
stop of `duration_min: 10099`** and **one hotspot visit** (`visit_count: 1`,
`time_spent_min: 10099`) — instead of two ~9-minute visits. The error grows
unboundedly with the calendar gap, with no crash to signal it.

Two independent gap-blind implementations:

1. **`bhulan/analytics/stops.py::detect_stops`** grows a cluster on *spatial*
   spread only (`_cluster_end`/`_cluster_radius_m` vs `radius_m`); it never
   inspects the time gap between consecutive samples, so two temporally-distant
   visits to the same spot merge into one stop.
2. **`bhulan/analytics/hotspots.py`** (`_time_spent_s` / `_visit_count`) defines
   a "visit" as a run of consecutive array indices in the same grid cell, with
   no elapsed-time check — so two revisits with nothing recorded between them
   become adjacent indices and read as one continuous dwell.

The fix already exists in the codebase to copy from: **`trips.py::_trip_bounds`
splits on `trip_split_gap_seconds` between consecutive samples for exactly this
reason.** Stops and hotspots must gain the same gap-awareness.

(`tests/adversary/test_stop_and_hotspot_ignore_time_gaps.py` — 2 failing tests)

## 2. Hard constraints

- **The named test is the acceptance test** — it already exists and is failing.
  Fix the product code to make it pass; do NOT weaken, rewrite, or delete it.
- **Reuse the existing gap logic.** `trips.py` already splits on a configurable
  gap. Introduce the same notion for stops/hotspots — a configurable
  `stop_split_gap_seconds` (or reuse the existing gap setting if appropriate),
  with a sensible default consistent with the rest of the analytics. Do NOT
  invent a second, divergent gap mechanism.
- **A stop/hotspot must not span a gap larger than the split threshold** — two
  visits separated by more than the threshold are two stops / two visits, and
  the reported duration is the sum of actual dwell times, never the elapsed
  calendar time including the gap.
- **Preserve every existing passing test** (unit + integration are the
  regression gate) — including the prior stop-detection tests (the cycle-2
  quadratic fix and the run-2 fixes must stay green). Normal continuous tracks
  must be unaffected.
- Backend/analytics only. No API-shape changes, no new dependencies, no DB or
  schema changes. Do not touch `trips.py` (it is already correct) beyond reading
  it for the pattern.
- The KML-parsing quadratic (`test_kml_point_timestamp_quadratic_blowup.py`) is
  **backlog for a later cycle** — leave it failing; it is not this cycle's job.

## 3. Non-negotiable acceptance criteria

- **AC1:** two visits to the same location separated by a long time gap (days,
  weeks) produce **two** stops (and two hotspot visits), each with a duration
  reflecting only the actual dwell — not one stop/visit spanning the gap.
- **AC2:** `hotspots[].visit_count` and `time_spent_min` count real visits and
  sum real dwell time, across a single track (`/v1/insights`) and across two
  tracks (`/v1/compare` `shared_hotspots`).
- **AC3:** a normal continuous track (no large gaps) yields exactly the same
  stops/hotspots as before — no regression to the happy path.
- **AC4:** the gap threshold is configurable and consistent with the existing
  analytics settings; the split logic reuses `trips.py`'s approach, not a new one.
- **AC5:** `poetry run pytest tests/unit/ tests/integration/ tests/adversary/test_stop_and_hotspot_ignore_time_gaps.py -q` is green (with a Mongo service for integration).

## 4. Known traps for the adversary to probe next

- Off-by-one at the exact gap threshold (a gap == threshold: one stop or two?).
- A gap that is exactly the sampling interval vs. a real absence.
- Interaction with the stop-detection radius: a gap *and* spatial drift.
- Whether `/v1/compare`'s pooling across tracks now double-counts or mis-merges.
- The KML quadratic (backlog) and any other complexity blowups.
- Other metamorphic properties: reordering, coordinate translation, unit changes.

## 5. Definition of done for this cycle

- AC1–AC5 pass. Stops and hotspots are gap-aware, reusing `trips.py`'s pattern.
- ADR recorded in `spec/adrs/`.
- A PR is opened for review. **No merge to `master` without your review.**

## 6. Deploy target

None from the loop. The loop opens a PR; you review and merge. bhulan is a
public demo — production stays gated behind your explicit approval.

---

## Change log
- cycle 1 — cockpit — make stops/hotspots time-gap-aware (robustness run-4
  metamorphic finding), reusing the gap logic trips.py already has.
