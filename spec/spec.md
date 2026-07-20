# Product Spec — bhulan

> Living spec. The cockpit writes it; the loop reads it, builds, iterates
> (eval → adversarial → robustness → refute → triage), then opens a PR.
> The loop NEVER merges to master and NEVER deploys — you do. bhulan is a public
> demo; keep it PR-gated.

**Status:** cycle 8
**Cycle of last revision:** 8

---

## 1. This cycle's single outcome

**Make a merged stop's reported centroid and `radius_m` truthful for a chain of
3+ stops**, so the geometry matches the members it claims to summarise.

Cycle 7 fixed `merge_nearby_stops` to stop *under-merging* a chain (the count is
now right), but it accumulates geometry by repeated **pairwise midpoint**:
`lat = (blob.lat + s.lat)/2`, `lon = circular_mean_lon([blob.lon, s.lon])`,
`radius_m = dist(prev_original, s)/2 + max(blob.radius_m, s.radius_m)`. For 3+
members this is wrong two ways:

- The centroid is **weighted toward the later members** (each new point pulls the
  blob halfway), so for A,B,C at 0/40/80 m it can land ~50 m from A — *outside*
  the caller's `merge_radius_m = 45`, violating cycle 7's own AC2.
- `radius_m` is computed from `dist(prev_original, s)` (the last hop only), so it
  **understates the true spread** — the distance from the reported centroid to
  the farthest member — by ~20% at 3 stops and more as the chain grows.

`radius_m` is the field a caller uses to judge how tight a "stop" really is, so
an understated value is a silently-wrong answer. Reachable unauthenticated via
`/v1/insights` with `merge_stops_within_m` and an ordinary jitter trail.

Acceptance test (already on master, currently red):
`tests/adversary/test_merge_nearby_stops_chain_centroid_radius_wrong.py`
(`test_chain_merge_centroid_stays_within_merge_radius_of_every_member`,
`test_chain_merge_radius_m_undersells_true_spread`).

## 2. Hard constraints

- **The named test is the acceptance test** — it already exists and is failing.
  Fix the product code; do NOT weaken it.
- **Accumulate from the real members, not by drifting midpoint.** Track the
  member stops that make up the current blob and compute, when emitting the
  merged stop:
  - `lat` = the mean of member centroids weighted by `sample_count` (plain mean).
  - `lon` = `circular_mean_lon` of the member centroid longitudes (so a chain
    across ±180° still centres correctly); weight by `sample_count` if practical,
    otherwise an unweighted circular mean is acceptable.
  - `radius_m` = the true enclosing radius: `max` over members of
    `haversine_m(centroid, member.centroid) + member.radius_m`. It MUST bound the
    distance from the reported centroid to every member (AC: `radius_m` ≥ the
    farthest member distance).
  - `duration_s` = sum of member dwells (unchanged); `start/end_index`,
    `sample_count`, `start_ts`, `end_ts` span all members (unchanged).
- **Do NOT change the merge *decision*** — cycle 7's single-linkage against the
  immediately preceding original stop stays exactly as is (this cycle is geometry
  only). The set of stops that merge must be identical to cycle 7; only the
  reported centroid/`radius_m` of a merged group changes.
- **No span cap this cycle.** Whether to bound a merged stop's total span is a
  separate product decision (see §4) — do NOT add a guard here.
- **Byte-identical for the single-merge case.** A merged group of exactly two
  stops must produce the same centroid and `radius_m` as today (two-member
  weighted mean + enclosing radius reduces to the current pairwise formula for
  equal weights; verify the two-stop path is unchanged for existing tests).
- **Do NOT regress cycles 1–7** — antimeridian projection/centroid, largest-gap
  bbox, gap-aware detect/merge, transitive merge count all stay green. Do NOT
  touch `haversine_m`, `latlon_to_xy_m`, `bounding_box`, or `circular_mean_lon`.
- Backend/analytics only. No new dependencies, no API-shape changes, no
  DB/schema changes.

## 3. Non-negotiable acceptance criteria

- **AC1:** for a merged chain A,B,C, the reported centroid is within
  `merge_radius_m` (haversine) of every member A, B, and C.
- **AC2:** the reported `radius_m` is ≥ the true distance from the reported
  centroid to the farthest member (no understatement).
- **AC3:** the *set* of stops that merge is identical to cycle 7 (count
  unchanged) — this cycle only corrects the merged group's geometry.
- **AC4:** a two-stop merge produces byte-identical centroid and `radius_m` to
  before.
- **AC5:** `poetry run pytest tests/unit/ tests/adversary/test_merge_nearby_stops_chain_centroid_radius_wrong.py tests/adversary/test_merge_nearby_stops_chain_drift_undermerges.py -q` is green.

## 4. Known traps for the adversary to probe next (backlog / product decisions)

- **Transitive-merge span cap (PRODUCT DECISION, owner):** a chain of many
  pairwise-close stops can collapse into one stop spanning ≫ `merge_radius_m`
  (e.g. a 6-stop chain → ~175 m span, ~3.9× a 45 m radius). Once `radius_m` is
  truthful (this cycle), a caller *can* at least see the span — but whether to
  additionally **cap** the merge (split when the span would exceed, say,
  2×`merge_radius_m`) is a semantics change to cycle 7's single-linkage and is
  left for the owner to decide. The parallel driver's
  `test_merge_nearby_stops_runaway_chain_span.py` encodes the capped behaviour.
- **Polar dwell false-negative** (`test_pole_dwell_stop_false_negative.py`).
- Deeply-nested-JSON recursion → 500; non-finite reported `speed_mps`;
  datetime-overflow / malformed-type.

## 5. Definition of done for this cycle

- AC1–AC5 pass. Merged centroid and `radius_m` are computed from the real
  members and are truthful; merge decision and count unchanged from cycle 7;
  two-stop path byte-identical; cycles 1–7 green.
- ADR recorded in `spec/adrs/` (document member-accumulated centroid + enclosing
  radius, and note the span-cap decision is deferred to the owner).
- A PR is opened for review.

## 6. Deploy target

None from the loop. The loop opens a PR; you review and merge. bhulan is a
public demo — production stays gated.
