# Triage — cycle 16 surviving findings

`poetry run pytest tests/adversary/ -q` → 1 failed, 65 passed. One
distinct defect survives this cycle.

| rank | severity | finding (test file::test) | unauth? | blast radius | why it matters |
|---|---|---|---|---|---|
| 1 | high | `test_merge_nearby_stops_anchor_cap_radius_still_balloons.py::test_merged_stop_radius_m_balloons_far_beyond_stop_radius_m` | yes — `/v1/insights`, ordinary documented `InsightsOptions` fields (`merge_stops_within_m`, `stop_radius_m`), no special payload shape | correctness / silently-wrong-answer | An everyday "one popular spot dominates a merged group" track (ten short dwells on one side of an anchor, one on the other, each individually within `stop_radius_m` of the anchor) is merged into a single reported stop whose `radius_m` (86m) is a 71% overshoot of the configured `stop_radius_m` (50m) — with no error or `quality.issues` entry, so a consumer trusting `radius_m <= stop_radius_m` per ADR 0013 is silently misled. |

## High detail

**#1 — Anchor-based merge cap bounds the wrong point, so `radius_m` can approach
2x `stop_radius_m` (high).** ADR 0013's fix to `merge_nearby_stops`
(`bhulan/analytics/stops.py`) caps merge admission by requiring each
candidate member to lie within `stop_radius_m` of the group's *anchor* (its
first member), and the ADR's stated guarantee is the stronger claim that
"every reported merged stop is a genuine cluster: `radius_m <= stop_radius_m`
... so a downstream 'was the vehicle parked here' consumer can trust the
location and spread" (matching spec.md AC1: "each reported centroid lies
within `stop_radius_m` of its own members"). Those are two different
guarantees: bounding every member's distance to the anchor only constrains
membership to a disk of radius `stop_radius_m` centred on the anchor — it says
nothing about where the `sample_count`-weighted centroid of an *asymmetric*
group within that disk ends up. When ten dwells sit ~49m east of the anchor
and one sits ~49m west (each individually admissible under the cap), the
weighted centroid is pulled toward the heavy east side, and the correctly
computed enclosing `radius_m` from that centroid to the lone west outlier
legitimately reports ~86m — approaching the disk's full diameter
(`2 * stop_radius_m`) rather than staying within it. This is not a contrived
tie-breaking edge case: a single popular location with occasional stops on
its far side is the ordinary shape of any frequently-visited-place track
(e.g., a loading dock approached from two directions), it is reachable
unauthenticated with a single `POST /v1/insights` call using only documented
options, and it reproduces exactly the "unbounded blob" symptom ADR 0013 was
written to eliminate — just capped at ~2x `stop_radius_m` instead of
unbounded. Ranked high rather than critical because it is a correctness
defect with a bounded (not unbounded) overshoot and no availability/crash
impact, unlike prior cycles' critical findings (e.g. cycle 12's unauthenticated
recursion-crash DoS).
