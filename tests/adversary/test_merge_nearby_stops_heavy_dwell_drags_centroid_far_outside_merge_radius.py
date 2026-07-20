"""
Defect (silently-wrong-answer / AC2 violation, cycle 9): the new
``sample_count``-weighted centroid in ``_merge_members``
(``bhulan/analytics/stops.py``) is only checked, by the cycle-9 acceptance
test, against a *symmetric* 3-stop scenario (brief, dense, brief -- A:2
samples, B:300 samples, C:2 samples, all ~40m apart in a straight line). Spec
section 2 asserts a *general* correctness argument for the fix: "the centroid
is the true weighted centroid, which for a single-linkage chain (every member
within merge_radius_m of the mass around the dense member) lies within
merge_radius_m of every member." ADR 0011 repeats the same general claim for
AC2.

That general claim is false. It happens to hold for the specific symmetric
scenario the acceptance test exercises (the heavy member sits in the middle,
so both light members are pulled to within ~1 hop of it), but it does not
hold once the *chain continues past* the heavy member: a heavy dwell followed
by a short walk of light, brief stops away from it.

Concretely: one heavy dwell (300 one-second samples, i.e. ``sample_count``
~300) followed by three brief (2-sample) stops walking away in a straight
line, each consecutive hop exactly 40m -- comfortably inside
``merge_stops_within_m=45``, so cycle 7's single-linkage decision folds all
four into one blob exactly as intended, and the *set* of stops that merge is
correct. But because the reported centroid is the ``sample_count``-weighted
mean, and one member's weight (~300) dwarfs the others (~1 each), the
centroid lands almost exactly on top of the heavy dwell (within ~2m) and
totally ignores how far the chain has walked since. The farthest brief stop
ends up **~118m** from the reported centroid -- 2.6x ``merge_radius_m`` --
even though every single consecutive pairwise merge-decision distance in the
chain was inside 45m. The middle brief stop is also already outside the
radius at ~78m.

This is a materially worse violation of AC2 than the one this cycle fixed
(cycle 8's bug reached ~49m against the same 45m radius; this reaches ~118m),
using a chain of only 4 members and a total geographic span of only 120m
(~2.6x merge_radius_m) -- an entirely ordinary GPS pattern (a vehicle idling
for a few minutes, then rolling forward slowly in heavy traffic), not the
"genuinely long drift chain" that spec section 4 / ADR 0011 flag as an
out-of-scope, `≫`-radius backlog item
(``test_merge_nearby_stops_runaway_chain_span.py``, not present in this repo).
Weight concentration, not chain length, is what breaks AC2 here.

Reachable unauthenticated via ``/v1/insights`` with the documented
``merge_stops_within_m`` option.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)
from bhulan.analytics.geodesy import haversine_m

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LAT_B, _LON = 40.0, -73.0
_MERGE_RADIUS_M = 45.0
_STEP_M = 40.0
_N_STEPS = 3

_METERS_PER_DEG_LAT = 111_320.0
_METERS_PER_DEG_LON = 111_320.0 * math.cos(math.radians(_LAT_B))


def _offset_north(lat0: float, lon0: float, north_m: float) -> tuple[float, float]:
    return lat0 + north_m / _METERS_PER_DEG_LAT, lon0


def _run() -> tuple[dict, list[tuple[float, float]]]:
    points = []
    t = _T0

    # B: a long, densely-sampled dwell (ordinary for a phone reporting once a
    # second while parked), giving it an overwhelming sample_count.
    for _ in range(300):
        points.append({"lat": _LAT_B, "lon": _LON, "ts_utc": t.isoformat()})
        t += timedelta(seconds=1)

    # A short walk away from B: brief (2-sample) stops, each consecutive hop
    # exactly _STEP_M apart -- comfortably inside merge_stops_within_m -- so
    # cycle 7's single-linkage decision folds every one of them into the same
    # blob as B.
    positions: list[tuple[float, float]] = []
    north = 0.0
    for _ in range(_N_STEPS):
        north += _STEP_M
        lat, lon = _offset_north(_LAT_B, _LON, north)
        positions.append((lat, lon))
        t += timedelta(minutes=2)
        for _ in range(2):
            points.append({"lat": lat, "lon": lon, "ts_utc": t.isoformat()})
            t += timedelta(seconds=60)

    payload = {
        "points": points,
        "options": {
            "stop_radius_m": 5.0,
            "min_stop_minutes": 0.5,
            "merge_stops_within_m": _MERGE_RADIUS_M,
        },
    }
    return payload, positions


def test_heavy_dwell_then_light_walk_merges_but_centroid_leaves_merge_radius(client: TestClient):
    """AC2 requires the merged centroid to stay within merge_radius_m of every
    member it claims to summarize. A heavy dwell (~300 samples) followed by a
    straight-line walk of brief (2-sample) stops, each hop well inside
    merge_stops_within_m, still merges into one stop per cycle 7 -- but the
    sample_count-weighted centroid snaps onto the heavy dwell and abandons the
    light members, landing the farthest one ~118m from the reported centroid
    against a 45m merge radius.
    """
    payload, positions = _run()
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    stops = report["stops"]
    assert len(stops) == 1, (
        "test setup: the heavy dwell and the three brief walk-away stops must "
        "all merge into one blob (every consecutive pairwise distance is "
        "well inside merge_stops_within_m, so cycle 7's single-linkage "
        "decision is unaffected)"
    )
    merged = stops[0]

    dist_to_b = haversine_m(merged["lat"], merged["lon"], _LAT_B, _LON)
    assert dist_to_b <= _MERGE_RADIUS_M, "test setup: centroid should stay near the heavy dwell"

    dists = [haversine_m(merged["lat"], merged["lon"], lat, lon) for lat, lon in positions]
    worst = max(dists)

    assert all(d <= _MERGE_RADIUS_M for d in dists), (
        f"AC2 requires the merged centroid to lie within merge_radius_m="
        f"{_MERGE_RADIUS_M}m of every member; got per-step distances "
        f"{[f'{d:.1f}m' for d in dists]} (worst {worst:.1f}m) for a walk-away "
        f"chain where every consecutive pairwise merge-decision distance was "
        f"only {_STEP_M}m -- the sample_count-weighted centroid concentrates "
        f"almost entirely on the heavy dwell and ignores how far the chain "
        f"has since walked, silently misreporting the merged stop's location "
        f"for every light member beyond the first hop."
    )
