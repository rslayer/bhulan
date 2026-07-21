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


def test_heavy_dwell_then_light_walk_stays_bounded_by_stop_radius(client: TestClient):
    """A stop is one place — points clustered within ``stop_radius_m`` of a
    centre — and the merge cap (ADR 0013) enforces exactly that.

    A heavy dwell (~300 samples at B) followed by a straight-line walk of brief
    stops 40/80/120 m away must NOT collapse into one stop: the walk stops are
    each beyond ``stop_radius_m`` of the dwell, so they are movement, not part of
    the dwell. Before the cap, cycle 7's unbounded single-linkage folded them
    all into one blob and the ``sample_count``-weighted centroid snapped onto the
    heavy dwell, misreporting the merged stop ~118 m from its own far members.
    The cap fixes this by keeping every reported stop bounded to one stop's
    radius, so the location is never misreported.
    """
    payload, positions = _run()
    stop_radius_m = payload["options"]["stop_radius_m"]
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    stops = report["stops"]

    # The cap splits the dwell from the walk: they do NOT collapse into one
    # wide stop. (Before the cap this was a single ~120 m-spanning blob.)
    assert len(stops) > 1, (
        "the heavy dwell and the walk-away stops are each beyond stop_radius_m "
        f"({stop_radius_m}m) of one another and must not merge into one stop; "
        f"got a single stop spanning the whole walk"
    )

    # Every reported stop is a genuine bounded cluster: its centroid lies within
    # stop_radius_m of every member it summarizes (radius_m never exceeds one
    # stop's radius, allowing a small numeric margin). This is the invariant the
    # cap guarantees and the old unbounded merge violated (worst member ~118 m
    # from the centroid against a 5 m stop radius).
    eps = 1e-6
    for st in stops:
        assert st["radius_m"] <= stop_radius_m + eps, (
            f"a reported stop has radius_m={st['radius_m']:.1f}m > "
            f"stop_radius_m={stop_radius_m}m — a merged stop must never be "
            f"wider than one stop's radius (ADR 0013)"
        )

    # The heavy dwell is still reported, centred on B (not dragged toward the
    # walk): some stop sits within stop_radius_m of B and carries the long dwell.
    dwell = [s for s in stops if haversine_m(s["lat"], s["lon"], _LAT_B, _LON) <= stop_radius_m + eps]
    assert dwell, "the heavy dwell at B must still be reported as its own bounded stop"
