"""
Defect (silently-wrong-answer, the "runaway chain over-merge" trap the spec
itself flags -- spec/spec.md section 4, "Known traps for the adversary to
probe next": "now that the merge is transitive, does an unbounded chain of
stops each just within merge_radius_m collapse into one implausibly wide
'stop'? Probe whether a span cap or total-extent guard is warranted.").

Six consecutive stops in a straight line, each ~35m from its immediate
neighbour (within merge_radius_m=45, no disqualifying time gap) -- the exact
single-linkage-over-consecutive-stops chain this cycle's fix is designed to
collapse -- fold into a single reported "stop" whose true extent is ~175m
end to end (five hops x 35m). There is no span cap or total-extent guard
anywhere in ``merge_nearby_stops``; the only bound is the time-gap check.

Two independent things go wrong, and both get worse as the chain gets
longer, not just at the 3-stop edge case:

1. The reported centroid drifts far outside merge_radius_m of the chain's
   earlier members (here, ~141m from the first stop against a configured
   merge_radius_m of 45m) -- the same unweighted-pairwise-midpoint bias as
   the 3-stop case
   (test_merge_nearby_stops_chain_centroid_radius_wrong.py), compounding
   with every additional link.
2. The reported ``radius_m`` field (which exists specifically so a caller
   can tell how spread out a reported "stop" really is) understates the true
   spread by a wide margin -- ~87m reported vs. ~141m true distance from the
   reported centroid to the farthest original member, because the
   recentring-displacement term is sized from
   ``dist(prev_original, incoming)`` instead of ``dist(blob, incoming)``.

A caller relying on ``radius_m`` to sanity-check whether a "stop" is a real,
tight dwell location (as opposed to, say, a slow walk along a long block)
gets a number that is silently ~40% too small -- and the "stop" itself
represents a physical span (~175m) many multiples of the configured
merge_radius_m (45m), with nothing in the API surface flagging that the
transitive merge stretched this far. Reachable unauthenticated via
``/v1/insights`` with the documented ``merge_stops_within_m`` option.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)
from bhulan.analytics.geodesy import haversine_m

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LON = -73.0
_LAT0 = 40.0
_METERS_PER_DEG_LAT = 111_320.0
_STEP_M = 35.0
_N_STOPS = 6
_MERGE_RADIUS_M = 45.0
_LATS = [_LAT0 + i * _STEP_M / _METERS_PER_DEG_LAT for i in range(_N_STOPS)]


def _cluster(lat: float, lon: float, t_start: datetime, n: int = 6, step_s: int = 60) -> list[dict]:
    points = []
    t = t_start
    for _ in range(n):
        points.append({"lat": lat, "lon": lon, "ts_utc": t.isoformat()})
        t += timedelta(seconds=step_s)
    return points


def _payload() -> dict:
    points: list[dict] = []
    t = _T0
    for lat in _LATS:
        points += _cluster(lat, _LON, t)
        t += timedelta(seconds=6 * 60) + timedelta(minutes=2)
    return {
        "points": points,
        "options": {"stop_radius_m": 5.0, "merge_stops_within_m": _MERGE_RADIUS_M},
    }


def test_runaway_chain_radius_m_understates_true_spread(client: TestClient):
    """radius_m must be at least the true distance from the reported
    centroid to the farthest original member -- otherwise it is not a valid
    upper bound on the "stop"'s spread, which defeats its purpose as a
    sanity-check field.
    """
    r = client.post("/v1/insights", json=_payload())
    assert r.status_code == 200
    report = r.json()

    stops = report["stops"]
    assert len(stops) == 1, (
        "test setup: every adjacent pair in the 6-stop chain is "
        f"{_STEP_M}m apart (within merge_radius_m={_MERGE_RADIUS_M}), so the "
        "transitive merge (this cycle's fix) should fold them all into one stop"
    )
    merged = stops[0]

    true_dists = [haversine_m(merged["lat"], merged["lon"], lat, _LON) for lat in _LATS]
    true_max_dist_m = max(true_dists)
    reported_radius_m = merged["radius_m"]

    assert reported_radius_m >= true_max_dist_m * 0.95, (
        f"merged stop reports radius_m={reported_radius_m:.1f}m over a "
        f"{_N_STOPS}-stop chain, but the true distance from its own reported "
        f"centroid to the farthest original member is {true_max_dist_m:.1f}m "
        f"-- radius_m understates the true spread by "
        f"{100 * (1 - reported_radius_m / true_max_dist_m):.0f}%, and the "
        f"error grows with chain length since the recentring-displacement "
        f"term is sized from the wrong pair of points on every merge after "
        f"the first."
    )


def test_runaway_chain_span_vastly_exceeds_merge_radius_with_no_guard(client: TestClient):
    """The spec explicitly asks the adversary to probe whether an unbounded
    chain of just-within-merge_radius_m stops collapses into an implausibly
    wide 'stop'. It does: the true end-to-end span here is several multiples
    of merge_radius_m, with no span cap or total-extent guard anywhere in
    merge_nearby_stops -- only the time-gap check bounds a chain at all.
    """
    r = client.post("/v1/insights", json=_payload())
    assert r.status_code == 200
    report = r.json()

    stops = report["stops"]
    assert len(stops) == 1, "test setup: expected the chain to fully merge into one stop"

    true_span_m = haversine_m(_LATS[0], _LON, _LATS[-1], _LON)

    assert true_span_m <= _MERGE_RADIUS_M * 2, (
        f"a single reported 'stop' spans {true_span_m:.1f}m end-to-end "
        f"({true_span_m / _MERGE_RADIUS_M:.1f}x the configured "
        f"merge_radius_m={_MERGE_RADIUS_M}m), produced purely by chaining "
        f"{_N_STOPS} stops each just within merge_radius_m of its immediate "
        f"neighbour. merge_nearby_stops has no span cap or total-extent "
        f"guard -- only the time-gap check bounds a transitive chain -- so a "
        f"caller cannot tell from merge_stops_within_m alone how far a "
        f"reported 'stop' might actually be stretched."
    )
