"""
Defect (silently-wrong-answer / AC2 violation, cycle 7): once
``merge_nearby_stops`` (``bhulan/analytics/stops.py``) transitively folds a
*third* stop onto an already-merged blob, both the reported centroid and the
reported ``radius_m`` go wrong -- even though the chain-drift under-merge bug
(cycle 7's headline fix) is gone and the stop count is correct.

Spec AC2 (spec/spec.md section 3) is explicit for exactly this three-stop
scenario: "the merged stop's duration_s is the sum of the three real dwells
and its centroid lies within the cluster (within ~merge_radius_m of each
member)." The acceptance test
(test_merge_nearby_stops_chain_drift_undermerges.py) only checks
``len(stops) == 1``; it never checks AC2's centroid/radius clause.

The bug: inside the merge loop, ``centroid_dist_m`` is computed once, from
``prev_original`` (the immediately preceding *original* stop -- correct, and
needed for the cycle-7 fix's spatial closeness test) to the incoming stop
``s``. That same variable is then reused to size the recentring displacement
in ``radius_m``:

    radius_m=centroid_dist_m / 2.0 + max(blob.radius_m, s.radius_m)

For the first merge in a chain (``blob is prev_original``), that's correct.
But by the second merge, the new centroid is computed as the midpoint of
``blob`` (the running blob, already recentred away from the original A/B
positions) and ``s`` -- yet the displacement term still uses
``dist(prev_original, s)`` = ``dist(B, C)`` instead of the geometrically
relevant ``dist(blob, s)``. The two distances diverge once the blob has
drifted, so both the final centroid and the reported ``radius_m`` silently
misstate the true spread.

Concretely, for three co-linear stops A, B, C 40m apart (A-B = B-C = ~40m,
A-C = ~80m, the exact scenario the cycle-7 acceptance test uses,
merge_radius_m=45):

  - The reported merged centroid sits ~50m from A -- *outside*
    merge_radius_m=45, directly violating AC2's "within ~merge_radius_m of
    each member."
  - The reported radius_m (~40m) is smaller than the true distance from the
    reported centroid to A (~50m) -- the field silently understates how
    spread out the samples it claims to summarize actually are, exactly the
    kind of "radius that undersells the true spread" defect the cycle-6/7
    radius_m fix (test_merge_nearby_stops_radius_m_wrong.py) was meant to
    close, just re-opened for the 3+-chain case that fix didn't cover.

This is unweighted-average centroid drift compounding pairwise, silently
biasing the reported location toward the *end* of the chain and invalidating
the radius bound -- reachable unauthenticated via ``/v1/insights`` with the
documented ``merge_stops_within_m`` option on an ordinary 3-stop GPS-jitter
chain.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)
from bhulan.analytics.geodesy import haversine_m

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LAT_A = 40.0
_LAT_B = 40.00036  # ~40m north of A
_LAT_C = 40.00072  # ~40m north of B, ~80m north of A
_LON = -73.0
_MERGE_RADIUS_M = 45.0


def _cluster(lat: float, lon: float, t_start: datetime, n: int = 6, step_s: int = 60) -> list[dict]:
    points = []
    t = t_start
    for _ in range(n):
        points.append({"lat": lat, "lon": lon, "ts_utc": t.isoformat()})
        t += timedelta(seconds=step_s)
    return points


def _run() -> dict:
    cluster_a = _cluster(_LAT_A, _LON, _T0)
    b_start = _T0 + timedelta(seconds=6 * 60) + timedelta(minutes=2)
    cluster_b = _cluster(_LAT_B, _LON, b_start)
    c_start = b_start + timedelta(seconds=6 * 60) + timedelta(minutes=2)
    cluster_c = _cluster(_LAT_C, _LON, c_start)

    payload = {
        "points": cluster_a + cluster_b + cluster_c,
        "options": {"stop_radius_m": 100.0, "merge_stops_within_m": _MERGE_RADIUS_M},
    }
    return payload


def test_chain_merge_centroid_stays_within_merge_radius_of_every_member(client: TestClient):
    """AC2: 'its centroid must land inside the merged cluster ... within
    ~merge_radius_m of each member.' A, B, C are the exact acceptance-test
    scenario; the chain now correctly collapses to one stop, but the
    unweighted pairwise-midpoint recentring pulls the reported centroid
    ~50m from A -- outside merge_radius_m=45 -- violating the spec's own
    acceptance criterion for this fix.
    """
    payload = _run()
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    stops = report["stops"]
    assert len(stops) == 1, "test setup: the chain-drift under-merge fix must still fold A,B,C into one stop"
    merged = stops[0]

    dist_to_a = haversine_m(merged["lat"], merged["lon"], _LAT_A, _LON)
    dist_to_b = haversine_m(merged["lat"], merged["lon"], _LAT_B, _LON)
    dist_to_c = haversine_m(merged["lat"], merged["lon"], _LAT_C, _LON)

    assert dist_to_a <= _MERGE_RADIUS_M and dist_to_b <= _MERGE_RADIUS_M and dist_to_c <= _MERGE_RADIUS_M, (
        f"AC2 requires the merged centroid to lie within ~merge_radius_m="
        f"{_MERGE_RADIUS_M}m of every member; got distances "
        f"A={dist_to_a:.1f}m B={dist_to_b:.1f}m C={dist_to_c:.1f}m -- the "
        f"merge loop recentres to the unweighted midpoint of the running "
        f"blob and each new stop, which compounds pairwise and drags the "
        f"reported centroid toward the tail of the chain, away from earlier "
        f"members."
    )


def test_chain_merge_radius_m_undersells_true_spread(client: TestClient):
    """The reported radius_m must bound the true distance from the reported
    centroid to every original member it claims to summarize. For a 3-stop
    chain, merge_nearby_stops sizes the recentring-displacement term in
    radius_m using dist(prev_original, s) instead of dist(blob, s), so the
    reported radius understates the true spread once a blob has already
    drifted from a prior merge.
    """
    payload = _run()
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    stops = report["stops"]
    assert len(stops) == 1, "test setup: expected the chain to fully merge into one stop"
    merged = stops[0]

    dist_to_a = haversine_m(merged["lat"], merged["lon"], _LAT_A, _LON)
    dist_to_b = haversine_m(merged["lat"], merged["lon"], _LAT_B, _LON)
    dist_to_c = haversine_m(merged["lat"], merged["lon"], _LAT_C, _LON)
    true_max_dist_m = max(dist_to_a, dist_to_b, dist_to_c)

    reported_radius_m = merged["radius_m"]

    assert reported_radius_m >= true_max_dist_m * 0.95, (
        f"merged stop reports radius_m={reported_radius_m:.1f}m, but the true "
        f"distance from its own reported centroid to the farthest original "
        f"member (A) is {true_max_dist_m:.1f}m -- the reported radius "
        f"silently understates the true spread of the samples it claims to "
        f"summarize by computing the recentring-displacement term from "
        f"dist(previous *original* stop, incoming stop) instead of "
        f"dist(running blob, incoming stop)."
    )
