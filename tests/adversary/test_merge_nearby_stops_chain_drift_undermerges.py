"""
Defect (silently-wrong-answer / documented-contract violation): a chain of
3+ stops, each pairwise within ``merge_radius_m`` of its immediate neighbour
(and all close in time -- no gap), does not fully merge, because
``merge_nearby_stops`` (``bhulan/analytics/stops.py``) compares each
incoming stop against the *previous merge result's* recomputed centroid --
which drifts as the running average shifts -- rather than against the
original stop that immediately precedes it:

    for s in stops:
        ...
        prev = merged[-1]   # the *merged blob* so far, not the original neighbour
        ...
        close_in_space = haversine_m(prev.lat, prev.lon, s.lat, s.lon) <= merge_radius_m
        ...

The docstring says: "Merge consecutive stops whose centroids are within
``merge_radius_m``." Read plainly, three co-linear stops A, B, C each 40m
from their neighbour (A-B = 40m, B-C = 40m) with ``merge_radius_m=45`` are
all "consecutive stops whose centroids are within merge_radius_m" of their
neighbour -- a caller would reasonably expect all three to fold into one
stop. Instead:

  1. A merges with B (40m <= 45m) into a blob AB centred at the midpoint,
     20m from both A and B.
  2. C is then compared not against B (40m away, well within range) but
     against AB's drifted centroid, which is 60m from C (40m + 20m) --
     *outside* ``merge_radius_m`` -- so C is left unmerged.

The result is order/drift-dependent: whether two stops that are genuinely
close together merge depends on what else merged before them, even though
every adjacent original pair satisfies the documented distance test. For a
real GPS-jitter trail (a walk along a building's perimeter, say), this means
some jitter-split stops get silently left unmerged while others fold in,
purely as an artifact of which end of the chain the algorithm started from.
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


def test_chain_of_pairwise_close_stops_fully_merges(client: TestClient):
    """A, B, C are each within merge_radius_m of their immediate neighbour
    and all close in time (no gap anywhere in the chain), so the documented
    contract ("merge consecutive stops whose centroids are within
    merge_radius_m") says all three should collapse into a single stop.
    """
    cluster_a = _cluster(_LAT_A, _LON, _T0)
    b_start = _T0 + timedelta(seconds=6 * 60) + timedelta(minutes=2)
    cluster_b = _cluster(_LAT_B, _LON, b_start)
    c_start = b_start + timedelta(seconds=6 * 60) + timedelta(minutes=2)
    cluster_c = _cluster(_LAT_C, _LON, c_start)

    dist_ab = haversine_m(_LAT_A, _LON, _LAT_B, _LON)
    dist_bc = haversine_m(_LAT_B, _LON, _LAT_C, _LON)
    assert dist_ab <= _MERGE_RADIUS_M, "test setup: A-B must be within merge_radius_m"
    assert dist_bc <= _MERGE_RADIUS_M, "test setup: B-C must be within merge_radius_m"

    payload = {
        "points": cluster_a + cluster_b + cluster_c,
        "options": {"stop_radius_m": 100.0, "merge_stops_within_m": _MERGE_RADIUS_M},
    }
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    stops = report["stops"]
    assert len(stops) == 1, (
        f"A-B are {dist_ab:.1f}m apart and B-C are {dist_bc:.1f}m apart -- both "
        f"pairs within merge_radius_m={_MERGE_RADIUS_M}, and every gap in the "
        f"chain is well under the time-gap threshold -- yet merge_nearby_stops "
        f"produced {len(stops)} stops instead of 1. It compares each new stop "
        f"against the *previous merge result's drifted centroid* rather than "
        f"the immediately preceding original stop, so B (already folded into "
        f"an A-B blob centred 20m away from both) reads as {dist_bc + dist_ab / 2:.1f}m "
        f"from C even though the real B-C distance is {dist_bc:.1f}m -- silently "
        f"under-merging a chain where every adjacent original pair satisfies "
        f"the documented merge_radius_m test."
    )
