"""
Defect (silently-wrong-answer / AC1 violation, cycle 8): the new
``_merge_members`` (``bhulan/analytics/stops.py``) computes the merged
centroid's ``lat`` as the ``sample_count``-*weighted* mean of member
centroids, but its ``lon`` as the *unweighted* ``circular_mean_lon`` of the
same members (spec/spec.md section 2 explicitly permits this: "weight by
``sample_count`` if practical, otherwise an unweighted circular mean is
acceptable"). That asymmetry is exactly what breaks AC1.

Spec AC1 (spec/spec.md section 3): "for a merged chain A,B,C, the reported
centroid is within ``merge_radius_m`` (haversine) of every member A, B, and
C." The cycle-8 acceptance test only exercises three *equal-weight* stops
(6 samples each), where weighted-lat and unweighted-lon happen to agree
closely enough that AC1 holds. It never exercises a chain whose members have
different ``sample_count`` -- an entirely ordinary situation: a dense/long
dwell (many GPS samples over several minutes) sandwiched between two brief
stops (a couple of samples each), all still pairwise within
``merge_stops_within_m`` of their immediate predecessor -- so cycle 7's
merge-decision logic folds them into one stop exactly as it should.

When member B carries most of the weight, the weighted ``lat`` mean snaps
close to B's latitude while the *unweighted* ``lon`` mean stays near the
simple average of A/B/C's longitudes -- pulling the reported centroid off
the true line connecting A-B-C. For the concrete track below (A: 2 samples,
B: 300 samples, C: 2 samples, each consecutive pair ~40m apart, well inside
merge_stops_within_m=45), the reported centroid ends up **~48.8m from C**,
violating AC1 by a comfortable margin -- even though every pairwise
merge-decision distance was inside the 45m radius and the chain is exactly
the 3-member A,B,C case AC1 is written for.

(AC2 -- radius_m must not understate the true spread -- is *not* violated
here: the ``max(haversine(centroid, member) + member.radius_m)`` formula is
correct by the triangle inequality regardless of how the centroid drifts.
This is specifically an AC1 defect: the reported location itself walks
outside the merge radius of a member it claims to summarize.)

Reachable unauthenticated via ``/v1/insights`` with ``merge_stops_within_m``
on an ordinary track containing one long dwell between two brief ones.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)
from bhulan.analytics.geodesy import haversine_m

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LAT_A, _LON_A = 40.0, -73.0
_LAT_B, _LON_B = 40.00004503159829, -73.00052343533768
_LAT_C, _LON_C = 40.00040218060538, -73.00076808776453
_MERGE_RADIUS_M = 45.0


def _run() -> dict:
    points = []
    t = _T0
    # A: a brief 2-sample stop.
    for _ in range(2):
        points.append({"lat": _LAT_A, "lon": _LON_A, "ts_utc": t.isoformat()})
        t += timedelta(seconds=60)

    # B: a long, densely-sampled dwell -- ordinary for a real track (e.g. a
    # phone reporting once a second while parked) -- sitting ~40m from A.
    t += timedelta(minutes=2)
    for _ in range(300):
        points.append({"lat": _LAT_B, "lon": _LON_B, "ts_utc": t.isoformat()})
        t += timedelta(seconds=1)

    # C: another brief 2-sample stop, ~40m from B.
    t += timedelta(minutes=2)
    for _ in range(2):
        points.append({"lat": _LAT_C, "lon": _LON_C, "ts_utc": t.isoformat()})
        t += timedelta(seconds=60)

    return {
        "points": points,
        "options": {
            "stop_radius_m": 5.0,
            "min_stop_minutes": 0.5,
            "merge_stops_within_m": _MERGE_RADIUS_M,
        },
    }


def test_chain_merge_with_unequal_sample_counts_centroid_leaves_merge_radius(client: TestClient):
    """AC1 requires the merged centroid to stay within merge_radius_m of
    every member A, B, and C. A dense/long dwell (B, 300 samples) between two
    brief stops (A, C: 2 samples each) -- all pairwise within
    merge_stops_within_m -- still merges into one stop per cycle 7, but the
    sample_count-weighted lat / unweighted-circular-mean lon combination
    drags the reported centroid outside merge_radius_m of C.
    """
    payload = _run()
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    stops = report["stops"]
    assert len(stops) == 1, "test setup: A, B, C must merge into one stop (cycle 7 decision unchanged)"
    merged = stops[0]

    dist_to_a = haversine_m(merged["lat"], merged["lon"], _LAT_A, _LON_A)
    dist_to_b = haversine_m(merged["lat"], merged["lon"], _LAT_B, _LON_B)
    dist_to_c = haversine_m(merged["lat"], merged["lon"], _LAT_C, _LON_C)

    assert dist_to_a <= _MERGE_RADIUS_M and dist_to_b <= _MERGE_RADIUS_M and dist_to_c <= _MERGE_RADIUS_M, (
        f"AC1 requires the merged centroid to lie within merge_radius_m="
        f"{_MERGE_RADIUS_M}m of every member; got distances "
        f"A={dist_to_a:.1f}m B={dist_to_b:.1f}m C={dist_to_c:.1f}m -- "
        f"weighting lat by sample_count while leaving lon an unweighted "
        f"circular mean pulls the reported centroid off the true A-B-C line "
        f"once member weights are unequal, even though every consecutive "
        f"pairwise merge-decision distance was inside merge_radius_m."
    )
