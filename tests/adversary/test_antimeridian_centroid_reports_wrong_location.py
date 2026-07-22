"""
Defect (silently-wrong-answer, cycle-4 follow-on): now that
``latlon_to_xy_m`` correctly *clusters* a dwell that straddles the
antimeridian (cycle 4's fix), the cluster's reported **centroid** is still
computed with a plain, non-wraparound ``numpy.mean`` over the raw longitude
values in ``detect_stops`` (``bhulan/analytics/stops.py``),
``detect_hotspots`` (``bhulan/analytics/hotspots.py``), and
``merge_nearby_stops`` (``bhulan/analytics/stops.py``). Averaging
``179.9999`` and ``-179.9999`` the naive way gives ``~0.0`` -- the
**antipode** of the true location, roughly 20,000 km (half the
circumference of the Earth) away from where every sample in the cluster
actually sits.

Before cycle 4's fix this bug was masked: a dwell at the antimeridian was
never clustered at all (``stops: []`` / no hotspot), so no centroid was ever
computed for it. The fix makes the cluster detection correct, which
un-masks this second, independent bug: the *fact* of a stop/hotspot is now
right, but its *reported location* is off by ~180 degrees of longitude --
worse than before in a new way, since a client now gets a confident-looking
but wildly wrong pin on the map instead of an empty list.

Reachable via ordinary, unauthenticated ``points`` input to ``/v1/insights``
and ``/v1/compare`` -- any real dwell near Fiji, Tonga, Kiribati, the
Aleutians, or Chukotka.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)
from bhulan.analytics.geodesy import haversine_m
from bhulan.analytics.stops import Stop, merge_nearby_stops

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LAT = 10.0
_LON_A = 179.9999
_LON_B = -179.9999


def _jitter_track(n: int = 30, step_s: int = 10) -> list[dict]:
    points = []
    t = _T0
    for i in range(n):
        lon = _LON_A if i % 2 == 0 else _LON_B
        points.append({"lat": _LAT, "lon": lon, "ts_utc": t.isoformat()})
        t += timedelta(seconds=step_s)
    return points


def test_stop_centroid_at_antimeridian_reports_the_antipode(client: TestClient):
    """A stop whose samples all sit within ~22m of the 180th meridian must
    be reported near lon +/-180, not near lon 0 -- the point on the exact
    opposite side of the planet.
    """
    payload = {
        "points": _jitter_track(),
        "options": {"stop_radius_m": 50.0, "min_stop_minutes": 1.0},
    }
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    stops = r.json()["stops"]
    assert len(stops) == 1, "cycle-4 clustering fix regressed: expected exactly one stop"

    reported_lon = stops[0]["lon"]
    # Distance from the reported centroid to the true cluster location
    # (anywhere in the tiny jitter, e.g. lon_a) must be small; the naive
    # mean (~0.0) is ~20,000 km away.
    dist_to_truth_m = haversine_m(_LAT, reported_lon, _LAT, _LON_A)
    assert dist_to_truth_m < 1_000.0, (
        f"stop centroid reported lon={reported_lon!r} which is "
        f"{dist_to_truth_m / 1000.0:.0f} km from the true dwell location "
        f"(lat={_LAT}, lon~=+/-180) -- detect_stops averages the raw "
        f"longitudes of the cluster with a plain numpy.mean, which for "
        f"samples split across +179.9999/-179.9999 lands on the antipode "
        f"(lon~=0.0) instead of the true location."
    )


def test_hotspot_centroid_at_antimeridian_reports_the_antipode(client: TestClient):
    """Same defect, ``detect_hotspots`` path: the hotspot for a dwell at
    the antimeridian must be reported near lon +/-180, not lon 0.
    """
    payload = {"points": _jitter_track()}
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    hotspots = r.json()["hotspots"]
    assert len(hotspots) == 1, "cycle-4 clustering fix regressed: expected exactly one hotspot"

    reported_lon = hotspots[0]["lon"]
    dist_to_truth_m = haversine_m(_LAT, reported_lon, _LAT, _LON_A)
    assert dist_to_truth_m < 1_000.0, (
        f"hotspot centroid reported lon={reported_lon!r} which is "
        f"{dist_to_truth_m / 1000.0:.0f} km from the true dwell location "
        f"-- detect_hotspots averages raw longitudes with numpy.mean, "
        f"landing on the antipode for a cluster split across +/-180."
    )


def test_shared_hotspot_pooled_across_compare_reports_the_antipode(client: TestClient):
    """``/v1/compare``'s pooled ``shared_hotspots`` inherits the same
    centroid bug: a hotspot shared by two tracks that both dwell at the
    antimeridian is reported at the antipode instead of near +/-180.
    """
    payload = {
        "tracks": [
            {"label": "visit 1", "points": _jitter_track()},
            {
                "label": "visit 2",
                "points": _jitter_track(step_s=10)[:20],
            },
        ]
    }
    # Shift the second visit's timestamps so it's a distinct visit, not an
    # overlapping duplicate of the first.
    for p in payload["tracks"][1]["points"]:
        ts = datetime.fromisoformat(p["ts_utc"]) + timedelta(days=1)
        p["ts_utc"] = ts.isoformat()

    r = client.post("/v1/compare", json=payload)
    assert r.status_code == 200
    shared = r.json()["shared_hotspots"]
    assert len(shared) == 1, "expected the two visits to pool into one shared hotspot"

    reported_lon = shared[0]["lon"]
    dist_to_truth_m = haversine_m(_LAT, reported_lon, _LAT, _LON_A)
    assert dist_to_truth_m < 1_000.0, (
        f"pooled shared_hotspot centroid reported lon={reported_lon!r} "
        f"which is {dist_to_truth_m / 1000.0:.0f} km from the true "
        f"location -- same naive-mean bug, now visible in /v1/compare."
    )


def test_merge_nearby_stops_midpoint_at_antimeridian_reports_the_antipode():
    """``merge_nearby_stops`` computes the merged centroid as the plain
    midpoint ``(prev.lon + s.lon) / 2.0``. For two stops on opposite sides
    of the antimeridian (a device that dwelt at the same spot, jittered
    across +/-180, and got split into two adjacent stops) the midpoint of
    +179.9999 and -179.9999 is ~0.0 -- the antipode -- instead of +/-180.
    """
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 1, 0, 6, tzinfo=timezone.utc)
    t3 = datetime(2024, 1, 1, 0, 11, tzinfo=timezone.utc)

    s1 = Stop(
        lat=_LAT, lon=_LON_A, start_ts=t0, end_ts=t1, duration_s=300.0,
        radius_m=5.0, start_index=0, end_index=5, sample_count=6,
    )
    s2 = Stop(
        lat=_LAT, lon=_LON_B, start_ts=t2, end_ts=t3, duration_s=300.0,
        radius_m=5.0, start_index=6, end_index=11, sample_count=6,
    )

    merged = merge_nearby_stops([s1, s2], merge_radius_m=50.0)
    assert len(merged) == 1, "expected the two nearby stops to merge into one"

    reported_lon = merged[0].lon
    dist_to_truth_m = haversine_m(_LAT, reported_lon, _LAT, _LON_A)
    assert dist_to_truth_m < 1_000.0, (
        f"merged stop centroid reported lon={reported_lon!r} which is "
        f"{dist_to_truth_m / 1000.0:.0f} km from the true dwell location "
        f"-- merge_nearby_stops takes the plain midpoint of the two "
        f"stops' longitudes, landing on the antipode when they straddle "
        f"+/-180."
    )
