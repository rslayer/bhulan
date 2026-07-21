"""
A stop is one *place* — points clustered around a common centre — not a window
sliding along a path. A slow walk or crawl that lasts longer than
``min_stop_minutes`` was previously chopped into ``stop_radius_m``-sized chunks
and each chunk reported as a phantom stop (a ~190 m walk → 2 "stops"). ADR 0014
rejects a cluster that progressively translates (its first- and second-half
centroids drift apart) instead of dwelling.

Reachable unauthenticated via ``POST /v1/insights``.
"""

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity)

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LON = -73.0
_METERS_PER_DEG_LAT = 111_320.0


def _walk(n: int, step_m: float, dt_s: float):
    """A straight progressive walk: n samples, each step_m further north."""
    pts = []
    t = _T0
    north = 0.0
    for _ in range(n):
        pts.append({"lat": 40.0 + north / _METERS_PER_DEG_LAT, "lon": _LON, "ts_utc": t.isoformat()})
        t += timedelta(seconds=dt_s)
        north += step_m
    return pts


def _dwell(n: int, dt_s: float):
    """A jittery dwell: n samples wandering (non-directionally) within ~15 m."""
    import math

    pts = []
    t = _T0
    for i in range(n):
        # deterministic pseudo-jitter with no net direction
        dlat = 0.00013 * math.sin(i * 2.3)
        dlon = 0.00013 * math.cos(i * 1.7)
        pts.append({"lat": 40.0 + dlat, "lon": _LON + dlon, "ts_utc": t.isoformat()})
        t += timedelta(seconds=dt_s)
    return pts


def test_slow_progressive_walk_reports_no_stops(client: TestClient):
    # ~200 m of steady northward walking over 20 min — movement, not a dwell.
    payload = {
        "points": _walk(n=40, step_m=5.0, dt_s=30.0),
        "options": {"stop_radius_m": 50.0, "min_stop_minutes": 5.0},
    }
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    stops = r.json()["stops"]
    assert stops == [] or len(stops) == 0, (
        f"a steady ~200 m walk must report zero stops, not phantom chunks; "
        f"got {len(stops)} stop(s)"
    )


def test_jittery_dwell_is_still_reported_as_one_stop(client: TestClient):
    # A real dwell that wanders within ~15 m for 20 min — one stop, not rejected.
    payload = {
        "points": _dwell(n=40, dt_s=30.0),
        "options": {"stop_radius_m": 50.0, "min_stop_minutes": 5.0},
    }
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    stops = r.json()["stops"]
    assert len(stops) == 1, (
        f"a jittery dwell (no net direction) must still be one stop; got "
        f"{len(stops)} — the progressive-movement filter must not reject a "
        f"genuine dwell"
    )
