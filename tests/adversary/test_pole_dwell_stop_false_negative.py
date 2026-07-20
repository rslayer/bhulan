"""
Defect (silently-wrong-answer): ``bhulan/analytics/geodesy.py::latlon_to_xy_m``
degrades badly for a dwell very close to a pole -- one of the spec's own
named "known traps to probe next" for this cycle's antimeridian fix ("The
poles (lat +/-90) -- does the tangent-plane projection degrade there too?").

Near a pole, longitude is nearly degenerate: two points a few meters apart
can differ by up to 180 degrees of longitude, since every meridian passes
arbitrarily close to the pole. ``latlon_to_xy_m`` projects longitude
linearly (``x = dlon * meters_per_deg_lon`` with a single global
``meters_per_deg_lon = 111_320 * cos(lat0)``), which is only a valid
local-plane approximation when nearby points also have nearby longitudes
-- false near a pole. The result: a real, tight physical dwell near the
pole gets projected onto a much *wider* spread than its true extent,
because near-antipodal longitudes (e.g. 0 and 180) get scaled by the same
factor as a genuine 180-degree separation would need, even though the true
physical distance is a few meters.

Concretely: 30 samples alternating between (lat=89.9999, lon=0) and
(lat=89.9999, lon=180) -- true separation ~22.2m (per the *correct*
``haversine_m``), i.e. a true cluster radius (distance from centroid to
either point) of ~11.1m. This is well within a stop_radius_m of 15m by any
reasonable standard. But ``latlon_to_xy_m`` projects this pair to points
~17.5m apart from the cluster centroid, silently overstating the true
spread by ~57% and causing ``detect_stops`` to report zero stops.

Reachable via ordinary, unauthenticated ``points`` input to
``/v1/insights`` -- any real dwell within ~15-20m of true north/south pole
(polar research stations, polar tourism GPS traces).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)
import pytest
from fastapi.testclient import TestClient

from bhulan.analytics.geodesy import haversine_m

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LAT = 89.9999
_LON_A = 0.0
_LON_B = 180.0


def _polar_jitter_track(n: int = 30, step_s: int = 10) -> list[dict]:
    """``n`` samples alternating between two points ~22m apart near the
    north pole -- a realistic GPS-jitter dwell at a single spot very close
    to the pole (longitude is near-degenerate there, so any real fix
    reading can flip between near-opposite longitudes while barely
    moving).
    """
    points = []
    t = _T0
    for i in range(n):
        lon = _LON_A if i % 2 == 0 else _LON_B
        points.append({"lat": _LAT, "lon": lon, "ts_utc": t.isoformat()})
        t += timedelta(seconds=step_s)
    return points


def test_polar_dwell_within_true_radius_is_detected_as_a_stop(client: TestClient):
    true_separation_m = haversine_m(_LAT, _LON_A, _LAT, _LON_B)
    true_radius_m = true_separation_m / 2.0
    assert true_radius_m < 15.0  # sanity: genuinely tight by any real-world standard

    payload = {
        "points": _polar_jitter_track(),
        # A stop_radius_m comfortably above the *true* cluster radius
        # (~11.1m) but below latlon_to_xy_m's inflated projected radius
        # (~17.5m).
        "options": {"stop_radius_m": 15.0, "min_stop_minutes": 1.0},
    }
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    assert len(report["stops"]) == 1, (
        f"a 30-sample dwell within a true radius of {true_radius_m:.1f}m of "
        f"its own centroid (well inside stop_radius_m=15) near the north "
        f"pole (lat={_LAT}) was reported as {len(report['stops'])} stops "
        f"instead of 1 -- latlon_to_xy_m's local-tangent-plane projection "
        f"degrades near the poles (longitude is near-degenerate there), "
        f"inflating the projected cluster spread to ~17.5m and causing "
        f"detect_stops to reject a dwell that is, in true physical terms, "
        f"well within the requested radius."
    )
