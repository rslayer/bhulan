"""
Acceptance test for the merge span cap (ADR 0013): a merged stop is one place —
points clustered within ``stop_radius_m`` of a centre — never a smear along a
path.

A long line of stops, each within ``merge_stops_within_m`` of the next but
progressively walking away, must NOT collapse into one implausibly wide "stop".
It splits into several stops, each bounded to one ``stop_radius_m`` disk, so a
downstream consumer never sees a "stop" that is really a slow walk/drive.

Reachable unauthenticated via ``POST /v1/insights`` with the documented
``merge_stops_within_m`` option.
"""

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity)
from bhulan.analytics.geodesy import haversine_m

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LAT0, _LON = 40.0, -73.0
_STOP_RADIUS_M = 50.0
_MERGE_RADIUS_M = 45.0  # each hop is mergeable...
_STEP_M = 40.0          # ...but the chain walks _STEP_M each time
_N_STEPS = 8            # ~320 m end to end — far more than one stop
_METERS_PER_DEG_LAT = 111_320.0


def _walk_payload():
    points = []
    t = _T0
    north = 0.0
    for _ in range(_N_STEPS):
        lat = _LAT0 + north / _METERS_PER_DEG_LAT
        # a brief dwell at each hop so detect_stops emits a distinct stop
        for _ in range(2):
            points.append({"lat": lat, "lon": _LON, "ts_utc": t.isoformat()})
            t += timedelta(seconds=60)
        t += timedelta(seconds=1)
        north += _STEP_M
    return points


def test_progressive_walk_splits_into_bounded_stops(client: TestClient):
    payload = {
        "points": _walk_payload(),
        "options": {
            "stop_radius_m": _STOP_RADIUS_M,
            "min_stop_minutes": 0.5,
            "merge_stops_within_m": _MERGE_RADIUS_M,
        },
    }
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    stops = r.json()["stops"]

    total_span_m = _STEP_M * (_N_STEPS - 1)  # ~280 m

    # It must NOT all merge into one stop (the whole point of the cap).
    assert len(stops) > 1, (
        f"a {total_span_m:.0f}m progressive walk collapsed into a single 'stop' "
        f"despite stop_radius_m={_STOP_RADIUS_M}m — the merge is not capped"
    )

    # Every reported stop is bounded to one stop's radius (a genuine cluster
    # around a common centre), never a wide smear.
    eps = 1e-6
    for st in stops:
        assert st["radius_m"] <= _STOP_RADIUS_M + eps, (
            f"reported stop radius_m={st['radius_m']:.1f}m exceeds one "
            f"stop_radius_m={_STOP_RADIUS_M}m (ADR 0013)"
        )
