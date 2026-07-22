"""
Defect (silently-wrong-answer): when ``detect_stops`` hits its scan-work budget
partway through a track, it raised ``StopScanBudgetExceeded`` and
``compute_insights`` caught it by setting ``raw_stops = []`` — discarding *every*
stop, including real dwells already detected before the offending segment.

So an ordinary track — a real 10-minute dwell followed by a long slow walk
(~9k samples, entirely realistic for a hike/commute file) — trips the absolute
12M-work cap on the walk segment and returns **zero stops**, silently dropping
the real dwell. The budget was meant to prevent a DoS from a pathological single
giant cluster; instead it turned one dense segment into "no stops" for a whole
legitimate file.

Fix: the budget still bounds work (DoS protection intact), but on exceeding it
``detect_stops`` hands back ``partial_stops`` — the stops found before the
budget was hit — so the caller keeps them and only truncates detection past that
point (plus a quality note). See ADR 0016.
"""

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity)
from bhulan.analytics.mobility import TrackSample
from bhulan.analytics.stops import StopScanBudgetExceeded, detect_stops

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _real_dwell(n: int, start_s: int):
    # n samples at one fixed spot, one per minute -> a genuine multi-minute stop.
    return [
        TrackSample(lat=40.0, lon=-73.0, ts_utc=_T0 + timedelta(seconds=start_s + i * 60))
        for i in range(n)
    ]


def _slow_drift(n: int, start_s: int):
    # A slow directional walk within one radius: forces repeated exact-spread
    # recomputes, i.e. the O(n*cluster_size) work the budget is there to cap.
    return [
        TrackSample(lat=41.0 + i * 7e-8, lon=-73.0, ts_utc=_T0 + timedelta(seconds=start_s + i))
        for i in range(n)
    ]


def test_detect_stops_returns_partial_stops_on_budget_exceeded():
    """Unit level: the real dwell is on the exception's partial_stops."""
    track = _real_dwell(11, 0) + _slow_drift(4000, 3600)
    # A low budget forces the cap deterministically once the drift segment is
    # scanned (the drift alone charges ~1 work unit per sample).
    with pytest.raises(StopScanBudgetExceeded) as ei:
        detect_stops(track, radius_m=50.0, min_duration_s=300.0, max_scan_work=1000)
    partial = ei.value.partial_stops
    assert len(partial) == 1, f"expected the real dwell preserved, got {partial}"
    assert partial[0].sample_count == 11
    # And the whole track under the default (very high) cap still finds the dwell.
    assert len(detect_stops(track, radius_m=50.0, min_duration_s=300.0)) >= 1


def test_insights_keeps_real_stop_before_a_budget_tripping_walk(client: TestClient):
    """End to end: a real dwell + a ~9k-point slow walk must not return 0 stops."""
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    def iso(s):
        return (t0 + timedelta(seconds=s)).isoformat().replace("+00:00", "Z")

    dwell = [{"lat": 40.0, "lon": -73.0, "ts_utc": iso(i * 60)} for i in range(11)]
    walk = [{"lat": 41.0 + i * 7e-8, "lon": -73.0, "ts_utc": iso(3600 + i)} for i in range(9000)]

    r = client.post("/v1/insights", json={"points": dwell + walk})
    assert r.status_code == 200, r.text[:200]
    body = r.json()
    assert len(body["stops"]) >= 1, (
        "the real 10-minute dwell must survive a later budget-tripping walk, "
        f"got {len(body['stops'])} stops"
    )
    # The surviving stop is the real dwell, not an artefact of the walk.
    assert any(s["sample_count"] == 11 for s in body["stops"])
