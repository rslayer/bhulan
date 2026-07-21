"""
A single unauthenticated request must not tie up a worker. ``detect_stops``
re-grows a spatial cluster from each start sample when the previous cluster is
rejected; a crafted single giant cluster (a very slow drift, or a same-timestamp
mass) is never accepted, so the scan runs O(n·cluster_size) — ~70s of CPU at the
100k-point cap. ADR 0016 bounds this: a zero-duration cluster is skipped
wholesale (exact), and a scan-work budget caps the rest, degrading gracefully.
"""

import time
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity)
from bhulan.analytics.mobility import TrackSample
from bhulan.analytics.stops import StopScanBudgetExceeded, detect_stops

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _drift(n: int, step_deg: float, same_ts: bool):
    return [
        TrackSample(
            lat=40.0 + i * step_deg,
            lon=-100.0,
            ts_utc=_T0 if same_ts else _T0 + timedelta(seconds=i),
        )
        for i in range(n)
    ]


def test_same_timestamp_mass_is_on_not_quadratic():
    # 30k samples drifting slowly, ALL sharing one timestamp: zero duration, so
    # every cluster is skipped wholesale rather than re-grown. Must be quick and
    # report no stops. (Without the skip this is tens of seconds.)
    pts = _drift(30_000, 1e-6, same_ts=True)
    start = time.monotonic()
    stops = detect_stops(pts, radius_m=50.0, min_duration_s=300.0)
    elapsed = time.monotonic() - start
    assert stops == []
    assert elapsed < 3.0, f"same-timestamp mass took {elapsed:.1f}s — the zero-duration skip is not working"


def test_scan_work_budget_raises_on_a_giant_rejected_cluster():
    # A slow drift with real timestamps forms one big progressively-moving
    # cluster that is rejected and re-grown from every sample. With a modest
    # explicit budget the scan must bail rather than grind.
    pts = _drift(20_000, 1e-6, same_ts=False)
    with pytest.raises(StopScanBudgetExceeded):
        detect_stops(pts, radius_m=50.0, min_duration_s=300.0, max_scan_work=200_000)


def test_one_long_accepted_dwell_scans_in_linear_work():
    # Structural (op-count, not wall-clock) replacement for the removed flaky
    # timing test: a real N-sample dwell — the original "quadratic blowup" case —
    # is grown once, accepted as a stop, and the scan jumps past it, so the total
    # work is ~O(N). A tight linear budget (4*N) proves it: an O(N^2) recompute
    # (the pre-incremental-bound behaviour) would blow past it long before the end.
    n = 40_000
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    # A dense dwell: many samples jittering within a few metres of one spot.
    pts = [
        TrackSample(
            lat=40.0 + ((i % 5) - 2) * 1e-6,  # ~±0.2 m of jitter
            lon=-100.0 + ((i % 3) - 1) * 1e-6,
            ts_utc=base + timedelta(seconds=i),
        )
        for i in range(n)
    ]
    stops = detect_stops(pts, radius_m=50.0, min_duration_s=300.0, max_scan_work=4 * n)
    assert len(stops) == 1, "a single long dwell should be one stop"
    assert stops[0].sample_count == n, "the whole dwell should be one accepted cluster"


def test_uncapped_default_is_unchanged_for_a_normal_track():
    # A real drive → dwell → drive with the default (no budget) still finds the
    # one stop; the budget only applies when a caller passes max_scan_work.
    pts = []
    for i in range(2000):
        pts.append(TrackSample(lat=40.0 + i * 3e-4, lon=-100.0, ts_utc=_T0 + timedelta(seconds=i)))
    for i in range(700):
        pts.append(TrackSample(lat=46.0 + (i % 3) * 1e-5, lon=-100.0, ts_utc=_T0 + timedelta(seconds=2000 + i)))
    stops = detect_stops(pts, radius_m=50.0, min_duration_s=300.0)
    assert len(stops) == 1


def test_insights_endpoint_bounds_a_pathological_body(client: TestClient):
    # The real DoS: a 100k-point slow drift through /v1/insights must not hang.
    # It returns 200 with the other insights intact and a quality note that stop
    # detection was skipped — bounded to a few seconds, not ~70s.
    points = [
        {"lat": 40.0 + i * 1e-6, "lon": -100.0, "ts_utc": (_T0 + timedelta(seconds=i)).isoformat()}
        for i in range(100_000)
    ]
    start = time.monotonic()
    r = client.post("/v1/insights", json={"points": points})
    elapsed = time.monotonic() - start
    assert r.status_code == 200
    assert elapsed < 20.0, f"a 100k-point drift took {elapsed:.1f}s — the scan-work budget is not bounding it"
    body = r.json()
    assert body["stops"] == []
    assert any("too dense or degenerate" in issue for issue in body["quality"]["issues"])
    # the rest of the pipeline still ran
    assert body["summary"]["total_distance_km"] > 0
