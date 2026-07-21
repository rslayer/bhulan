"""
Defect (silently-wrong-answer): after a *legitimate* merge (two stops close
in BOTH space and time -- the exact GPS-jitter-cleanup use case
``merge_stops_within_m`` exists for), the merged stop's ``radius_m`` is
computed as ``max(prev.radius_m, s.radius_m)`` -- see
``bhulan/analytics/stops.py::merge_nearby_stops``:

    merged[-1] = Stop(
        ...
        radius_m=max(prev.radius_m, s.radius_m),
        ...
    )

This is not the spread of the merged cluster. The merged centroid is the
*unweighted average* of the two original centroids (``lat=(prev.lat +
s.lat)/2.0``), so when the two original stops sit apart from each other by
some distance ``d`` (up to ``merge_radius_m``), every original sample is now
roughly ``d/2`` away from the *new* centroid -- but ``radius_m`` only
reports ``max(prev.radius_m, s.radius_m)``, which is the spread *around the
old, discarded centroids* and ignores the displacement introduced by
recentring.

Concretely: two tight clusters (each with true radius ~0) sitting ~40m apart
merge (within ``merge_radius_m=60``) into one reported stop whose
``radius_m`` is ~0 -- while the actual spread of the *reported* centroid
against the real underlying samples is ~20m. A stop that claims a 0m radius
while its own centroid sits 20m from every sample that makes it up is a
silently wrong answer, and it's also self-contradictory: the stop was
detected with ``stop_radius_m=5`` (i.e. every detected stop is certified to
have spread <= 5m by construction), yet the merged report claims 0m while
the true spread against the reported centroid is ~20m -- 4x its own
detection radius.

This is exactly the trap the spec calls out (spec/spec.md, "Known traps for
the adversary to probe next": "Whether merge_nearby_stops recomputes
radius_m/sample_count correctly after a legitimate merge (not just
duration)."). Duration is correct (this cycle's fix); radius_m is not, and
this cycle's fix didn't touch that line.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)
from bhulan.analytics.geodesy import haversine_m

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LAT_A = 40.0
_LAT_B = 40.00036  # ~40m north of _LAT_A
_LON = -73.0


def _cluster(lat: float, lon: float, t_start: datetime, n: int = 6, step_s: int = 60) -> list[dict]:
    points = []
    t = t_start
    for _ in range(n):
        points.append({"lat": lat, "lon": lon, "ts_utc": t.isoformat()})
        t += timedelta(seconds=step_s)
    return points


def test_merged_stop_radius_m_reflects_true_spread(client: TestClient):
    """Two tight clusters ~40m apart, 2 minutes apart in time -- well within
    both ``merge_radius_m`` (space) and the gap threshold (time), so this is
    the legitimate jitter-merge case AC3 requires to keep working. The
    merged stop's reported ``radius_m`` must reflect the real spread of the
    samples it claims to summarize, not the spread of the discarded,
    pre-merge centroids.
    """
    cluster_a = _cluster(_LAT_A, _LON, _T0)
    b_start = _T0 + timedelta(seconds=6 * 60) + timedelta(minutes=2)
    cluster_b = _cluster(_LAT_B, _LON, b_start)

    true_separation_m = haversine_m(_LAT_A, _LON, _LAT_B, _LON)
    assert true_separation_m > 35.0  # sanity: the two clusters really are ~40m apart

    payload = {
        "points": cluster_a + cluster_b,
        "options": {"stop_radius_m": 50.0, "merge_stops_within_m": 60.0},
    }
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    stops = report["stops"]
    assert len(stops) == 1, (
        f"expected the two clusters to merge into a single stop (legitimate "
        f"jitter-merge case: close in both space and time), got {len(stops)}"
    )
    merged = stops[0]
    assert merged["sample_count"] == 12  # merge did happen, not a fluke of detection

    reported_radius_m = merged["radius_m"]
    # The true spread of the merged cluster's own reported centroid against
    # the real samples is >= half the original separation (triangle
    # inequality, since each original cluster's own points sit essentially
    # at that cluster's centroid).
    min_true_radius_m = true_separation_m / 2.0

    assert reported_radius_m >= min_true_radius_m * 0.9, (
        f"merged stop reports radius_m={reported_radius_m}, but the true "
        f"spread of its own centroid against the underlying samples is "
        f"~{min_true_radius_m:.1f}m (half the {true_separation_m:.1f}m gap "
        f"between the two original clusters) -- merge_nearby_stops computes "
        f"radius_m=max(prev.radius_m, s.radius_m), which measures spread "
        f"around the discarded pre-merge centroids and ignores the "
        f"displacement introduced by recentring to the merged centroid. The "
        f"stop was detected with stop_radius_m=5, so it silently reports a "
        f"radius far tighter than the samples it claims to summarize."
    )
