"""
Defect (metamorphic / silently-wrong-answer): ``merge_stops_within_m`` --
a documented, existing ``InsightsOptions`` field -- reintroduces exactly the
week-long-stop bug this cycle's fix (gap-aware ``detect_stops`` /
``detect_hotspots``) was supposed to eliminate.

``bhulan/analytics/insights.py::compute_insights`` does:

    raw_stops = detect_stops(prepared, ..., split_gap_s=opts.trip_split_gap_minutes * 60.0)
    stops = merge_nearby_stops(raw_stops, merge_radius_m=opts.merge_stops_within_m)

``detect_stops`` correctly produces two ~9-minute stops for two visits to the
same spot a week apart (that's what the cycle-1 fix bought us -- see
``test_stop_and_hotspot_ignore_time_gaps.py``). But
``bhulan/analytics/stops.py::merge_nearby_stops`` (never touched by this
cycle's fix, and never exercised by any adversary test -- see
``reports/adversary/coverage-gaps.md``, lines 199-213 of ``stops.py`` were
unprobed) merges *any* two consecutive stops whose centroids are within
``merge_radius_m``, with **no check at all on the elapsed time between
them**:

    if haversine_m(prev.lat, prev.lon, s.lat, s.lon) <= merge_radius_m:
        combined_start = prev.start_ts
        combined_end = s.end_ts
        duration = (combined_end - combined_start).total_seconds()
        ...

Two stops at the *same* location (distance 0, so any positive
``merge_stops_within_m`` merges them) are folded into one, and duration is
recomputed as ``combined_end - combined_start`` -- the entire calendar span
again, gap and all. This is not a contrived setting: ``merge_stops_within_m``
exists specifically to fix GPS-jitter-split stops
(insights.py/InsightsOptions docstring: "merge consecutive stops whose
centroids are within this radius"), so any caller who turns it on to clean
up jittery stops silently loses the cycle-1 gap-awareness fix for
``stops[].duration_min`` -- while ``hotspots[].time_spent_min`` /
``visit_count`` in the *very same response* correctly stays gap-aware,
since hotspots have no analogous merge step. The two fields disagree by
three orders of magnitude about how long the same physical dwell lasted.
"""

from __future__ import annotations

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_OFFICE_LAT = 40.0
_OFFICE_LON = -73.0
_GAP_SECONDS = 7 * 24 * 3600  # one week, device fully off -- no samples at all


def _two_visits_a_week_apart() -> list[dict]:
    points = []
    t = 0
    for _ in range(10):
        points.append(
            {"lat": _OFFICE_LAT, "lon": _OFFICE_LON, "ts_utc": (_T0 + timedelta(seconds=t)).isoformat()}
        )
        t += 60
    t += _GAP_SECONDS
    for _ in range(10):
        points.append(
            {"lat": _OFFICE_LAT, "lon": _OFFICE_LON, "ts_utc": (_T0 + timedelta(seconds=t)).isoformat()}
        )
        t += 60
    return points


def test_insights_merge_stops_within_m_reintroduces_week_long_stop(client: TestClient):
    """With ``merge_stops_within_m`` set (a real, documented option), the
    two 9-minute visits a week apart collapse back into one stop spanning
    the entire week -- the exact defect this cycle's ADR claims to have
    fixed, just reachable through a different, unpatched code path.
    """
    payload = {
        "points": _two_visits_a_week_apart(),
        "options": {"merge_stops_within_m": 100.0},
    }
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    stops = report["stops"]
    assert len(stops) >= 1, "expected at least one detected stop"
    longest_stop_minutes = max(s["duration_min"] for s in stops)

    assert longest_stop_minutes < 60, (
        f"detect_stops correctly splits the two visits, but "
        f"merge_nearby_stops (invoked via options.merge_stops_within_m) folds "
        f"them back into one stop lasting {longest_stop_minutes:.1f} minutes "
        f"(~{longest_stop_minutes / 60 / 24:.1f} days) -- merge_nearby_stops "
        f"in bhulan/analytics/stops.py has no time-gap check of its own, so "
        f"it silently re-merges two stops that detect_stops correctly split "
        f"on a real-world absence, reintroducing this cycle's headline bug "
        f"through an option that exists specifically to clean up GPS jitter"
    )


def test_insights_merge_stops_within_m_stop_duration_disagrees_with_hotspot_in_same_report(
    client: TestClient,
):
    """Internal-consistency check: within a *single* /v1/insights response,
    ``stops[].duration_min`` and ``hotspots[].time_spent_min`` describe the
    same physical dwell (same location, same track) and should agree in
    order of magnitude. With merge_stops_within_m set, they don't: hotspots
    stays gap-aware (~18 min) while the merged stop claims ~7 days.
    """
    payload = {
        "points": _two_visits_a_week_apart(),
        "options": {"merge_stops_within_m": 100.0},
    }
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    assert report["hotspots"], "expected at least one detected hotspot"
    assert report["stops"], "expected at least one detected stop"

    hotspot_time_spent_min = report["hotspots"][0]["time_spent_min"]
    longest_stop_minutes = max(s["duration_min"] for s in report["stops"])

    # Both describe dwell time at the same centroid in the same track; they
    # should be within the same ballpark (a factor of a few, generously).
    assert longest_stop_minutes < hotspot_time_spent_min * 10, (
        f"stops[].duration_min ({longest_stop_minutes:.1f} min) and "
        f"hotspots[].time_spent_min ({hotspot_time_spent_min:.1f} min) describe "
        f"the same physical location/dwell in the same /v1/insights response "
        f"but disagree by {longest_stop_minutes / hotspot_time_spent_min:.0f}x -- "
        f"hotspots correctly excludes the week-long gap while merge_nearby_stops "
        f"folds it back into the reported stop duration"
    )


def test_compare_per_track_merge_stops_within_m_reintroduces_gap_bug(client: TestClient):
    """Same defect, reached through /v1/compare's per-track reports (each
    track's report is built via the same compute_insights ->
    merge_nearby_stops path as /v1/insights).
    """
    payload = {
        "tracks": [
            {"label": "solo track", "points": _two_visits_a_week_apart()},
            {
                "label": "filler track",
                "points": [
                    {"lat": 10.0, "lon": 10.0, "ts_utc": _T0.isoformat()},
                    {
                        "lat": 10.001,
                        "lon": 10.001,
                        "ts_utc": (_T0 + timedelta(minutes=5)).isoformat(),
                    },
                ],
            },
        ],
        "options": {"merge_stops_within_m": 100.0},
    }
    r = client.post("/v1/compare", json=payload)
    assert r.status_code == 200
    report = r.json()

    track_stops = report["tracks"][0]["report"]["stops"]
    assert track_stops, "expected at least one detected stop in the first track"
    longest_stop_minutes = max(s["duration_min"] for s in track_stops)

    assert longest_stop_minutes < 60, (
        f"/v1/compare's per-track report (track 0) shows a stop lasting "
        f"{longest_stop_minutes:.1f} minutes for two 9-minute visits a week "
        f"apart -- merge_nearby_stops reintroduces the gap bug through the "
        f"compare endpoint's per-track insights just as it does through "
        f"/v1/insights directly"
    )
