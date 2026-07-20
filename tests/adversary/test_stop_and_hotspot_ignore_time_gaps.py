"""
Defect (metamorphic / silently-wrong-answer): a single detected "stop" (and,
identically, a single "hotspot") can silently span a real-world absence of
arbitrary length -- days, weeks, months -- with zero samples in between, as
long as the caller revisits the same location. The reported
``duration_min`` / ``time_spent_min`` then claims a single continuous
presence for the *entire* elapsed calendar time, including the gap, instead
of the sum of the actual dwell times.

Concretely: a device that is parked at the office for 10 minutes, then
switched off for a week (no samples at all -- not even a data gap the
device tried and failed to report), then parked at the *same* spot again
for 10 minutes, is reported as ONE "stop" lasting >7 days
(``duration_min: 10099.0``), and correspondingly one "hotspot" visit
(``visit_count: 1``, ``time_spent_min: 10099.0``) -- instead of two
9-minute-apart visits totalling 18-20 minutes of actual presence. This is
not a contrived pathological input: "the same commonly-visited place, days
or weeks apart" is exactly the recurring-location use case
``bhulan/analytics/hotspots.py``'s own module docstring calls out
("multi-track inputs ... hotspots are the primary way to identify
recurring places across days").

Root cause -- two independent gap-blind implementations:

1. ``bhulan/analytics/stops.py::detect_stops`` (lines 96-152) only checks
   the *spatial* spread of a growing window (via ``_cluster_end`` /
   ``_cluster_radius_m``) against ``radius_m``. It never inspects the time
   gap between consecutive samples in the window, so two visits to the same
   spot merge into one cluster/"stop" regardless of how much real time (and
   how many missing samples) separate them. Contrast this with
   ``bhulan/analytics/trips.py::_trip_bounds`` (lines 83-89), which
   explicitly splits on ``trip_split_gap_seconds`` between consecutive
   samples for exactly this reason -- trips get gap-awareness, stops do
   not.

2. ``bhulan/analytics/hotspots.py::_time_spent_s`` /
   ``_visit_count`` (lines 107-148) define a "visit" purely as a run of
   *consecutive array indices* that fall in the same grid cell, with no
   check on the elapsed time between them. Once ``prepare_track`` sorts all
   samples by timestamp, two revisits to the same grid cell with nothing
   else recorded in between become adjacent indices and are indistinguishable
   from one continuous dwell -- ``_time_spent_s`` sums every inter-index
   ``dt`` inside the cluster including the multi-day gap, and
   ``_visit_count`` counts it as a single unbroken run.

Both effects reproduce identically through ``/v1/insights`` (a single
track) and pool straight through ``/v1/compare``'s ``shared_hotspots`` (two
separate tracks recorded on different days at the same spot report
``visit_count: 1`` for what are obviously two distinct visits).

Impact: any client relying on ``stops[].duration_min`` or
``hotspots[].time_spent_min`` / ``visit_count`` to answer "how long did the
user spend at this place" or "how many times did they visit" gets a
silently wrong answer that grows unboundedly with the calendar gap between
visits -- there is no crash or rejection to signal the problem.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)

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


def test_insights_stop_duration_does_not_span_a_week_long_absence(client: TestClient):
    payload = {"points": _two_visits_a_week_apart()}
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    # Sanity check the fixture actually represents two distinct visits: the
    # trip segmenter (which IS gap-aware) correctly reports two trips.
    assert len(report["trips"]) == 2, report["trips"]

    stops = report["stops"]
    assert len(stops) >= 1, "expected at least one detected stop"
    longest_stop_minutes = max(s["duration_min"] for s in stops)

    # Real dwell time at the office is ~9 minutes per visit (10 samples,
    # 60s apart). A stop duration anywhere near a week long means the
    # week-long absence was silently folded into "time spent at the stop".
    assert longest_stop_minutes < 60, (
        f"detect_stops reported a stop lasting {longest_stop_minutes:.1f} minutes for "
        f"two 9-minute visits separated by a 7-day absence with zero samples in "
        f"between -- the spatial-only clustering in "
        f"bhulan/analytics/stops.py::detect_stops has no time-gap check (unlike "
        f"trips._trip_bounds), so it silently merges two distinct real-world "
        f"visits into one stop spanning the whole calendar gap"
    )


def test_insights_hotspot_visit_count_and_dwell_time_do_not_span_a_week_long_absence(
    client: TestClient,
):
    payload = {"points": _two_visits_a_week_apart()}
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    hotspots = report["hotspots"]
    assert len(hotspots) >= 1, "expected at least one detected hotspot"
    hotspot = hotspots[0]

    assert hotspot["visit_count"] >= 2, (
        f"hotspot reports visit_count={hotspot['visit_count']} for two visits to "
        f"the same location separated by a 7-day absence with zero intervening "
        f"samples -- bhulan/analytics/hotspots.py::_visit_count treats any run of "
        f"consecutive *array indices* in the same grid cell as one visit, with no "
        f"check on the elapsed time between them, so it can't tell a genuine "
        f"revisit from continuous presence"
    )
    assert hotspot["time_spent_min"] < 60, (
        f"hotspot reports time_spent_min={hotspot['time_spent_min']} (~"
        f"{hotspot['time_spent_min'] / 60:.1f} hours) for two 9-minute visits a "
        f"week apart -- bhulan/analytics/hotspots.py::_time_spent_s sums every "
        f"inter-sample duration inside the cluster's index run, including the "
        f"multi-day gap between the two real visits"
    )


def test_compare_shared_hotspot_visit_count_across_tracks_recorded_days_apart(
    client: TestClient,
):
    """Same defect, reached through /v1/compare's pooled-hotspot pass: two
    *separate* tracks recorded on different days at the same spot should
    obviously register as 2 visits, not 1.
    """

    def _one_visit(day_offset: int) -> list[dict]:
        pts = []
        t = 0
        for _ in range(10):
            pts.append(
                {
                    "lat": _OFFICE_LAT,
                    "lon": _OFFICE_LON,
                    "ts_utc": (_T0 + timedelta(days=day_offset, seconds=t)).isoformat(),
                }
            )
            t += 60
        return pts

    payload = {
        "tracks": [
            {"label": "Monday", "points": _one_visit(0)},
            {"label": "next Monday", "points": _one_visit(7)},
        ]
    }
    r = client.post("/v1/compare", json=payload)
    assert r.status_code == 200
    report = r.json()

    shared = report["shared_hotspots"]
    assert len(shared) >= 1, "expected at least one shared hotspot"
    assert shared[0]["visit_count"] >= 2, (
        f"POST /v1/compare pooled shared_hotspots reports visit_count="
        f"{shared[0]['visit_count']} for two tracks recorded a week apart at the "
        f"same location -- same root cause as the single-track case: "
        f"_visit_count/_time_spent_s key off array-index adjacency after "
        f"prepare_track's global time-sort, not elapsed time"
    )
