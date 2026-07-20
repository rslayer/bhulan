"""
Defect (metamorphic / silently-wrong-answer): consecutive GPS samples
that share an identical timestamp but differ in position are classified
as a single "stopped" segment -- yet that same segment's own
``distance_km`` field reports the real, nonzero distance between them.
The response is internally self-contradictory: a segment cannot be
"stopped" (zero speed, not moving) while also having travelled several
kilometres.

Concretely: 20 points spread linearly over ~2.1 km of latitude, all
timestamped identically (a realistic artifact of loggers that batch-flush
buffered fixes with one wall-clock stamp, or that round to whole
seconds), produce:

    "segments": [{"kind": "stopped", "distance_km": 2.1127,
                  "duration_min": 0.0, "avg_speed_kmh": 0.0}]

and ``summary.max_speed_kmh: 0.0`` / ``summary.avg_moving_speed_kmh: 0.0``
despite ``summary.total_distance_km: 2.1127``. No entry is added to
``quality.issues`` to flag the anomaly.

Root cause: two independent divide-by-zero guards in
``bhulan/analytics/mobility.py`` silently collapse "distance covered in
zero elapsed time" to a *speed of zero* instead of leaving it undefined
or flagging it:

1. ``segment_by_motion`` (around line 156-157):
   ``step_speed = np.where(step_secs > 0, step_dist / step_secs, 0.0)``
   -- when ``step_secs`` is 0 (identical consecutive timestamps) the
   per-sample speed used for the moving/stopped classification is forced
   to 0.0 regardless of how far apart the two points actually are, so the
   whole run gets classified "stopped".
2. ``speed_stats_mps`` (around line 248-252): the per-step max-speed scan
   only considers a step ``if s > 0``, so a same-timestamp step's
   distance never contributes to ``max_speed_kmh`` either.

But the segment's ``distance_m``/``distance_km`` (``segment_by_motion``,
``dist = float(np.sum(step_dist[a:b]))``) is computed unconditionally
from the same ``step_dist`` array, with no such guard -- so the (correct)
distance and the (silently wrong) "stopped, 0 km/h" classification for
the very same span both end up in the response together, contradicting
each other.

Impact: any client that filters/aggregates by segment ``kind`` (e.g. "how
far did they travel while stopped" should always be ~0) gets an answer
that is off by the entire zero-time-delta hop -- and a genuinely
anomalous input (real displacement across zero elapsed time, which is
physically impossible and usually indicates a clock/logging glitch) is
reported with total confidence and no ``quality`` warning at all, instead
of being flagged the way duplicate points already are (see
``quality.issues: ["Removed N duplicate points"]`` for exact duplicates).
"""

from __future__ import annotations

from fastapi.testclient import TestClient

_DISTANCE_EPSILON_KM = 0.01


def test_stopped_segment_does_not_report_multi_km_distance(client: TestClient):
    points = [
        {"lat": 10.0 + i * 0.001, "lon": 20.0, "ts_utc": "2024-01-01T00:00:00Z"}
        for i in range(20)
    ]
    r = client.post("/v1/insights", json={"points": points})
    assert r.status_code == 200, f"unexpected status {r.status_code}: {r.text[:200]}"
    body = r.json()

    assert body["summary"]["total_distance_km"] > 2.0, (
        "sanity check: the points really are ~2.1km apart in total -- if this "
        "fails the test fixture itself is wrong, not the defect"
    )

    stopped_distance_km = sum(
        seg["distance_km"] for seg in body["segments"] if seg["kind"] == "stopped"
    )
    assert stopped_distance_km <= _DISTANCE_EPSILON_KM, (
        f"a segment classified 'stopped' reports {stopped_distance_km} km of "
        f"travel (expected ~0) -- consecutive samples sharing an identical "
        f"timestamp but differing in position are misclassified as 'stopped' "
        f"purely because step_secs==0 forces speed to 0.0, while the "
        f"segment's own distance_km still (correctly) sums the real "
        f"haversine distance between them: segments={body['segments']!r}"
    )


def test_zero_time_delta_movement_does_not_silently_zero_max_speed(client: TestClient):
    """The distance is real (total_distance_km > 0); a max_speed_kmh of
    exactly 0.0 for the same input is an internally inconsistent answer,
    not merely a conservative one -- it should at least surface as a
    quality issue if the service intends to report it as 0."""
    points = [
        {"lat": 10.0 + i * 0.001, "lon": 20.0, "ts_utc": "2024-01-01T00:00:00Z"}
        for i in range(20)
    ]
    r = client.post("/v1/insights", json={"points": points})
    assert r.status_code == 200, f"unexpected status {r.status_code}: {r.text[:200]}"
    body = r.json()

    total_km = body["summary"]["total_distance_km"]
    max_speed = body["summary"]["max_speed_kmh"]
    issues = body["quality"]["issues"]

    assert not (total_km > 2.0 and max_speed == 0.0 and not issues), (
        f"summary reports total_distance_km={total_km} (real movement) but "
        f"max_speed_kmh={max_speed} with no quality.issues warning -- these "
        f"two facts contradict each other: distance was covered but no speed "
        f"is ever attributed to it, and nothing tells the caller their input "
        f"has a zero-elapsed-time hop. quality={body['quality']!r}"
    )
