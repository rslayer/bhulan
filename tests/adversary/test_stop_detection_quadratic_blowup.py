"""
Defect: ``detect_stops`` is documented as O(n log n) but is actually
O(n^2) (worse, for a single long cluster) on tracks where many
consecutive samples stay within ``radius_m`` of each other -- e.g. a
parked vehicle streaming GPS once a second for a long stop. This is
exactly the workload the endpoint is built to analyze, not a contrived
corner case.

Root cause: ``bhulan/analytics/stops.py::detect_stops`` (lines 74-111)
uses a double ``while`` loop. For each starting index ``i`` it grows a
window ``j`` one sample at a time and, on *every* growth step, calls
``_cluster_radius_m`` which recomputes the centroid and the max distance
over the *entire current window* (lines 41-45: ``np.mean`` + a full
``np.sqrt((xs - cx) ** 2 + ...)`` pass). Growing a single window of size
k from 1 to k costs 1 + 2 + ... + k = O(k^2) -- and the module docstring
itself claims this "brings the worst case down ... to O(n log n) in the
common case" (lines 6-8), which is inaccurate for the actual
implementation (no KD-tree is used anywhere in this file despite the
docstring's claim).

Measured on this endpoint (in-process, no network): a single track of
30,000 tightly clustered, evenly-timed points (one long "stop") takes
~1.9s; 50,000 points ~4.5s; 100,000 points (the service's own
``MAX_PUBLIC_POINTS``/``MAX_POINTS`` cap) ~15.5s of wall-clock CPU time
for one request.

Impact: an unauthenticated caller can tie up a worker process for many
seconds with a single request that is well within the service's own
published size limits (100,000 points), and the per-IP rate limit
(``RATE_LIMIT_INSIGHTS = "30/minute"``) does not prevent this -- a
handful of such requests before the limiter kicks in already produces
tens of seconds of sustained CPU load, a straightforward
denial-of-service vector against a stateless, unauthenticated endpoint.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

# 20,000 points is well under MAX_PUBLIC_POINTS (100,000) and a fraction
# of a realistic "long stop" track, yet already reproduces multi-hundred-
# millisecond quadratic blowup. A linear or O(n log n) implementation
# would comfortably finish this in well under 100ms.
_CLUSTER_SIZE = 20_000
_MAX_ACCEPTABLE_SECONDS = 0.5


def _clustered_track_payload(n: int):
    t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "lat": 1.0 + (i % 5) * 0.00001,
            "lon": 1.0,
            "ts_utc": (t0 + timedelta(seconds=i)).isoformat(),
        }
        for i in range(n)
    ]


def test_insights_clustered_track_completes_in_reasonable_time(client: TestClient):
    payload = {"points": _clustered_track_payload(_CLUSTER_SIZE)}
    start = time.monotonic()
    r = client.post("/v1/insights", json=payload)
    elapsed = time.monotonic() - start

    assert r.status_code == 200
    assert elapsed < _MAX_ACCEPTABLE_SECONDS, (
        f"a single-stop track of {_CLUSTER_SIZE} points took {elapsed:.2f}s "
        f"(expected < {_MAX_ACCEPTABLE_SECONDS}s) -- detect_stops is "
        f"quadratic in cluster size, not O(n log n) as documented; this is "
        f"a CPU-exhaustion / DoS vector on an unauthenticated endpoint"
    )
