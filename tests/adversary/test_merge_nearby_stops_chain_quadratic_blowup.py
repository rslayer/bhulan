"""
Defect (algorithmic complexity, cycle 8): ``merge_nearby_stops``'s new
per-blob geometry accumulator (``bhulan/analytics/stops.py::_merge_members``,
introduced this cycle to fix the chain-drift centroid/``radius_m`` bug) turns
what was an O(1)-per-stop pairwise update into an O(k) recompute over the
*entire* growing blob on every single stop folded in.

Root cause: inside ``merge_nearby_stops``'s loop, each time a new stop ``s``
is close enough to merge, the code does::

    groups[-1].append(s)
    merged[-1] = _merge_members(groups[-1])

``_merge_members`` iterates every member of ``groups[-1]`` (``sum(...)``,
``circular_mean_lon(...)``, and another ``max(...)`` generator, each O(k) for
a blob of ``k`` members so far) to emit the updated centroid/``radius_m``.
For a chain of ``n`` consecutive stops that all merge into one blob (an
ordinary GPS-jitter trail: a track drifting slowly with each detected stop
just outside ``stop_radius_m`` of the last but well inside
``merge_stops_within_m``), the total work across the chain is
``1 + 2 + ... + n`` = O(n^2) instead of O(n). The previous (cycle 7 and
earlier) pairwise-midpoint recentring was O(1) per merge step, so this
quadratic blowup is new in cycle 8's fix, not a pre-existing issue covered
by ``test_stop_detection_quadratic_blowup.py`` (which targets
``detect_stops``, a different function).

Measured directly against ``merge_nearby_stops`` (no HTTP/detect_stops
overhead): 1,000 chained stops ~0.44s, 2,000 ~1.72s, 4,000 ~6.78s, 8,000
~26.8s -- each doubling of ``n`` roughly quadruples the time, the signature
of O(n^2). End-to-end through ``/v1/insights`` (below) reproduces the same
growth: 500 stops ~0.17s, 1,000 ~0.54s, 2,000 ~1.9s.

Impact: an unauthenticated caller can submit an ordinary slowly-drifting GPS
trail with ``merge_stops_within_m`` set (a documented, public option) and
tie up a worker process for seconds on an input well within the service's
own size limits -- the exact same class of CPU-exhaustion DoS the
``detect_stops`` quadratic-blowup test already documents, just reopened in
the code this cycle's fix touches.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

# 2,000 merging stops (4,000 points) is a small, ordinary "slow drift" GPS
# trail -- far under MAX_POINTS (100,000) -- yet already reproduces multi-
# second quadratic blowup in the merge-geometry accumulator. An O(n) (or
# O(n log n)) implementation would comfortably finish this in well under
# 500ms, consistent with the bound used by the sibling
# ``test_stop_detection_quadratic_blowup.py``.
_N_STOPS = 2_000
_STOP_RADIUS_M = 2.0
_MERGE_RADIUS_M = 25.0
_STEP_M = 20.0  # > stop_radius_m (separate raw stops) but < merge_radius_m (all merge)
_MAX_ACCEPTABLE_SECONDS = 0.5


def _drifting_chain_payload(n_stops: int):
    points = []
    t = datetime(2024, 1, 1, tzinfo=timezone.utc)
    lat = 40.0
    lon = -73.0
    step_deg = _STEP_M / 111_320.0
    for _ in range(n_stops):
        points.append({"lat": lat, "lon": lon, "ts_utc": t.isoformat()})
        t += timedelta(seconds=1)
        points.append({"lat": lat, "lon": lon, "ts_utc": t.isoformat()})
        t += timedelta(seconds=2)
        lat += step_deg
    return points


def test_merging_long_drift_chain_completes_in_reasonable_time(client: TestClient):
    payload = {
        "points": _drifting_chain_payload(_N_STOPS),
        "options": {
            "stop_radius_m": _STOP_RADIUS_M,
            "min_stop_minutes": 0.001,
            "merge_stops_within_m": _MERGE_RADIUS_M,
        },
    }
    start = time.monotonic()
    r = client.post("/v1/insights", json=payload)
    elapsed = time.monotonic() - start

    assert r.status_code == 200
    stops = r.json()["stops"]
    assert len(stops) == 1, "test setup: the whole drift chain must merge into a single blob"
    assert elapsed < _MAX_ACCEPTABLE_SECONDS, (
        f"merging a {_N_STOPS}-stop drift chain took {elapsed:.2f}s (expected "
        f"< {_MAX_ACCEPTABLE_SECONDS}s) -- merge_nearby_stops's per-blob "
        f"geometry accumulator (_merge_members) recomputes the centroid and "
        f"radius_m over the *entire* growing blob on every stop folded in, "
        f"making a long merge chain O(n^2) instead of O(n); this is a "
        f"CPU-exhaustion / DoS vector on an unauthenticated endpoint, newly "
        f"introduced by this cycle's merge-geometry fix."
    )
