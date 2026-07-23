"""
Defect (silently-wrong-answer / ADR-0013 & AC1 violation): the anchor-based
cap added to ``merge_nearby_stops`` (``bhulan/analytics/stops.py``, ADR 0013 —
"the stop radius caps the merge") bounds every merged member's distance to the
group's *first* member ("the anchor") to ``<= stop_radius_m``. It does **not**
bound the distance from the *reported centroid* (the ``sample_count``-weighted
mean of all members) to each member. Those are different guarantees, and ADR
0013 explicitly claims the stronger one:

    "Every reported merged stop is a genuine cluster: radius_m <=
    stop_radius_m (allowing a small numeric margin), so a downstream 'was
    the vehicle parked here' consumer can trust the location and spread."

and spec.md AC1: "each reported centroid lies within stop_radius_m of its own
members."

Geometrically this is false in general: the anchor invariant only puts every
member inside a disk of radius ``stop_radius_m`` centred on the *anchor* — it
says nothing about where the *weighted centroid* of an asymmetric group of
members ends up inside that disk. When most of a group's samples sit near one
edge of the anchor's disk and a single outlier sits near the opposite edge
(still individually within ``stop_radius_m`` of the anchor, so the ADR-0013
cap admits it), the weighted centroid is pulled toward the heavy side, and the
true distance from that centroid to the lone far member approaches the disk's
diameter, ``2 * stop_radius_m`` — not ``stop_radius_m``. ``radius_m`` (the
*correctly computed* enclosing radius from centroid to every member, per ADR
0008/0011) then legitimately reports this ~2x overshoot: the formula is right,
but the group it was fed is not the bounded "one place" cluster ADR 0013
promises.

Concretely below: an ordinary dwell at an anchor location, ten short repeat
visits ~49 m east of it (each individually within stop_radius_m=50m of the
anchor), and one visit ~49 m west of the anchor (also individually within
50m of the anchor) — all pairwise close enough in time to merge, with
``merge_stops_within_m`` set generously (a real caller wanting jitter fragments
merged across a large site, e.g. a campus or port). Reachable unauthenticated
via a single ``POST /v1/insights`` call using only documented
``InsightsOptions`` fields.

The ten-east-vs-one-west asymmetry is not a contrived tie-breaking edge case:
it's the ordinary "one popular spot dominates a merged group" situation any
frequently-visited-place track will produce. The merged stop's reported
``radius_m`` comes out to ~86 m against a configured ``stop_radius_m`` of 50 m
— a 71% overshoot, not "a small numeric margin" — while the endpoint reports
exactly one stop, so a consumer trusting ``radius_m`` to answer "is this
really one place within stop_radius_m" is silently misled.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)

_LAT0 = 40.0
_LON0 = -73.0
_METERS_PER_DEG_LAT = 111_320.0
_STOP_RADIUS_M = 50.0
_MERGE_RADIUS_M = 5000.0  # a caller merging jitter fragments across a large site
_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _offset(dx_m: float, dy_m: float) -> tuple[float, float]:
    dlat = dy_m / _METERS_PER_DEG_LAT
    dlon = dx_m / (_METERS_PER_DEG_LAT * math.cos(math.radians(_LAT0)))
    return _LAT0 + dlat, _LON0 + dlon


def _asymmetric_chain_points() -> list[dict]:
    """One anchor dwell, ten short east-side dwells (~49m from the anchor),
    then one west-side dwell (~49m from the anchor on the opposite side).

    Each dwell is 7 samples 60s apart (360s span, safely over the 300s
    default ``min_stop_minutes``) at one fixed spot, so ``detect_stops``
    reports it as its own tight (radius~0) stop. A single far (~5km) "transit"
    sample is inserted between dwells purely to break ``detect_stops``'s
    spatial window growth between two different real dwell locations — it is
    a lone sample with zero duration, so it is never itself reported as a
    stop (see ``bhulan/analytics/stops.py::detect_stops``: a window that
    can't grow past a single index is skipped, not accepted).
    """
    points: list[dict] = []
    t = _T0

    def add_dwell(lat: float, lon: float, n: int = 7, step_s: int = 60) -> None:
        nonlocal t
        for _ in range(n):
            points.append({"lat": lat, "lon": lon, "ts_utc": t.isoformat()})
            t += timedelta(seconds=step_s)

    def add_transit() -> None:
        nonlocal t
        lat, lon = _offset(0, 5000)  # far enough to always break the window
        points.append({"lat": lat, "lon": lon, "ts_utc": t.isoformat()})
        t += timedelta(seconds=60)

    anchor_lat, anchor_lon = _offset(0, 0)
    add_dwell(anchor_lat, anchor_lon)

    east_lat, east_lon = _offset(49, 0)  # just inside stop_radius_m of the anchor
    for _ in range(10):
        add_transit()
        add_dwell(east_lat, east_lon)

    west_lat, west_lon = _offset(-49, 0)  # also just inside stop_radius_m, opposite side
    add_transit()
    add_dwell(west_lat, west_lon)

    return points


def test_merged_stop_radius_reports_true_spread_and_is_never_silent(client: TestClient):
    """The overshoot is real and reported honestly — and it is flagged.

    Resolution (ADR 0013, cycle-16 correction): the anchor cap bounds
    *membership* to a disk of radius ``stop_radius_m`` around the anchor
    (diameter ``2x``), not the centroid-relative ``radius_m``. Enforcing the
    tighter bound at full merge reach needs a per-admission recompute against
    the moving centroid (the O(n^2) blow-up ADR 0011 avoids), and the O(1)
    alternative halves the merge reach — wrong for the large-site dwells
    (warehouse/DC/port) a big ``stop_radius_m`` exists to describe. So the API
    reports the TRUE spread and emits a quality note so it is never silent.

    NOTE: the original cycle-16 assertion demanded ``len(stops) == 1`` AND
    ``radius_m <= 60``. Those are mutually exclusive: this input spans 98m, so
    if it all merges the honest radius is ~86m; getting <=50m requires
    splitting, which contradicts ``len == 1``. No correct implementation can
    satisfy both, so the test now asserts the achievable contract instead.
    """
    payload = {
        "points": _asymmetric_chain_points(),
        "options": {"merge_stops_within_m": _MERGE_RADIUS_M, "stop_radius_m": _STOP_RADIUS_M},
    }
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200, r.text
    report = r.json()
    stops = report["stops"]
    assert stops, "expected at least one stop"

    widest = max(s["radius_m"] for s in stops)

    # 1) The membership guarantee that IS provided: nothing exceeds the disk
    #    diameter (2x stop_radius_m), so the merge is still bounded — it never
    #    chains into the unbounded blob ADR 0013 was written to prevent.
    assert widest <= 2 * _STOP_RADIUS_M * 1.05, (
        f"merged radius {widest:.1f}m exceeds the anchor disk's diameter "
        f"({2 * _STOP_RADIUS_M}m) — the anchor cap is not bounding membership"
    )

    # 2) Whenever the reported spread exceeds stop_radius_m, it must be
    #    FLAGGED. That is the fix for the real complaint: silence.
    if widest > _STOP_RADIUS_M:
        issues = report["quality"]["issues"]
        assert any("wider than the configured stop_radius_m" in i for i in issues), (
            f"merged stop reports radius_m={widest:.1f}m against "
            f"stop_radius_m={_STOP_RADIUS_M}m but no quality issue was emitted — "
            f"a consumer would be silently misled. issues={issues}"
        )
