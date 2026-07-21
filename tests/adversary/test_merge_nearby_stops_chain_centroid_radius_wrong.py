"""
Merge geometry (cycle 8, AC2): once ``merge_nearby_stops``
(``bhulan/analytics/stops.py``) transitively folds a *third* stop onto an
already-merged blob, the reported centroid and ``radius_m`` must still be
correct — the merged stop's centroid within ~``merge_radius_m`` of every member
and ``radius_m`` bounding the true spread. The original bug drifted the centroid
toward the tail of the chain (unweighted pairwise-midpoint recentring) and
understated ``radius_m``.

This exercises ``merge_nearby_stops`` **directly** (unit level) rather than
through ``/v1/insights``: a co-linear A/B/C chain routed through the endpoint is
— correctly, per ADR 0014 — rejected by ``detect_stops`` as *progressive
movement* before it ever reaches the merge, so merge geometry can only be
exercised by handing the merger the already-detected stops. (Merge geometry is a
``merge_nearby_stops`` concern; its API reachability is covered elsewhere.)
``stop_radius_m`` is set large enough that the chain is one stop's worth of
ground so the ADR-0013 cap admits the merge.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from bhulan.analytics.geodesy import haversine_m
from bhulan.analytics.stops import Stop, merge_nearby_stops

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LAT_A = 40.0
_LAT_B = 40.00036  # ~40m north of A
_LAT_C = 40.00072  # ~40m north of B, ~80m north of A
_LON = -73.0
_MERGE_RADIUS_M = 45.0
_STOP_RADIUS_M = 100.0  # the A..C chain (~80 m) is one stop's worth of ground here


def _stop(lat: float, idx: int, t_start: datetime, dur_s: float = 300.0, n: int = 6) -> Stop:
    """A tight already-detected stop at ``lat`` (radius ~0, ``n`` samples)."""
    return Stop(
        lat=lat,
        lon=_LON,
        start_ts=t_start,
        end_ts=t_start + timedelta(seconds=dur_s),
        duration_s=dur_s,
        radius_m=1.0,
        start_index=idx,
        end_index=idx,
        sample_count=n,
    )


def _chain() -> list[Stop]:
    a = _stop(_LAT_A, 0, _T0)
    b = _stop(_LAT_B, 1, a.end_ts + timedelta(minutes=2))
    c = _stop(_LAT_C, 2, b.end_ts + timedelta(minutes=2))
    return [a, b, c]


def _merge():
    return merge_nearby_stops(
        _chain(), merge_radius_m=_MERGE_RADIUS_M, stop_radius_m=_STOP_RADIUS_M
    )


def test_chain_merge_centroid_stays_within_merge_radius_of_every_member():
    """AC2: the merged stop's centroid must lie within ~``merge_radius_m`` of
    every member A, B, C — not drift toward the tail of the chain."""
    stops = _merge()
    assert len(stops) == 1, "A, B, C are within one stop radius and must merge into one stop"
    merged = stops[0]

    dist_to_a = haversine_m(merged.lat, merged.lon, _LAT_A, _LON)
    dist_to_b = haversine_m(merged.lat, merged.lon, _LAT_B, _LON)
    dist_to_c = haversine_m(merged.lat, merged.lon, _LAT_C, _LON)

    assert dist_to_a <= _MERGE_RADIUS_M and dist_to_b <= _MERGE_RADIUS_M and dist_to_c <= _MERGE_RADIUS_M, (
        f"AC2 requires the merged centroid within ~merge_radius_m={_MERGE_RADIUS_M}m "
        f"of every member; got A={dist_to_a:.1f}m B={dist_to_b:.1f}m C={dist_to_c:.1f}m"
    )


def test_chain_merge_radius_m_undersells_true_spread():
    """``radius_m`` must bound the true distance from the reported centroid to
    every original member it claims to summarize."""
    stops = _merge()
    assert len(stops) == 1, "expected the chain to fully merge into one stop"
    merged = stops[0]

    dist_to_a = haversine_m(merged.lat, merged.lon, _LAT_A, _LON)
    dist_to_b = haversine_m(merged.lat, merged.lon, _LAT_B, _LON)
    dist_to_c = haversine_m(merged.lat, merged.lon, _LAT_C, _LON)
    true_max_dist_m = max(dist_to_a, dist_to_b, dist_to_c)

    assert merged.radius_m >= true_max_dist_m * 0.95, (
        f"merged stop reports radius_m={merged.radius_m:.1f}m, but the true "
        f"distance from its reported centroid to the farthest member is "
        f"{true_max_dist_m:.1f}m — radius_m understates the true spread"
    )
