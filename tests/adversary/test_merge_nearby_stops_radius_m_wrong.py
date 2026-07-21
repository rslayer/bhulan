"""
Merge geometry (radius_m): when two tight stops ~40 m apart merge (the
legitimate jitter-merge case), the merged stop's reported ``radius_m`` must
reflect the real spread of its own reported centroid against the members it
summarizes — not the spread around the discarded pre-merge centroids (the old
``max(prev.radius_m, s.radius_m)`` bug reported ~0 m for a stop that truly spans
~20 m).

Exercised at the unit level against ``merge_nearby_stops`` directly: two
detected stops 40 m apart routed through ``/v1/insights`` would be one
progressive cluster and ``detect_stops`` (ADR 0014) rejects it before the merge,
so merge geometry is verified by handing the merger the already-detected stops.
``stop_radius_m`` is set large enough (ADR 0013 cap) to admit the merge.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from bhulan.analytics.geodesy import haversine_m
from bhulan.analytics.stops import Stop, merge_nearby_stops

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LAT_A = 40.0
_LAT_B = 40.00036  # ~40m north of _LAT_A
_LON = -73.0


def _stop(lat: float, idx: int, t_start: datetime, dur_s: float = 300.0, n: int = 6) -> Stop:
    return Stop(
        lat=lat,
        lon=_LON,
        start_ts=t_start,
        end_ts=t_start + timedelta(seconds=dur_s),
        duration_s=dur_s,
        radius_m=0.0,  # each original cluster is essentially a point
        start_index=idx,
        end_index=idx,
        sample_count=n,
    )


def test_merged_stop_radius_m_reflects_true_spread():
    """Two tight stops ~40 m apart merge; the merged ``radius_m`` must reflect
    the real spread (>= half the separation), not the discarded centroids."""
    true_separation_m = haversine_m(_LAT_A, _LON, _LAT_B, _LON)
    assert true_separation_m > 35.0  # sanity: really ~40m apart

    a = _stop(_LAT_A, 0, _T0)
    b = _stop(_LAT_B, 1, a.end_ts + timedelta(minutes=2))
    # merge_radius 60 (space) admits it; stop_radius 100 (cap) is one stop's
    # worth of ground, so ADR 0013 does not split it.
    stops = merge_nearby_stops([a, b], merge_radius_m=60.0, stop_radius_m=100.0)

    assert len(stops) == 1, f"the two close stops must merge into one; got {len(stops)}"
    merged = stops[0]
    assert merged.sample_count == 12  # merge did happen, not a fluke

    # The reported centroid sits between the two, so its distance to each real
    # member is >= half the separation (each original stop is essentially a
    # point at its own centroid).
    min_true_radius_m = true_separation_m / 2.0
    assert merged.radius_m >= min_true_radius_m * 0.9, (
        f"merged stop reports radius_m={merged.radius_m:.1f}m, but the true "
        f"spread of its centroid against the members is ~{min_true_radius_m:.1f}m "
        f"(half the {true_separation_m:.1f}m separation) — radius_m understates it"
    )
