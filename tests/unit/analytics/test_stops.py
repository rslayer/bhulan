"""Tests for :mod:`bhulan.analytics.stops`."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from bhulan.analytics.mobility import TrackSample
from bhulan.analytics.stops import detect_stops, merge_nearby_stops


def _drive_then_stop_then_drive(
    stop_minutes: float = 15.0,
) -> list[TrackSample]:
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    pts: list[TrackSample] = []

    # Drive away from (0, 0): 30 samples, 1s apart, moving ~11m per step
    for i in range(30):
        pts.append(TrackSample(lat=i * 0.0001, lon=0.0, ts_utc=t0 + timedelta(seconds=i)))

    # Stop for `stop_minutes` at (0.003, 0) with small jitter
    stop_start = t0 + timedelta(seconds=30)
    n_stop = int(stop_minutes * 60)
    for i in range(n_stop):
        jitter_lat = 0.003 + (i % 3) * 0.00001
        jitter_lon = (i % 5) * 0.00001
        pts.append(
            TrackSample(lat=jitter_lat, lon=jitter_lon, ts_utc=stop_start + timedelta(seconds=i))
        )

    # Drive away
    move_start = stop_start + timedelta(seconds=n_stop)
    for i in range(30):
        pts.append(
            TrackSample(
                lat=0.003 + i * 0.0001, lon=0.0, ts_utc=move_start + timedelta(seconds=i)
            )
        )
    return pts


def test_detect_stops_finds_single_stop_with_default_radius():
    pts = _drive_then_stop_then_drive(stop_minutes=15)
    stops = detect_stops(pts, radius_m=50.0, min_duration_s=300.0)
    assert len(stops) == 1
    assert stops[0].duration_s >= 15 * 60 - 2
    assert stops[0].sample_count >= 15 * 60 - 5


def test_detect_stops_ignores_short_stops():
    pts = _drive_then_stop_then_drive(stop_minutes=2)
    stops = detect_stops(pts, radius_m=50.0, min_duration_s=300.0)
    assert stops == []


def test_detect_stops_ignores_points_without_timestamps():
    pts = [TrackSample(0.0, 0.0, None), TrackSample(0.001, 0.0, None)]
    assert detect_stops(pts) == []


def test_merge_nearby_stops_combines_close_stops():
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    from bhulan.analytics.stops import Stop

    s1 = Stop(
        lat=12.9716,
        lon=77.5946,
        start_ts=t0,
        end_ts=t0 + timedelta(minutes=10),
        duration_s=600.0,
        radius_m=10.0,
        start_index=0,
        end_index=10,
        sample_count=11,
    )
    s2 = Stop(
        lat=12.9717,
        lon=77.5947,
        start_ts=t0 + timedelta(minutes=15),
        end_ts=t0 + timedelta(minutes=25),
        duration_s=600.0,
        radius_m=10.0,
        start_index=20,
        end_index=30,
        sample_count=11,
    )
    merged = merge_nearby_stops([s1, s2], merge_radius_m=50.0)
    assert len(merged) == 1
    assert merged[0].sample_count == 22
