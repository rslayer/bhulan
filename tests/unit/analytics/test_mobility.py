"""Tests for :mod:`bhulan.analytics.mobility`."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from bhulan.analytics.mobility import (
    TrackSample,
    bbox,
    prepare_track,
    segment_by_motion,
    speed_stats_mps,
    time_range,
    total_distance_m,
)


def _track(start: datetime, steps):
    points = []
    t = start
    for dlat, dlon, dt in steps:
        t = t + timedelta(seconds=dt)
        points.append(TrackSample(lat=dlat, lon=dlon, ts_utc=t))
    return points


def test_prepare_track_sorts_and_dedupes():
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    a = TrackSample(1.0, 2.0, t0 + timedelta(seconds=10))
    b = TrackSample(1.0, 2.0, t0)
    c = TrackSample(1.0, 2.0, t0)  # duplicate
    cleaned = prepare_track([a, b, c])
    assert [p.ts_utc for p in cleaned] == [t0, t0 + timedelta(seconds=10)]


def test_prepare_track_adds_utc_when_naive():
    naive = datetime(2025, 1, 1, 12, 0, 0)
    cleaned = prepare_track([TrackSample(0.0, 0.0, naive)])
    assert cleaned[0].ts_utc.tzinfo is not None


def test_total_distance_zero_for_single_point():
    assert total_distance_m([TrackSample(0.0, 0.0, None)]) == 0.0


def test_total_distance_sums_consecutive_hops():
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    pts = [
        TrackSample(0.0, 0.0, t0),
        TrackSample(0.0, 0.001, t0 + timedelta(seconds=1)),
        TrackSample(0.0, 0.002, t0 + timedelta(seconds=2)),
    ]
    d = total_distance_m(pts)
    assert d == pytest.approx(222.4, rel=0.01)


def test_time_range_is_min_and_max():
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    pts = [
        TrackSample(0.0, 0.0, t0 + timedelta(seconds=10)),
        TrackSample(0.0, 0.0, t0),
        TrackSample(0.0, 0.0, None),
    ]
    start, end = time_range(pts)
    assert start == t0
    assert end == t0 + timedelta(seconds=10)


def test_bbox_none_for_empty():
    assert bbox([]) is None


def test_segment_by_motion_detects_stop_in_middle():
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    pts = []
    # Move for 60s
    for i in range(60):
        pts.append(
            TrackSample(lat=0.0 + i * 0.0001, lon=0.0, ts_utc=t0 + timedelta(seconds=i))
        )
    # Stop for 120s (same coords)
    stop_start = t0 + timedelta(seconds=60)
    for i in range(120):
        pts.append(
            TrackSample(lat=0.006, lon=0.0, ts_utc=stop_start + timedelta(seconds=i))
        )
    # Move again for 60s
    move_start = stop_start + timedelta(seconds=120)
    for i in range(60):
        pts.append(
            TrackSample(lat=0.006 + i * 0.0001, lon=0.0, ts_utc=move_start + timedelta(seconds=i))
        )
    segs = segment_by_motion(pts, moving_speed_mps=1.0, min_segment_s=30.0)
    kinds = [s.kind for s in segs]
    assert "stopped" in kinds
    assert "moving" in kinds


def test_segment_by_motion_returns_single_segment_without_timestamps():
    pts = [TrackSample(0.0, 0.0, None), TrackSample(0.001, 0.001, None)]
    segs = segment_by_motion(pts)
    assert len(segs) == 1
    assert segs[0].kind == "moving"


def test_speed_stats_reports_max_from_samples():
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    pts = [
        TrackSample(0.0, 0.0, t0, speed_mps=0.0),
        TrackSample(0.001, 0.0, t0 + timedelta(seconds=1), speed_mps=50.0),
    ]
    _, max_mps = speed_stats_mps(pts)
    assert max_mps >= 50.0
