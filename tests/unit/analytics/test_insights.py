"""Tests for :mod:`bhulan.analytics.insights`."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from bhulan.analytics.insights import (
    InsightsOptions,
    InsightsRequest,
    PointIn,
    compute_insights,
)


def _build_points(stop_minutes: float = 15.0) -> list[PointIn]:
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    pts: list[PointIn] = []
    for i in range(30):
        pts.append(
            PointIn(lat=i * 0.0001, lon=0.0, ts_utc=t0 + timedelta(seconds=i))
        )
    stop_start = t0 + timedelta(seconds=30)
    n_stop = int(stop_minutes * 60)
    for i in range(n_stop):
        pts.append(
            PointIn(
                lat=0.003 + (i % 3) * 0.000001,
                lon=(i % 5) * 0.000001,
                ts_utc=stop_start + timedelta(seconds=i),
            )
        )
    move_start = stop_start + timedelta(seconds=n_stop)
    for i in range(30):
        pts.append(
            PointIn(
                lat=0.003 + i * 0.0001,
                lon=0.0,
                ts_utc=move_start + timedelta(seconds=i),
            )
        )
    return pts


def test_compute_insights_end_to_end():
    req = InsightsRequest(
        points=_build_points(stop_minutes=15),
        options=InsightsOptions(stop_radius_m=50.0, min_stop_minutes=5.0),
    )
    report = compute_insights(req)
    assert report.summary.point_count == len(req.points)
    assert report.summary.accepted_point_count > 0
    assert len(report.stops) == 1
    assert report.stops[0].duration_min >= 14.5
    assert report.summary.total_distance_km > 0
    assert report.summary.bbox is not None
    assert report.summary.time_range is not None
    assert any(s.kind == "moving" for s in report.segments)
    assert any(s.kind == "stopped" for s in report.segments)


def test_compute_insights_empty_input():
    req = InsightsRequest(points=[])
    report = compute_insights(req)
    assert report.summary.point_count == 0
    assert report.summary.accepted_point_count == 0
    assert report.stops == []
    assert report.segments == []


def test_compute_insights_single_point():
    req = InsightsRequest(
        points=[PointIn(lat=12.97, lon=77.59, ts_utc=datetime(2025, 1, 1, tzinfo=timezone.utc))]
    )
    report = compute_insights(req)
    assert report.summary.total_distance_km == 0.0
    assert report.summary.bbox is not None


def test_options_defaults_general_audience():
    opts = InsightsOptions()
    assert opts.stop_radius_m == 50.0
    assert opts.min_stop_minutes == 5.0
