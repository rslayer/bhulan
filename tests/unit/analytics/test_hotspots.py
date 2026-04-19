"""Unit tests for :mod:`bhulan.analytics.hotspots`."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

import pytest

from bhulan.analytics.hotspots import (
    DEFAULT_HOTSPOT_GRID_M,
    DEFAULT_HOTSPOT_MAX_RESULTS,
    DEFAULT_HOTSPOT_MIN_SAMPLES,
    detect_hotspots,
)
from bhulan.analytics.mobility import TrackSample


def _p(lat: float, lon: float, ts_min: float = 0.0) -> TrackSample:
    return TrackSample(
        lat=lat,
        lon=lon,
        ts_utc=datetime(2025, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
        + timedelta(minutes=ts_min),
    )


def test_empty_input_returns_empty_list():
    assert detect_hotspots([]) == []


def test_sparse_track_has_no_hotspots():
    # Points are spaced ~110 m apart — one sample per cell, no cell ever
    # has min_samples so no hotspots.
    track = [_p(12.97 + i * 0.001, 77.59, i) for i in range(5)]
    assert detect_hotspots(track, min_samples=3) == []


def test_dense_cluster_becomes_one_hotspot():
    # 8 samples within a ~15 m square — the default 100 m grid will put
    # them all in the same cell.
    track: List[TrackSample] = []
    for i in range(8):
        track.append(_p(12.9720 + i * 0.00005, 77.5950 + i * 0.00005, ts_min=i))
    hs = detect_hotspots(track, min_samples=3)
    assert len(hs) == 1
    h = hs[0]
    assert h.sample_count == 8
    assert h.visit_count == 1  # all contiguous indices → one visit
    assert h.lat == pytest.approx(12.9720 + 3.5 * 0.00005, abs=1e-5)
    assert h.time_spent_s is not None
    # 7 inter-sample gaps of 60s each = 420s
    assert h.time_spent_s == pytest.approx(7 * 60.0)


def test_two_clusters_ranked_by_sample_count():
    # Cluster A: 6 samples at (12.972, 77.595). Cluster B: 10 samples far
    # away at (12.990, 77.610). Clusters sorted by sample count desc.
    track: List[TrackSample] = []
    for i in range(6):
        track.append(_p(12.9720, 77.5950, ts_min=i))
    for i in range(10):
        track.append(_p(12.9900, 77.6100, ts_min=20 + i))
    hs = detect_hotspots(track, min_samples=3)
    assert len(hs) == 2
    assert hs[0].sample_count == 10
    assert hs[1].sample_count == 6


def test_visit_count_counts_distinct_runs():
    # Visit cluster at (12.972, 77.595) three separate times, interleaved
    # with samples at a different, far-away cluster.
    track: List[TrackSample] = []
    t = 0
    for visit in range(3):
        for _ in range(5):
            track.append(_p(12.9720, 77.5950, ts_min=t))
            t += 1
        for _ in range(5):
            # 1 km away — different cell entirely.
            track.append(_p(12.9900, 77.6100, ts_min=t))
            t += 1
    hs = detect_hotspots(track, min_samples=3)
    # Both clusters detected; the first-visited (and first-listed) has
    # 3 visits.
    top = next(h for h in hs if abs(h.lat - 12.9720) < 1e-4)
    assert top.sample_count == 15
    assert top.visit_count == 3


def test_max_results_truncates():
    # Make 5 distinct clusters but cap at 2.
    track: List[TrackSample] = []
    for cluster_id in range(5):
        for i in range(5):
            track.append(
                _p(12.97 + cluster_id * 0.01, 77.59, ts_min=cluster_id * 10 + i)
            )
    hs = detect_hotspots(track, min_samples=3, max_results=2)
    assert len(hs) == 2


def test_timestampless_track_reports_none_time_spent():
    track = [
        TrackSample(lat=12.972, lon=77.595, ts_utc=None) for _ in range(6)
    ]
    hs = detect_hotspots(track, min_samples=3)
    assert len(hs) == 1
    assert hs[0].time_spent_s is None
    assert hs[0].first_ts is None
    assert hs[0].last_ts is None
    assert hs[0].sample_count == 6


def test_diagonally_adjacent_cells_are_one_cluster():
    # 5 samples in one cell + 5 samples diagonally across the boundary
    # into the neighbour — 8-neighbour adjacency should merge them into
    # a single cluster. Uses a 500 m grid so the cell edge is an easy
    # target to straddle without floating-point rounding getting in the
    # way.
    track: List[TrackSample] = []
    for i in range(5):
        # First cluster, tight bunch.
        track.append(_p(12.9700 + i * 0.00001, 77.5900 + i * 0.00001, ts_min=i))
    for i in range(5):
        # ~780 m NE (0.005° lat ≈ 557 m, 0.005° lon ≈ 542 m) — one diagonal
        # cell over at grid_m=500 m, still within 8-neighbour adjacency.
        track.append(_p(12.9750 + i * 0.00001, 77.5950 + i * 0.00001, ts_min=10 + i))
    hs = detect_hotspots(track, min_samples=3, grid_m=500.0)
    assert len(hs) == 1
    assert hs[0].sample_count == 10


def test_defaults_are_reasonable():
    assert DEFAULT_HOTSPOT_GRID_M == 100.0
    assert DEFAULT_HOTSPOT_MIN_SAMPLES == 5
    assert DEFAULT_HOTSPOT_MAX_RESULTS == 10
