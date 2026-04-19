"""Unit tests for :mod:`bhulan.analytics.trips`."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

import pytest

from bhulan.analytics.mobility import TrackSample
from bhulan.analytics.stops import Stop
from bhulan.analytics.trips import (
    DEFAULT_TRIP_SPLIT_GAP_S,
    DEFAULT_TRIP_SPLIT_STOP_S,
    detect_trips,
)


def _sample(lat: float, lon: float, ts_min: float) -> TrackSample:
    return TrackSample(
        lat=lat,
        lon=lon,
        ts_utc=datetime(2025, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
        + timedelta(minutes=ts_min),
    )


def _straight_line(n: int, start_min: float = 0.0, step_min: float = 1.0) -> List[TrackSample]:
    """n samples stepping north ~111 m every ``step_min`` minutes."""
    out: List[TrackSample] = []
    for i in range(n):
        lat = 12.97 + i * 0.001
        out.append(_sample(lat, 77.59, start_min + i * step_min))
    return out


def test_empty_input_returns_empty_list():
    assert detect_trips([], stops=[]) == []


def test_single_trip_no_stops_no_gaps():
    track = _straight_line(8)
    trips = detect_trips(track, stops=[])
    assert len(trips) == 1
    t = trips[0]
    assert t.start_index == 0
    assert t.end_index == 7
    assert t.sample_count == 8
    assert t.distance_m > 0
    assert t.duration_s == pytest.approx(7 * 60.0)
    # Every step covers ~111 m in 60 s → ~1.85 m/s, well above the 1 m/s
    # moving threshold, so the whole trip counts as moving.
    assert t.moving_s == pytest.approx(7 * 60.0)
    assert t.idle_s == 0.0


def test_gap_splits_into_two_trips():
    first = _straight_line(4)
    # 90-min jump between trips — longer than the default 60-min gap split.
    second = _straight_line(4, start_min=4 + 90)
    trips = detect_trips(first + second, stops=[])
    assert len(trips) == 2
    assert trips[0].end_index == 3
    assert trips[1].start_index == 4
    # The big gap does NOT count as trip duration — trip durations end at
    # the last sample of that trip.
    assert trips[0].duration_s == pytest.approx(3 * 60.0)
    assert trips[1].duration_s == pytest.approx(3 * 60.0)


def test_long_stop_splits_trip():
    # 4 moving samples, then a 40-min stop, then 4 more. The stop itself
    # is part of the track (samples 4..7) — trip-splitting should put the
    # stop's samples with the first trip and kick off trip 2 at sample 8.
    track = _straight_line(4) + [
        _sample(12.974, 77.59, 4.0 + i) for i in range(40)
    ] + _straight_line(4, start_min=4.0 + 40)
    # Fabricate a stop covering indices 4..43 (40 min duration).
    stop = Stop(
        lat=12.974,
        lon=77.59,
        start_ts=track[4].ts_utc,  # type: ignore[arg-type]
        end_ts=track[43].ts_utc,  # type: ignore[arg-type]
        duration_s=39 * 60.0,
        radius_m=5.0,
        start_index=4,
        end_index=43,
        sample_count=40,
    )
    trips = detect_trips(
        track,
        stops=[stop],
        trip_split_stop_seconds=30 * 60.0,
    )
    assert len(trips) == 2
    assert trips[0].start_index == 0
    assert trips[0].end_index == 43
    assert trips[1].start_index == 44
    assert trips[1].end_index == len(track) - 1


def test_short_stop_does_not_split():
    # A 10-min stop should stay inside a single trip when the split
    # threshold is 30 min.
    track = _straight_line(4) + [
        _sample(12.974, 77.59, 4.0 + i) for i in range(10)
    ] + _straight_line(4, start_min=4.0 + 10)
    stop = Stop(
        lat=12.974,
        lon=77.59,
        start_ts=track[4].ts_utc,  # type: ignore[arg-type]
        end_ts=track[13].ts_utc,  # type: ignore[arg-type]
        duration_s=9 * 60.0,
        radius_m=5.0,
        start_index=4,
        end_index=13,
        sample_count=10,
    )
    trips = detect_trips(track, stops=[stop], trip_split_stop_seconds=30 * 60.0)
    assert len(trips) == 1
    assert trips[0].sample_count == len(track)
    # The stop samples are idle time inside the trip.
    assert trips[0].idle_s > 0
    assert trips[0].moving_s > 0


def test_start_and_end_coordinates_match_endpoints():
    track = _straight_line(5)
    trips = detect_trips(track, stops=[])
    t = trips[0]
    assert t.start_lat == pytest.approx(track[0].lat)
    assert t.start_lon == pytest.approx(track[0].lon)
    assert t.end_lat == pytest.approx(track[-1].lat)
    assert t.end_lon == pytest.approx(track[-1].lon)


def test_timestampless_track_yields_one_trip_with_zero_duration():
    points = [TrackSample(lat=12.97 + i * 0.001, lon=77.59, ts_utc=None) for i in range(5)]
    trips = detect_trips(points, stops=[])
    assert len(trips) == 1
    assert trips[0].duration_s == 0.0
    assert trips[0].moving_s == 0.0
    # distance is still computed because haversine doesn't need timestamps
    assert trips[0].distance_m > 0


def test_defaults_are_reasonable():
    # 30 min / 60 min. Small sanity check that the exported constants
    # haven't drifted from the general-audience UX choice.
    assert DEFAULT_TRIP_SPLIT_STOP_S == 30 * 60.0
    assert DEFAULT_TRIP_SPLIT_GAP_S == 60 * 60.0


def test_single_sample_trip_reports_device_speed():
    # Regression: per-trip _segment_kmh_window only inspected points[i+1]'s
    # device-reported speed, so a 1-sample trip (loop body never runs)
    # always reported max_speed_mps=0 even when the sample carried a
    # non-zero device speed. Build a 2-trip track where a 90-min gap
    # isolates a single fast sample as its own trip.
    base = _straight_line(3)
    # Lone fast sample 120 min later with a device speed of 40 m/s.
    isolated = TrackSample(
        lat=12.98, lon=77.59,
        ts_utc=datetime(2025, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
        + timedelta(minutes=120),
        speed_mps=40.0,
    )
    trips = detect_trips(base + [isolated], stops=[])
    assert len(trips) == 2
    # The second trip has exactly one sample — its max speed must come
    # from that sample's device-reported speed, not be 0.
    assert trips[1].sample_count == 1
    assert trips[1].max_speed_mps == pytest.approx(40.0)


def test_first_sample_device_speed_counts_in_multi_sample_trip():
    # Multi-sample variant of the same bug: points[start].speed_mps was
    # never checked, so if only the first sample carried a high device
    # speed, it would be ignored.
    start = TrackSample(
        lat=12.97, lon=77.59,
        ts_utc=datetime(2025, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        speed_mps=50.0,
    )
    rest = _straight_line(4, start_min=1.0)
    trips = detect_trips([start] + rest, stops=[])
    assert len(trips) == 1
    # Per-step derived speed across the rest is ~1.85 m/s. If the first
    # sample's device speed were ignored, max would be ~1.85 not 50.
    assert trips[0].max_speed_mps == pytest.approx(50.0)
