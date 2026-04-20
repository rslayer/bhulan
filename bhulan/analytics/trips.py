"""
Trip segmentation over a time-ordered GPS track.

A "trip" is a contiguous sub-track between two extended stops (or between
a data gap and the start/end of the track). The segmentation deliberately
reuses the already-computed ``stops`` list — anything long enough to count
as a *stop* is a natural trip boundary, and we don't want to run a second
clustering pass.

Split heuristics (both configurable):

* a stop whose duration exceeds ``trip_split_stop_seconds`` ends the
  current trip and begins a new one after the stop. Short visits
  (typical traffic lights / 5-min coffee stops) stay inside a trip.
* a gap between consecutive samples longer than
  ``trip_split_gap_seconds`` ends the current trip — either the device
  was off or the data is missing.

The algorithm is O(n + s) where n is the number of samples and s the
number of stops; it does not touch the KD-tree / projection code used by
:mod:`bhulan.analytics.stops`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Sequence

from bhulan.analytics.geodesy import haversine_vec_m
from bhulan.analytics.mobility import Segment, TrackSample
from bhulan.analytics.stops import Stop

# General-audience defaults. A 30-min stop is long enough that "the user
# did something else" (lunch, meeting) but short enough that we don't
# lump an entire morning of errands into one trip.
DEFAULT_TRIP_SPLIT_STOP_S = 30 * 60.0
# 60-min data gap is almost always a signal gap or device-off, not just
# slow GPS. Lower than an hour starts splitting mid-ride on tunnels.
DEFAULT_TRIP_SPLIT_GAP_S = 60 * 60.0

# Per-step instantaneous speed threshold, in m/s: anything at or above
# this counts as "moving" for the per-trip idle/moving split. ~3.6 km/h,
# so leisurely walking still registers as moving.
_MOVING_THRESHOLD_MPS = 1.0


@dataclass(frozen=True)
class Trip:
    """A detected trip with start/end info and derived mobility stats."""

    start_index: int
    end_index: int  # inclusive
    start_ts: Optional[datetime]
    end_ts: Optional[datetime]
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    distance_m: float
    duration_s: float
    moving_s: float
    idle_s: float
    max_speed_mps: float
    sample_count: int


def _trip_bounds(
    points: Sequence[TrackSample],
    stops: Sequence[Stop],
    trip_split_stop_s: float,
    trip_split_gap_s: float,
) -> List[int]:
    """Return the ordered list of sample indices that begin a new trip."""
    n = len(points)
    if n == 0:
        return []

    # Use a set so we can union multiple split rules without duplicating
    # boundaries. Convert back to a sorted list at the end.
    starts: set[int] = {0}

    # Gap-based splits
    for i in range(1, n):
        a = points[i - 1].ts_utc
        b = points[i].ts_utc
        if a is not None and b is not None:
            if (b - a).total_seconds() >= trip_split_gap_s:
                starts.add(i)

    # Stop-based splits — any long stop ends the current trip, and the
    # next trip begins at the first sample AFTER the stop's last sample.
    for stop in stops:
        if stop.duration_s < trip_split_stop_s:
            continue
        next_start = stop.end_index + 1
        if 0 < next_start < n:
            starts.add(next_start)

    return sorted(starts)


def _segment_kmh_window(
    points: Sequence[TrackSample],
    step_dist: List[float],
    start: int,
    end: int,
) -> tuple[float, float, float, float]:
    """Return ``(distance_m, duration_s, moving_s, max_speed_mps)`` over
    ``points[start..end]``. Uses ``step_dist[i]`` = distance from
    ``points[i]`` to ``points[i + 1]``.

    ``moving_s`` is the sum of inter-sample durations whose instantaneous
    speed exceeds :data:`MOVING_SPEED_MPS`. We intentionally don't reuse
    :func:`bhulan.analytics.mobility.segment_by_motion` here because
    per-trip stats look awkward when a trip happens to be labelled as
    one long "stopped" segment (e.g. the driver sits at a light at the
    start of the trip).
    """
    dist = 0.0
    duration = 0.0
    moving = 0.0
    max_v = 0.0

    for i in range(start, end):
        d = step_dist[i]
        dist += d
        a = points[i].ts_utc
        b = points[i + 1].ts_utc
        if a is None or b is None:
            continue
        dt = (b - a).total_seconds()
        if dt <= 0:
            continue
        duration += dt
        v = d / dt
        if v >= _MOVING_THRESHOLD_MPS:
            moving += dt
        # Device-reported speed wins over derived speed when available.
        reported = points[i + 1].speed_mps
        if reported is not None and reported > max_v:
            max_v = float(reported)
        if v > max_v:
            max_v = v

    # Also check the first sample's device-reported speed — the loop
    # above only inspects ``points[i + 1]``, so ``points[start]`` is
    # never visited. For single-sample trips (start == end) the loop
    # body doesn't run at all, making this the only speed source.
    first_reported = points[start].speed_mps
    if first_reported is not None and first_reported > max_v:
        max_v = float(first_reported)

    return dist, duration, moving, max_v


def detect_trips(
    points: Sequence[TrackSample],
    stops: Sequence[Stop] = (),
    trip_split_stop_seconds: float = DEFAULT_TRIP_SPLIT_STOP_S,
    trip_split_gap_seconds: float = DEFAULT_TRIP_SPLIT_GAP_S,
) -> List[Trip]:
    """Segment ``points`` into trips.

    The first point of every returned trip is either index 0, an index
    immediately following a long stop, or the first sample after a big
    time gap. Trips with zero movement (e.g. the tail of a track where
    the device is stationary but never long enough to trigger a stop)
    are still returned — a client that wants to filter them out can do
    so from the distance/duration fields.
    """
    n = len(points)
    if n == 0:
        return []

    starts = _trip_bounds(points, stops, trip_split_stop_seconds, trip_split_gap_seconds)

    # Pre-compute pairwise distances once; the trip loop slices into this.
    step_dist_np = haversine_vec_m([p.lat for p in points], [p.lon for p in points])
    step_dist: List[float] = [float(d) for d in step_dist_np]

    trips: List[Trip] = []
    for si, start in enumerate(starts):
        end = starts[si + 1] - 1 if si + 1 < len(starts) else n - 1
        if end < start:
            # Shouldn't happen, but guard against a boundary set that
            # tried to open a new trip at the very last index.
            continue
        dist, duration, moving, max_v = _segment_kmh_window(points, step_dist, start, end)
        trips.append(
            Trip(
                start_index=start,
                end_index=end,
                start_ts=points[start].ts_utc,
                end_ts=points[end].ts_utc,
                start_lat=points[start].lat,
                start_lon=points[start].lon,
                end_lat=points[end].lat,
                end_lon=points[end].lon,
                distance_m=dist,
                duration_s=duration,
                moving_s=moving,
                idle_s=max(duration - moving, 0.0),
                max_speed_mps=max_v,
                sample_count=end - start + 1,
            )
        )
    return trips


__all__ = [
    "DEFAULT_TRIP_SPLIT_STOP_S",
    "DEFAULT_TRIP_SPLIT_GAP_S",
    "Trip",
    "detect_trips",
    # re-export so call sites don't have to reach into two modules
    "Segment",
]
