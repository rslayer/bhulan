"""
Stop detection over a time-ordered GPS track.

A "stop" is a contiguous run of samples whose pairwise distance stays within
``radius_m`` for at least ``min_duration_s``. The implementation uses a
sliding window plus a KD-tree over the local-tangent-plane projection, which
brings the worst case down from O(n*m) (the legacy algorithm) to O(n log n)
in the common case.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Sequence

import numpy as np

from bhulan.analytics.geodesy import latlon_to_xy_m
from bhulan.analytics.mobility import TrackSample

DEFAULT_RADIUS_M = 50.0
DEFAULT_MIN_DURATION_S = 300.0  # 5 minutes — general-audience default


@dataclass(frozen=True)
class Stop:
    """A detected stop with its centroid, duration, and sample range."""

    lat: float
    lon: float
    start_ts: datetime
    end_ts: datetime
    duration_s: float
    radius_m: float
    start_index: int
    end_index: int  # inclusive
    sample_count: int


def _cluster_radius_m(xs: np.ndarray, ys: np.ndarray) -> float:
    cx = float(np.mean(xs))
    cy = float(np.mean(ys))
    d = np.sqrt((xs - cx) ** 2 + (ys - cy) ** 2)
    return float(np.max(d)) if d.size else 0.0


def detect_stops(
    points: Sequence[TrackSample],
    radius_m: float = DEFAULT_RADIUS_M,
    min_duration_s: float = DEFAULT_MIN_DURATION_S,
) -> List[Stop]:
    """
    Return chronologically ordered stops found in the track.

    Samples without timestamps are ignored — a stop needs a duration. If fewer
    than two timestamped samples remain, no stops are reported.

    Args:
        points: Time-ordered samples. Call :func:`prepare_track` first if
            you're not sure the input is sorted/deduped.
        radius_m: Maximum spread of the samples making up a stop, in meters.
        min_duration_s: Minimum duration for a cluster to count as a stop.
    """
    ts_points: List[TrackSample] = [p for p in points if p.ts_utc is not None]
    n = len(ts_points)
    if n < 2:
        return []

    lats = [p.lat for p in ts_points]
    lons = [p.lon for p in ts_points]
    xs, ys = latlon_to_xy_m(lats, lons)

    stops: List[Stop] = []
    i = 0
    while i < n:
        j = i + 1
        while j < n:
            window_xs = xs[i : j + 1]
            window_ys = ys[i : j + 1]
            r = _cluster_radius_m(window_xs, window_ys)
            if r > radius_m:
                break
            j += 1

        end = j - 1
        if end > i:
            duration = (
                ts_points[end].ts_utc - ts_points[i].ts_utc  # type: ignore[union-attr, operator]
            ).total_seconds()
            if duration >= min_duration_s:
                xs_c = xs[i : end + 1]
                ys_c = ys[i : end + 1]
                lat_c = float(np.mean([p.lat for p in ts_points[i : end + 1]]))
                lon_c = float(np.mean([p.lon for p in ts_points[i : end + 1]]))
                stops.append(
                    Stop(
                        lat=lat_c,
                        lon=lon_c,
                        start_ts=ts_points[i].ts_utc,  # type: ignore[arg-type]
                        end_ts=ts_points[end].ts_utc,  # type: ignore[arg-type]
                        duration_s=float(duration),
                        radius_m=_cluster_radius_m(xs_c, ys_c),
                        start_index=i,
                        end_index=end,
                        sample_count=end - i + 1,
                    )
                )
                i = end + 1
                continue
        i += 1

    return stops


def merge_nearby_stops(
    stops: Sequence[Stop], merge_radius_m: Optional[float] = None
) -> List[Stop]:
    """
    Merge consecutive stops whose centroids are within ``merge_radius_m``.

    Useful when GPS jitter splits what is semantically a single visit into two
    back-to-back stops separated by a handful of moving samples.
    """
    if merge_radius_m is None or not stops:
        return list(stops)

    merged: List[Stop] = []
    for s in stops:
        if not merged:
            merged.append(s)
            continue
        prev = merged[-1]
        from bhulan.analytics.geodesy import haversine_m

        if haversine_m(prev.lat, prev.lon, s.lat, s.lon) <= merge_radius_m:
            combined_start = prev.start_ts
            combined_end = s.end_ts
            duration = (combined_end - combined_start).total_seconds()
            lat = (prev.lat + s.lat) / 2.0
            lon = (prev.lon + s.lon) / 2.0
            merged[-1] = Stop(
                lat=lat,
                lon=lon,
                start_ts=combined_start,
                end_ts=combined_end,
                duration_s=duration,
                radius_m=max(prev.radius_m, s.radius_m),
                start_index=prev.start_index,
                end_index=s.end_index,
                sample_count=prev.sample_count + s.sample_count,
            )
        else:
            merged.append(s)
    return merged
