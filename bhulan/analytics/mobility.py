"""
Mobility metrics over a time-ordered GPS track.

All functions operate on a simple list of ``TrackSample`` rows (lat, lon,
timestamp, optional speed). The track is assumed to already be sorted by
timestamp; :func:`prepare_track` handles sorting and duplicate removal.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Sequence, Tuple

import numpy as np

from bhulan.analytics.geodesy import bounding_box, haversine_vec_m


@dataclass(frozen=True)
class TrackSample:
    """A single parsed GPS point used by the analytics pipeline."""

    lat: float
    lon: float
    ts_utc: Optional[datetime]
    speed_mps: Optional[float] = None


@dataclass(frozen=True)
class Segment:
    """A contiguous stretch of the track labelled as moving or stopped."""

    kind: str  # "moving" or "stopped"
    start_index: int
    end_index: int  # inclusive
    start_ts: Optional[datetime]
    end_ts: Optional[datetime]
    distance_m: float
    duration_s: float


def _to_utc(ts: Optional[datetime]) -> Optional[datetime]:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    try:
        return ts.astimezone(timezone.utc)
    except (OverflowError, OSError):
        # Timestamps at the very edge of ``datetime``'s representable range
        # (year 1 / year 9999) overflow when the offset is shifted to UTC —
        # e.g. ``0001-01-01T00:00:00+14:00`` would underflow past year 1.
        # These are implausible for GPS data; reinterpret the wall-clock time
        # as UTC rather than crashing the whole request with a 500.
        return ts.replace(tzinfo=timezone.utc)


def prepare_track(points: Sequence[TrackSample]) -> List[TrackSample]:
    """
    Return a cleaned, time-ordered copy of ``points``.

    Points with timestamps are sorted by timestamp; points without timestamps
    keep their relative order after timestamped ones. Exact duplicates
    (same lat/lon/ts) are removed.
    """
    with_ts = [
        TrackSample(p.lat, p.lon, _to_utc(p.ts_utc), p.speed_mps)
        for p in points
        if p.ts_utc is not None
    ]
    without_ts = [p for p in points if p.ts_utc is None]

    with_ts.sort(key=lambda p: p.ts_utc)  # type: ignore[arg-type, return-value]

    seen = set()
    deduped: List[TrackSample] = []
    for p in with_ts + without_ts:
        key = (round(p.lat, 7), round(p.lon, 7), p.ts_utc.isoformat() if p.ts_utc else None)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)
    return deduped


def total_distance_m(points: Sequence[TrackSample]) -> float:
    """Sum of consecutive-point great-circle distances, in meters."""
    if len(points) < 2:
        return 0.0
    lats = [p.lat for p in points]
    lons = [p.lon for p in points]
    return float(np.sum(haversine_vec_m(lats, lons)))


def time_range(points: Sequence[TrackSample]) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Earliest and latest timestamp present in the track, or ``(None, None)``."""
    stamps = [p.ts_utc for p in points if p.ts_utc is not None]
    if not stamps:
        return None, None
    return min(stamps), max(stamps)


def bbox(points: Sequence[TrackSample]) -> Optional[Tuple[float, float, float, float]]:
    """Return ``(min_lat, min_lon, max_lat, max_lon)`` or ``None`` if empty."""
    if not points:
        return None
    return bounding_box([p.lat for p in points], [p.lon for p in points])


def segment_by_motion(
    points: Sequence[TrackSample],
    moving_speed_mps: float = 1.0,
    min_segment_s: float = 30.0,
) -> List[Segment]:
    """
    Classify the track into alternating moving / stopped segments.

    A sample is considered "moving" if its instantaneous speed (derived from
    the distance/time to the next sample, or the provided ``speed_mps``) is
    at least ``moving_speed_mps``. Short segments below ``min_segment_s`` are
    merged into their neighbour to avoid chattering labels.

    If the track has no timestamps, a single "moving" segment covering the
    whole track is returned as a best-effort fallback.
    """
    n = len(points)
    if n == 0:
        return []

    has_ts = all(p.ts_utc is not None for p in points)
    if not has_ts or n < 2:
        dist = total_distance_m(points)
        return [
            Segment(
                kind="moving",
                start_index=0,
                end_index=n - 1,
                start_ts=points[0].ts_utc,
                end_ts=points[-1].ts_utc,
                distance_m=dist,
                duration_s=0.0,
            )
        ]

    lats = [p.lat for p in points]
    lons = [p.lon for p in points]
    step_dist = haversine_vec_m(lats, lons)
    step_secs = np.array(
        [
            max((points[i + 1].ts_utc - points[i].ts_utc).total_seconds(), 0.0)  # type: ignore[operator]
            for i in range(n - 1)
        ],
        dtype=np.float64,
    )
    with np.errstate(divide="ignore", invalid="ignore"):
        step_speed = np.where(step_secs > 0, step_dist / step_secs, 0.0)

    sample_speed = np.zeros(n, dtype=np.float64)
    sample_speed[:-1] = step_speed
    sample_speed[-1] = step_speed[-1] if step_speed.size else 0.0
    for i, p in enumerate(points):
        if p.speed_mps is not None:
            sample_speed[i] = float(p.speed_mps)

    moving_mask = sample_speed >= moving_speed_mps

    raw: List[Tuple[str, int, int]] = []
    start = 0
    for i in range(1, n):
        if moving_mask[i] != moving_mask[start]:
            raw.append(("moving" if moving_mask[start] else "stopped", start, i - 1))
            start = i
    raw.append(("moving" if moving_mask[start] else "stopped", start, n - 1))

    def _seg_duration(a: int, b: int) -> float:
        return (points[b].ts_utc - points[a].ts_utc).total_seconds()  # type: ignore[operator]

    merged: List[Tuple[str, int, int]] = []
    for seg in raw:
        kind, a, b = seg
        if merged and _seg_duration(a, b) < min_segment_s and a > 0 and b < n - 1:
            prev_kind, pa, _ = merged[-1]
            merged[-1] = (prev_kind, pa, b)
        else:
            merged.append(seg)

    collapsed: List[Tuple[str, int, int]] = []
    for seg in merged:
        if collapsed and collapsed[-1][0] == seg[0]:
            collapsed[-1] = (seg[0], collapsed[-1][1], seg[2])
        else:
            collapsed.append(seg)

    result: List[Segment] = []
    for kind, a, b in collapsed:
        dist = float(np.sum(step_dist[a:b])) if b > a else 0.0
        dur = _seg_duration(a, b)
        result.append(
            Segment(
                kind=kind,
                start_index=a,
                end_index=b,
                start_ts=points[a].ts_utc,
                end_ts=points[b].ts_utc,
                distance_m=dist,
                duration_s=dur,
            )
        )
    return result


def speed_stats_mps(
    points: Sequence[TrackSample], segments: Optional[Sequence[Segment]] = None
) -> Tuple[float, float]:
    """
    Return ``(average_moving_speed_mps, max_speed_mps)``.

    Average is computed as total moving distance divided by total moving time
    so short stops don't drag it down. Max is taken over per-sample speeds.
    """
    n = len(points)
    if n == 0:
        return 0.0, 0.0

    if segments is None:
        segments = segment_by_motion(points)

    moving_dist = sum(s.distance_m for s in segments if s.kind == "moving")
    moving_secs = sum(s.duration_s for s in segments if s.kind == "moving")
    avg = moving_dist / moving_secs if moving_secs > 0 else 0.0

    max_speed = 0.0
    for p in points:
        if p.speed_mps is not None and p.speed_mps > max_speed:
            max_speed = float(p.speed_mps)

    if n >= 2:
        lats = [p.lat for p in points]
        lons = [p.lon for p in points]
        step_dist = haversine_vec_m(lats, lons)
        step_secs = [
            (points[i + 1].ts_utc - points[i].ts_utc).total_seconds()  # type: ignore[operator]
            if points[i + 1].ts_utc and points[i].ts_utc
            else 0.0
            for i in range(n - 1)
        ]
        for d, s in zip(step_dist, step_secs):
            if s > 0:
                v = d / s
                if v > max_speed:
                    max_speed = float(v)

    return avg, max_speed
