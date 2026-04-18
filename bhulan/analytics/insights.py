"""
Top-level insights pipeline.

Takes a list of raw GPS points and returns an :class:`InsightsReport` with
summary metrics, detected stops, and motion segments. Pure Python, no I/O.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Tuple

from pydantic import BaseModel, Field, field_validator

from bhulan.analytics import mobility
from bhulan.analytics.mobility import Segment, TrackSample, prepare_track
from bhulan.analytics.stops import (
    DEFAULT_MIN_DURATION_S,
    DEFAULT_RADIUS_M,
    Stop,
    detect_stops,
    merge_nearby_stops,
)

MS_TO_KMH = 3.6
MAX_POINTS = 100_000


class PointIn(BaseModel):
    """One GPS sample as it arrives over the wire."""

    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    ts_utc: Optional[datetime] = Field(
        None, description="Timestamp in UTC; required for any time-based insight"
    )
    speed_mps: Optional[float] = Field(None, ge=0, description="Optional device-reported speed")


class InsightsOptions(BaseModel):
    """Tunables for the insights pipeline."""

    stop_radius_m: float = Field(DEFAULT_RADIUS_M, gt=0, le=10_000)
    min_stop_minutes: float = Field(
        DEFAULT_MIN_DURATION_S / 60.0, gt=0, le=24 * 60, description="Minutes"
    )
    moving_speed_kmh: float = Field(3.6, ge=0, le=300, description="Speed threshold for 'moving'")
    merge_stops_within_m: Optional[float] = Field(
        None,
        ge=0,
        le=10_000,
        description="If set, merge consecutive stops whose centroids are within this radius",
    )


class InsightsRequest(BaseModel):
    """Request body for :func:`compute_insights`."""

    points: List[PointIn] = Field(..., description="Raw GPS samples")
    options: InsightsOptions = Field(default_factory=InsightsOptions)

    @field_validator("points")
    @classmethod
    def _cap_length(cls, v: List[PointIn]) -> List[PointIn]:
        if len(v) > MAX_POINTS:
            raise ValueError(f"Too many points: {len(v)} > {MAX_POINTS}")
        return v


class BBox(BaseModel):
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float


class TimeRange(BaseModel):
    start: datetime
    end: datetime


class InsightsSummary(BaseModel):
    point_count: int
    accepted_point_count: int
    time_range: Optional[TimeRange]
    total_distance_km: float
    moving_time_min: float
    idle_time_min: float
    avg_moving_speed_kmh: float
    max_speed_kmh: float
    bbox: Optional[BBox]


class StopOut(BaseModel):
    lat: float
    lon: float
    start_ts: datetime
    end_ts: datetime
    duration_min: float
    radius_m: float
    sample_count: int


class SegmentOut(BaseModel):
    kind: str
    start_ts: Optional[datetime]
    end_ts: Optional[datetime]
    distance_km: float
    duration_min: float
    avg_speed_kmh: float


class InsightsQuality(BaseModel):
    rejected_points: int = 0
    issues: List[str] = Field(default_factory=list)


class InsightsReport(BaseModel):
    summary: InsightsSummary
    stops: List[StopOut]
    segments: List[SegmentOut]
    quality: InsightsQuality


def _segment_to_out(seg: Segment) -> SegmentOut:
    dist_km = seg.distance_m / 1000.0
    dur_min = seg.duration_s / 60.0
    avg_kmh = (seg.distance_m / seg.duration_s * MS_TO_KMH) if seg.duration_s > 0 else 0.0
    return SegmentOut(
        kind=seg.kind,
        start_ts=seg.start_ts,
        end_ts=seg.end_ts,
        distance_km=round(dist_km, 4),
        duration_min=round(dur_min, 3),
        avg_speed_kmh=round(avg_kmh, 3),
    )


def _stop_to_out(s: Stop) -> StopOut:
    return StopOut(
        lat=s.lat,
        lon=s.lon,
        start_ts=s.start_ts,
        end_ts=s.end_ts,
        duration_min=round(s.duration_s / 60.0, 3),
        radius_m=round(s.radius_m, 2),
        sample_count=s.sample_count,
    )


def _split_points(points: List[PointIn]) -> Tuple[List[TrackSample], InsightsQuality]:
    quality = InsightsQuality()
    samples: List[TrackSample] = []
    for i, p in enumerate(points):
        try:
            samples.append(
                TrackSample(
                    lat=float(p.lat),
                    lon=float(p.lon),
                    ts_utc=p.ts_utc,
                    speed_mps=p.speed_mps,
                )
            )
        except (TypeError, ValueError) as e:  # pragma: no cover - pydantic usually catches earlier
            quality.rejected_points += 1
            quality.issues.append(f"point[{i}]: {e}")
    return samples, quality


def compute_insights(request: InsightsRequest) -> InsightsReport:
    """Compute an :class:`InsightsReport` for the given request."""
    samples, quality = _split_points(request.points)
    opts = request.options

    prepared = prepare_track(samples)
    dropped = len(samples) - len(prepared)
    if dropped > 0:
        quality.rejected_points += dropped
        quality.issues.append(f"Removed {dropped} duplicate points")

    if not prepared:
        return InsightsReport(
            summary=InsightsSummary(
                point_count=len(request.points),
                accepted_point_count=0,
                time_range=None,
                total_distance_km=0.0,
                moving_time_min=0.0,
                idle_time_min=0.0,
                avg_moving_speed_kmh=0.0,
                max_speed_kmh=0.0,
                bbox=None,
            ),
            stops=[],
            segments=[],
            quality=quality,
        )

    moving_threshold_mps = opts.moving_speed_kmh / MS_TO_KMH
    segments = mobility.segment_by_motion(prepared, moving_speed_mps=moving_threshold_mps)
    total_m = mobility.total_distance_m(prepared)
    avg_mps, max_mps = mobility.speed_stats_mps(prepared, segments)
    start_ts, end_ts = mobility.time_range(prepared)
    box = mobility.bbox(prepared)

    raw_stops = detect_stops(
        prepared,
        radius_m=opts.stop_radius_m,
        min_duration_s=opts.min_stop_minutes * 60.0,
    )
    stops = merge_nearby_stops(raw_stops, merge_radius_m=opts.merge_stops_within_m)

    moving_secs = sum(s.duration_s for s in segments if s.kind == "moving")
    idle_secs = sum(s.duration_s for s in segments if s.kind == "stopped")

    summary = InsightsSummary(
        point_count=len(request.points),
        accepted_point_count=len(prepared),
        time_range=TimeRange(start=start_ts, end=end_ts) if start_ts and end_ts else None,
        total_distance_km=round(total_m / 1000.0, 4),
        moving_time_min=round(moving_secs / 60.0, 3),
        idle_time_min=round(idle_secs / 60.0, 3),
        avg_moving_speed_kmh=round(avg_mps * MS_TO_KMH, 3),
        max_speed_kmh=round(max_mps * MS_TO_KMH, 3),
        bbox=BBox(
            min_lat=box[0],
            min_lon=box[1],
            max_lat=box[2],
            max_lon=box[3],
        )
        if box
        else None,
    )

    return InsightsReport(
        summary=summary,
        stops=[_stop_to_out(s) for s in stops],
        segments=[_segment_to_out(s) for s in segments],
        quality=quality,
    )
