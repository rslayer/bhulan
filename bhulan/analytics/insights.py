"""
Top-level insights pipeline.

Takes a list of raw GPS points and returns an :class:`InsightsReport` with
summary metrics, detected stops, and motion segments. Pure Python, no I/O.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import List, Optional, Tuple

from pydantic import BaseModel, Field, field_validator

from bhulan.analytics import mobility
from bhulan.analytics.hotspots import (
    DEFAULT_HOTSPOT_GRID_M,
    DEFAULT_HOTSPOT_MAX_RESULTS,
    DEFAULT_HOTSPOT_MIN_SAMPLES,
    Hotspot,
    detect_hotspots,
)
from bhulan.analytics.mobility import Segment, TrackSample, prepare_track
from bhulan.analytics.stops import (
    DEFAULT_MIN_DURATION_S,
    DEFAULT_RADIUS_M,
    Stop,
    StopScanBudgetExceeded,
    _MAX_STOP_SCAN_WORK,
    detect_stops,
    merge_nearby_stops,
)
from bhulan.analytics.trips import (
    DEFAULT_TRIP_SPLIT_GAP_S,
    DEFAULT_TRIP_SPLIT_STOP_S,
    Trip,
    detect_trips,
)

MS_TO_KMH = 3.6
MAX_POINTS = 100_000

# Plausibility ceiling for device-reported speed. ``speed_mps`` is the only
# unbounded caller-supplied float that flows into the summary's ``max_speed_mps``
# and gets multiplied by ``MS_TO_KMH`` (see ``compute_insights`` / ``build_trip``).
# Left unbounded, an ``inf``/``NaN`` value (e.g. JSON ``1e400``) — or even a finite
# one large enough that ``value * 3.6`` overflows to ``inf`` (~5e307) — ends up as
# a non-finite float in a *successful* response, which Starlette's ``JSONResponse``
# (``allow_nan=False``) then refuses to serialize, surfacing as an unhandled 500.
# 1e7 m/s (36 million km/h) is orders of magnitude beyond any real GPS-tracked
# object, so anything above it is bad telemetry rather than a usable reading.
MAX_SPEED_MPS = 1e7


class PointIn(BaseModel):
    """One GPS sample as it arrives over the wire."""

    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    ts_utc: Optional[datetime] = Field(
        None, description="Timestamp in UTC; required for any time-based insight"
    )
    speed_mps: Optional[float] = Field(
        None,
        ge=0,
        description="Optional device-reported speed; implausible values are ignored",
    )

    @field_validator("speed_mps", mode="before")
    @classmethod
    def _drop_implausible_speed(cls, v: object) -> object:
        """Treat an implausible device-reported speed as missing.

        A non-finite (``inf``/``NaN``, e.g. from JSON ``1e400``) or absurdly
        large ``speed_mps`` is bad telemetry, not a usable reading — and if it
        reaches the response it serializes to a non-finite float and 500s the
        request (see ``MAX_SPEED_MPS``). Rather than rejecting the whole point
        over one bad field, drop the value: the analytics already derive speed
        from distance/time for points that carry no ``speed_mps`` at all.

        Runs ``mode="before"`` so the bad value never reaches the ``ge=0``
        constraint. Values that are merely *invalid* rather than implausible
        (e.g. a negative speed, a non-numeric string) are passed through
        untouched so they still produce the usual 422.
        """
        if v is None:
            return None
        try:
            as_float = float(v)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return v  # not a number — let normal validation report it
        if not math.isfinite(as_float) or as_float > MAX_SPEED_MPS:
            return None
        return v


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
    geocode_stops: bool = Field(
        False,
        description=(
            "If true, reverse-geocode each stop's centroid via Nominatim and "
            "populate StopOut.place_name. Respects Nominatim's 1 req/sec "
            "limit; expect +N seconds latency for N unique stops."
        ),
    )
    trip_split_stop_minutes: float = Field(
        DEFAULT_TRIP_SPLIT_STOP_S / 60.0,
        gt=0,
        le=24 * 60,
        description=(
            "Stops longer than this end the current trip and start a new one. "
            "Shorter stops stay inside the trip they happen during."
        ),
    )
    trip_split_gap_minutes: float = Field(
        DEFAULT_TRIP_SPLIT_GAP_S / 60.0,
        gt=0,
        le=7 * 24 * 60,
        description=(
            "A gap between consecutive samples longer than this ends the "
            "current trip (device-off or missing data)."
        ),
    )
    hotspot_grid_m: float = Field(
        DEFAULT_HOTSPOT_GRID_M,
        gt=0,
        le=10_000,
        description="Grid cell size (meters) used for hotspot clustering.",
    )
    hotspot_min_samples: int = Field(
        DEFAULT_HOTSPOT_MIN_SAMPLES,
        ge=1,
        le=10_000,
        description="Minimum samples per grid cell to qualify as a hotspot.",
    )
    hotspot_max_results: int = Field(
        DEFAULT_HOTSPOT_MAX_RESULTS,
        ge=1,
        le=100,
        description="Maximum number of hotspots returned, sorted by sample count desc.",
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
    place_name: Optional[str] = Field(
        None,
        description=(
            "Reverse-geocoded label for the centroid. Only populated when "
            "InsightsOptions.geocode_stops is true; otherwise None."
        ),
    )


class SegmentOut(BaseModel):
    kind: str
    start_ts: Optional[datetime]
    end_ts: Optional[datetime]
    distance_km: float
    duration_min: float
    avg_speed_kmh: float


class TripOut(BaseModel):
    """One detected trip with derived mobility stats.

    Timestamps are ``None`` when the underlying samples carried none;
    everything else is zero-safe so the UI can render without guarding.
    """

    index: int = Field(..., description="0-based index of the trip in start order")
    start_ts: Optional[datetime]
    end_ts: Optional[datetime]
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    distance_km: float
    duration_min: float
    moving_time_min: float
    idle_time_min: float
    max_speed_kmh: float
    sample_count: int


class HotspotOut(BaseModel):
    """One detected density cluster.

    ``time_spent_min`` is ``None`` when the input has no timestamps —
    we can still identify the hotspot geographically, we just can't
    say how long was spent there.
    """

    lat: float
    lon: float
    sample_count: int
    visit_count: int
    time_spent_min: Optional[float]
    first_ts: Optional[datetime]
    last_ts: Optional[datetime]
    place_name: Optional[str] = Field(
        None,
        description=(
            "Reverse-geocoded label for the centroid. Only populated when "
            "InsightsOptions.geocode_stops is true."
        ),
    )


class InsightsQuality(BaseModel):
    rejected_points: int = 0
    issues: List[str] = Field(default_factory=list)


class InsightsReport(BaseModel):
    summary: InsightsSummary
    stops: List[StopOut]
    segments: List[SegmentOut]
    trips: List[TripOut] = Field(default_factory=list)
    hotspots: List[HotspotOut] = Field(default_factory=list)
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


def _trip_to_out(index: int, t: Trip) -> TripOut:
    max_kmh = t.max_speed_mps * MS_TO_KMH
    return TripOut(
        index=index,
        start_ts=t.start_ts,
        end_ts=t.end_ts,
        start_lat=t.start_lat,
        start_lon=t.start_lon,
        end_lat=t.end_lat,
        end_lon=t.end_lon,
        distance_km=round(t.distance_m / 1000.0, 4),
        duration_min=round(t.duration_s / 60.0, 3),
        moving_time_min=round(t.moving_s / 60.0, 3),
        idle_time_min=round(t.idle_s / 60.0, 3),
        max_speed_kmh=round(max_kmh, 3),
        sample_count=t.sample_count,
    )


def _hotspot_to_out(h: Hotspot) -> HotspotOut:
    return HotspotOut(
        lat=h.lat,
        lon=h.lon,
        sample_count=h.sample_count,
        visit_count=h.visit_count,
        time_spent_min=(
            round(h.time_spent_s / 60.0, 3) if h.time_spent_s is not None else None
        ),
        first_ts=h.first_ts,
        last_ts=h.last_ts,
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


async def compute_insights_with_geocoding(
    request: InsightsRequest,
) -> InsightsReport:
    """Compute insights and, if requested, reverse-geocode the stops.

    HTTP handlers use this async helper instead of :func:`compute_insights`
    when the caller opts into geocoding. Keeps :func:`compute_insights`
    synchronous and pure — geocoding is a network-bound side effect that
    doesn't belong in the analytics kernel.
    """
    report = compute_insights(request)
    if not request.options.geocode_stops:
        return report
    # A track can legitimately produce hotspots but no stops (dense but
    # always-moving samples, e.g. commute corridors). Bail only when
    # there's nothing to geocode on either side.
    if not report.stops and not report.hotspots:
        return report

    # Local import to avoid httpx cost when geocoding is off.
    from bhulan.analytics.geocoding import reverse_geocode_stops

    # Geocode stops AND hotspots in a single batched call so the
    # Nominatim throttle (~1 req/sec) amortizes across both. The cache
    # inside ``reverse_geocode_stops`` dedupes coordinate repeats at
    # ~11 m precision, so if a stop and a hotspot share a centroid the
    # second lookup is free.
    stop_coords = [(s.lat, s.lon) for s in report.stops]
    hotspot_coords = [(h.lat, h.lon) for h in report.hotspots]
    all_names = await reverse_geocode_stops(stop_coords + hotspot_coords)
    stop_names = all_names[: len(stop_coords)]
    hotspot_names = all_names[len(stop_coords) :]
    enriched_stops = [
        s.model_copy(update={"place_name": name})
        for s, name in zip(report.stops, stop_names)
    ]
    enriched_hotspots = [
        h.model_copy(update={"place_name": name})
        for h, name in zip(report.hotspots, hotspot_names)
    ]
    return report.model_copy(
        update={"stops": enriched_stops, "hotspots": enriched_hotspots}
    )


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
            trips=[],
            hotspots=[],
            quality=quality,
        )

    moving_threshold_mps = opts.moving_speed_kmh / MS_TO_KMH
    segments = mobility.segment_by_motion(prepared, moving_speed_mps=moving_threshold_mps)
    total_m = mobility.total_distance_m(prepared)
    avg_mps, max_mps = mobility.speed_stats_mps(prepared, segments)
    start_ts, end_ts = mobility.time_range(prepared)
    box = mobility.bbox(prepared)

    # Movement in zero elapsed time (consecutive samples sharing a timestamp but
    # differing in position) has an undefined instantaneous speed: its distance
    # still counts toward the totals, but it can't contribute a finite speed, so
    # flag it rather than let the summary read as real distance at 0 km/h with no
    # explanation. (segment_by_motion already classifies these as moving, not a
    # self-contradictory "stopped" segment.)
    zero_time_moves = mobility.zero_time_position_change_count(prepared)
    if zero_time_moves > 0:
        quality.issues.append(
            f"{zero_time_moves} sample(s) changed position with no elapsed time "
            "(identical timestamps); their speed is undefined and excluded from "
            "speed stats, though their distance is still counted."
        )

    try:
        raw_stops = detect_stops(
            prepared,
            radius_m=opts.stop_radius_m,
            min_duration_s=opts.min_stop_minutes * 60.0,
            # Reuse the trip gap setting so stops split on the same real-world
            # absence trips do — one knob, no divergent gap mechanism.
            split_gap_s=opts.trip_split_gap_minutes * 60.0,
            # Bound the scan so a pathological single giant cluster (a very slow
            # drift, or a same-timestamp mass) can't tie up a worker — see ADR
            # 0016. A real track does far less work than this absolute cap.
            max_scan_work=_MAX_STOP_SCAN_WORK,
        )
    except StopScanBudgetExceeded:
        # The input is too dense/degenerate for stop detection within budget.
        # Skip stops (and therefore trips) but still return every other insight
        # — distance, speed, bbox, hotspots — rather than hang or 500.
        raw_stops = []
        quality.issues.append(
            "Stop detection skipped: the track is too dense or degenerate "
            "(e.g. a very slow drift or many samples at one timestamp) to "
            "analyze within the allotted budget."
        )
    stops = merge_nearby_stops(
        raw_stops,
        merge_radius_m=opts.merge_stops_within_m,
        # Same real-world-absence gap detect_stops split on, so merge can't
        # recombine what detect_stops correctly kept apart. One knob.
        split_gap_s=opts.trip_split_gap_minutes * 60.0,
        # A merged stop is never larger than one stop: the same stop_radius the
        # detector uses caps the merge, so a slow walk/drift can't chain into a
        # single implausibly wide "stop." See ADR 0013.
        stop_radius_m=opts.stop_radius_m,
    )

    # Trips use the already-detected stops as split candidates so we
    # don't re-cluster the same points.
    trips = detect_trips(
        prepared,
        stops=stops,
        trip_split_stop_seconds=opts.trip_split_stop_minutes * 60.0,
        trip_split_gap_seconds=opts.trip_split_gap_minutes * 60.0,
    )
    hotspots = detect_hotspots(
        prepared,
        grid_m=opts.hotspot_grid_m,
        min_samples=opts.hotspot_min_samples,
        max_results=opts.hotspot_max_results,
        split_gap_s=opts.trip_split_gap_minutes * 60.0,
    )

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
        trips=[_trip_to_out(i, t) for i, t in enumerate(trips)],
        hotspots=[_hotspot_to_out(h) for h in hotspots],
        quality=quality,
    )
