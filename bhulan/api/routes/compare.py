"""
HTTP route for multi-track comparison.

``POST /v1/compare`` takes 2+ tracks (structured points or raw text) and
returns per-track insights plus a set of hotspots computed over the
*pooled* samples. The hotspot pass is what makes this endpoint more than
a fan-out of ``/v1/insights`` — it's how a user sees "I visited these 3
places across all these runs" instead of "here's 3 separate heatmaps".

Each sub-track is parsed and analyzed with the same options (including
``geocode_stops``) so the UI can lay the reports out side by side
without a per-track option grid.
"""

from typing import List, Optional, Tuple

from fastapi import APIRouter, Body, HTTPException, Request
from pydantic import BaseModel, Field, field_validator
from slowapi import Limiter
from slowapi.util import get_remote_address

from bhulan.analytics.hotspots import detect_hotspots
from bhulan.analytics.insights import (
    HotspotOut,
    InsightsOptions,
    InsightsReport,
    InsightsRequest,
    PointIn,
    _hotspot_to_out,
    compute_insights_with_geocoding,
)
from bhulan.analytics.mobility import TrackSample, prepare_track
from bhulan.analytics.parsers import ParseError, parse_any
from bhulan.config.settings import settings

router = APIRouter(prefix="/v1", tags=["compare"])

# Local limiter — same keying function as app.state so slowapi's 429
# handler recognizes decorated routes.
limiter = Limiter(key_func=get_remote_address)

# Hard cap on the number of tracks accepted in a single request. 20 is
# well above the common "compare my last 10 runs" use case and keeps the
# pooled-hotspot pass bounded.
MAX_TRACKS = 20


class CompareTrack(BaseModel):
    """One input track for ``POST /v1/compare``."""

    label: Optional[str] = Field(
        None,
        max_length=120,
        description="Short human-readable name (e.g. 'Monday run'). Optional.",
    )
    points: Optional[List[PointIn]] = Field(
        None, description="Pre-structured points (preferred when the client can parse)."
    )
    text: Optional[str] = Field(
        None,
        description="Raw coordinates (CSV, JSON, GeoJSON, or lat/lon lines).",
    )


class CompareRequest(BaseModel):
    """Request body for :func:`compare_endpoint`."""

    tracks: List[CompareTrack] = Field(..., description="2+ tracks to compare")
    options: InsightsOptions = Field(default_factory=InsightsOptions)

    @field_validator("tracks")
    @classmethod
    def _min_tracks(cls, v: List[CompareTrack]) -> List[CompareTrack]:
        if len(v) < 2:
            raise ValueError("Provide at least 2 tracks to compare")
        if len(v) > MAX_TRACKS:
            raise ValueError(f"Too many tracks: {len(v)} > {MAX_TRACKS}")
        return v


class CompareTrackResult(BaseModel):
    """Per-track slice of the compare response."""

    label: str = Field(..., description="Either the caller-supplied label or a generated one")
    report: InsightsReport
    points: List[PointIn] = Field(
        default_factory=list,
        description="Normalized points for this track, ready to drop onto a map.",
    )


class CompareResponse(BaseModel):
    """Top-level compare response."""

    tracks: List[CompareTrackResult]
    shared_hotspots: List[HotspotOut] = Field(
        default_factory=list,
        description="Hotspots computed across all tracks' pooled samples.",
    )


def _track_to_points(track: CompareTrack, index: int) -> Tuple[List[PointIn], str]:
    """Return ``(points, label)`` for one track, raising HTTPException on
    empty / unparseable input.
    """
    label = (track.label or "").strip() or f"Track {index + 1}"
    if track.points:
        return list(track.points), label
    if not track.text or not track.text.strip():
        raise HTTPException(
            status_code=400,
            detail=(
                f"Track '{label}' must include either 'points' or non-empty 'text'"
            ),
        )
    try:
        parsed = parse_any(track.text)
    except ParseError as e:
        raise HTTPException(
            status_code=400, detail=f"Track '{label}': {e}"
        ) from e
    if not parsed:
        raise HTTPException(
            status_code=400,
            detail=f"Track '{label}': no coordinates parsed from text",
        )
    return parsed, label


@router.post("/compare", response_model=CompareResponse)
@limiter.limit(lambda: settings.RATE_LIMIT_INSIGHTS or "1000/second")
async def compare_endpoint(
    request: Request,
    payload: CompareRequest = Body(...),
) -> CompareResponse:
    """Compare 2+ tracks side by side.

    Applies the same ``options`` to every track so users don't have to
    tune knobs per-track. The response is a list of per-track reports
    plus a pooled-hotspot list so the UI can render "shared places"
    separately from per-track breakdowns.
    """
    per_track: List[CompareTrackResult] = []
    pooled_samples: List[TrackSample] = []

    for idx, track in enumerate(payload.tracks):
        points, label = _track_to_points(track, idx)
        req = InsightsRequest(points=points, options=payload.options)
        report = await compute_insights_with_geocoding(req)
        per_track.append(
            CompareTrackResult(label=label, report=report, points=points)
        )

        for p in points:
            pooled_samples.append(
                TrackSample(
                    lat=float(p.lat),
                    lon=float(p.lon),
                    ts_utc=p.ts_utc,
                    speed_mps=p.speed_mps,
                )
            )

    pooled_prepared = prepare_track(pooled_samples)
    shared = detect_hotspots(
        pooled_prepared,
        grid_m=payload.options.hotspot_grid_m,
        min_samples=payload.options.hotspot_min_samples,
        max_results=payload.options.hotspot_max_results,
    )
    shared_out = [_hotspot_to_out(h) for h in shared]

    if payload.options.geocode_stops and shared_out:
        # Match the stop/hotspot batching policy from the single-track
        # path so a shared hotspot already seen via a per-track geocode
        # hits the cache and returns free.
        from bhulan.analytics.geocoding import reverse_geocode_stops

        names = await reverse_geocode_stops([(h.lat, h.lon) for h in shared_out])
        shared_out = [
            h.model_copy(update={"place_name": n}) for h, n in zip(shared_out, names)
        ]

    return CompareResponse(tracks=per_track, shared_hotspots=shared_out)
