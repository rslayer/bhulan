"""
HTTP routes for the public mobility-insights surface.

These endpoints are stateless: no MongoDB, no auth, no persistence. They
accept a list of GPS coordinates and return either a computed
:class:`~bhulan.analytics.insights.InsightsReport` or a validation summary
suitable for driving a client-side map.
"""

import asyncio
from typing import List, Optional, Tuple

from fastapi import APIRouter, Body, Depends, File, HTTPException, Request, UploadFile
from pydantic import BaseModel, Field

from bhulan.analytics.file_parsers import parse_file_bytes
from bhulan.analytics.insights import (
    InsightsOptions,
    InsightsReport,
    InsightsRequest,
    PointIn,
    compute_insights_with_geocoding,
)
from bhulan.analytics.parsers import ParseError, estimate_input_rows, parse_any
from bhulan.api.deps import current_user_optional
from bhulan.api.limiter import limiter
from bhulan.auth import service as auth_service
from bhulan.auth.service import User
from bhulan.config.settings import settings

router = APIRouter(prefix="/v1", tags=["insights"])


class PlotRequest(BaseModel):
    """Request for :func:`plot_validate` — either structured points or raw text."""

    points: Optional[List[PointIn]] = Field(
        None, description="Pre-structured points (preferred when the client can parse)"
    )
    text: Optional[str] = Field(
        None, description="Raw user-pasted text: CSV, JSON, GeoJSON, or lat/lon lines"
    )


class PlotResponse(BaseModel):
    """Normalized points the frontend can drop straight onto a map."""

    accepted: int
    rejected: int
    issues: List[str]
    points: List[PointIn]


class RawInsightsRequest(BaseModel):
    """``/v1/insights`` request that accepts pasted text in lieu of structured input."""

    points: Optional[List[PointIn]] = None
    text: Optional[str] = None
    options: InsightsOptions = Field(default_factory=InsightsOptions)
    # Optional human-readable label for the history entry. Ignored when the
    # caller is anonymous.
    label: Optional[str] = Field(
        default=None,
        description="Optional label stored alongside the history entry",
        max_length=120,
    )


def _enforce_text_cap(text: Optional[str]) -> None:
    if text is None:
        return
    size = len(text.encode("utf-8"))
    if size > settings.MAX_PUBLIC_TEXT_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"Text payload too large: {size} bytes; "
                f"limit is {settings.MAX_PUBLIC_TEXT_BYTES}."
            ),
        )


def _enforce_point_cap(points: List[PointIn], label: str = "Request") -> None:
    if len(points) > settings.MAX_PUBLIC_POINTS:
        raise HTTPException(
            status_code=413,
            detail=(
                f"{label} has too many points: {len(points)} > "
                f"{settings.MAX_PUBLIC_POINTS}."
            ),
        )


def _require_geocoding_enabled(options: InsightsOptions) -> None:
    if options.geocode_stops and not settings.ENABLE_REVERSE_GEOCODING:
        raise HTTPException(
            status_code=403,
            detail="Reverse geocoding is disabled on this public instance",
        )


def _materialize_points(
    structured: Optional[List[PointIn]], text: Optional[str]
) -> Tuple[List[PointIn], List[str]]:
    issues: List[str] = []
    if structured:
        points = list(structured)
        _enforce_point_cap(points)
        return points, issues
    if text is None or not text.strip():
        raise HTTPException(
            status_code=400, detail="Request must include either 'points' or non-empty 'text'"
        )
    _enforce_text_cap(text)
    try:
        parsed = parse_any(text)
    except ParseError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    _enforce_point_cap(parsed)
    if not parsed:
        issues.append("No coordinates could be parsed from the provided text")
    return parsed, issues


def _summarize_report_for_history(report: InsightsReport) -> dict:
    """Compact projection of an InsightsReport for the history list view.

    The full report can be multi-MB on big tracks; the list UI only ever
    shows a handful of headline numbers. We persist the full request body
    separately so "replay" can reconstruct the run exactly.
    """
    return {
        "summary": report.summary.model_dump(mode="json"),
        "quality": report.quality.model_dump(mode="json"),
        "stop_count": len(report.stops),
        "trip_count": len(report.trips),
        "hotspot_count": len(report.hotspots),
    }


@router.post("/insights", response_model=InsightsReport)
@limiter.limit(lambda: settings.RATE_LIMIT_INSIGHTS or "1000/second")
async def insights_endpoint(
    request: Request,
    payload: RawInsightsRequest = Body(...),
    user: Optional[User] = Depends(current_user_optional),
) -> InsightsReport:
    """
    Compute mobility insights for a batch of GPS coordinates.

    Accepts either a structured ``points`` array (fast path) or raw ``text``
    that the server will parse using :func:`bhulan.analytics.parsers.parse_any`.
    Setting ``options.geocode_stops = true`` enriches each detected stop
    with a Nominatim-derived ``place_name`` (adds ~1s per unique stop).

    When the caller is authenticated (``Authorization: Bearer <session>``)
    and server-side auth is enabled, the run is also persisted to the
    user's history — look it up with ``GET /v1/history``.
    """
    _require_geocoding_enabled(payload.options)
    points, parse_issues = _materialize_points(payload.points, payload.text)
    req = InsightsRequest(points=points, options=payload.options)
    report = await compute_insights_with_geocoding(req)
    if parse_issues:
        report.quality.issues = list(report.quality.issues) + parse_issues

    # Best-effort history write. Never fail the insights request because
    # of a history persistence error — the user still gets their report.
    # ``save_history`` does blocking SQLite I/O so we push it to a thread
    # rather than stall the event loop on every authenticated request.
    if user is not None and settings.BHULAN_AUTH_ENABLED:
        try:
            await asyncio.to_thread(
                auth_service.save_history,
                settings.BHULAN_DB_PATH,
                user_id=user.id,
                kind="insights",
                label=payload.label,
                request_payload=payload.model_dump(mode="json"),
                summary_payload=_summarize_report_for_history(report),
            )
        except Exception:
            import logging

            logging.getLogger(__name__).exception(
                "Failed to persist history for user %s", user.id
            )

    return report


class ParseFileResponse(BaseModel):
    """Response for :func:`parse_file_endpoint` — the frontend feeds the
    accepted points straight into ``/v1/insights`` or ``/v1/plot/validate``.
    """

    filename: str
    accepted: int
    rejected: int
    issues: List[str]
    points: List[PointIn]


@router.post("/parse/file", response_model=ParseFileResponse)
@limiter.limit(lambda: settings.RATE_LIMIT_PLOT or "1000/second")
async def parse_file_endpoint(
    request: Request,
    file: UploadFile = File(..., description="GPX, KML, FIT, CSV, JSON, or plain-text coordinates"),
) -> ParseFileResponse:
    """Parse an uploaded GPS file and return normalized points.

    Dispatches on the filename extension: ``.gpx`` → gpxpy, ``.kml`` →
    stdlib XML, ``.fit`` → fitdecode, anything else → the text parsers.
    Files over ``MAX_UPLOAD_BYTES`` are rejected; the content is read into
    memory once and parsed synchronously (parsing is CPU-bound and fast
    enough that pushing it to a threadpool isn't worth it at this scale).
    """
    data = await file.read()
    if len(data) > settings.MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"File too large: {len(data)} bytes; limit is {settings.MAX_UPLOAD_BYTES}."
            ),
        )
    if not data:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    try:
        points = parse_file_bytes(file.filename or "", data)
    except ParseError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    _enforce_point_cap(points, label=file.filename or "Uploaded file")

    issues: List[str] = []
    if not points:
        issues.append(
            f"No coordinates could be parsed from '{file.filename}'. "
            f"Supported: .gpx, .kml, .fit, or UTF-8 CSV/JSON/plain text."
        )

    return ParseFileResponse(
        filename=file.filename or "",
        accepted=len(points),
        # File parsers drop malformed rows silently; we don't have a row
        # count from binary formats, so report 0 rejected for now.
        rejected=0,
        issues=issues,
        points=points,
    )


@router.post("/plot/validate", response_model=PlotResponse)
@limiter.limit(lambda: settings.RATE_LIMIT_PLOT or "1000/second")
async def plot_validate_endpoint(
    request: Request,
    payload: PlotRequest = Body(...),
) -> PlotResponse:
    """
    Parse and validate coordinates for the map view.

    Returns the cleaned points plus counts of accepted and rejected rows so
    the frontend can show a short summary alongside the rendered track.
    """
    points, issues = _materialize_points(payload.points, payload.text)

    accepted: List[PointIn] = []
    rejected = 0
    for p in points:
        if -90 <= p.lat <= 90 and -180 <= p.lon <= 180:
            accepted.append(p)
        else:
            rejected += 1

    # Rows that the parser silently dropped (bad floats, out-of-range lat/lon,
    # malformed CSV cells) don't show up in ``points`` at all. Estimate how
    # many rows the user intended to submit from the raw text so we can
    # surface the full rejected count to the client.
    if payload.text and not payload.points:
        expected = estimate_input_rows(payload.text)
        if expected is not None:
            dropped_in_parse = max(expected - len(points), 0)
            if dropped_in_parse:
                rejected += dropped_in_parse

    if rejected:
        issues.append(f"{rejected} row(s) could not be used (invalid or out of range)")

    return PlotResponse(
        accepted=len(accepted),
        rejected=rejected,
        issues=issues,
        points=accepted,
    )
