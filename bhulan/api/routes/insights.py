"""
HTTP routes for the public mobility-insights surface.

These endpoints are stateless: no MongoDB, no auth, no persistence. They
accept a list of GPS coordinates and return either a computed
:class:`~bhulan.analytics.insights.InsightsReport` or a validation summary
suitable for driving a client-side map.
"""

from typing import List, Optional, Tuple

from fastapi import APIRouter, Body, File, HTTPException, Request, UploadFile
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address

from bhulan.analytics.file_parsers import parse_file_bytes
from bhulan.analytics.insights import (
    InsightsOptions,
    InsightsReport,
    InsightsRequest,
    PointIn,
    compute_insights_with_geocoding,
)
from bhulan.analytics.parsers import ParseError, estimate_input_rows, parse_any
from bhulan.config.settings import settings

router = APIRouter(prefix="/v1", tags=["insights"])

# Local limiter instance — shares the same keying function as the app-level
# limiter so slowapi's app.state handler recognizes decorated routes.
limiter = Limiter(key_func=get_remote_address)

# Cap uploaded file size so a single request can't blow the process heap.
# 25 MB is comfortably above the largest GPX exports phones produce (~8 MB
# for a multi-hour Strava ride) and still well under default LB body limits.
MAX_UPLOAD_BYTES = 25 * 1024 * 1024


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


def _materialize_points(
    structured: Optional[List[PointIn]], text: Optional[str]
) -> Tuple[List[PointIn], List[str]]:
    issues: List[str] = []
    if structured:
        return list(structured), issues
    if text is None or not text.strip():
        raise HTTPException(
            status_code=400, detail="Request must include either 'points' or non-empty 'text'"
        )
    try:
        parsed = parse_any(text)
    except ParseError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not parsed:
        issues.append("No coordinates could be parsed from the provided text")
    return parsed, issues


@router.post("/insights", response_model=InsightsReport)
@limiter.limit(lambda: settings.RATE_LIMIT_INSIGHTS or "1000/second")
async def insights_endpoint(
    request: Request,
    payload: RawInsightsRequest = Body(...),
) -> InsightsReport:
    """
    Compute mobility insights for a batch of GPS coordinates.

    Accepts either a structured ``points`` array (fast path) or raw ``text``
    that the server will parse using :func:`bhulan.analytics.parsers.parse_any`.
    Setting ``options.geocode_stops = true`` enriches each detected stop
    with a Nominatim-derived ``place_name`` (adds ~1s per unique stop).
    """
    points, parse_issues = _materialize_points(payload.points, payload.text)
    req = InsightsRequest(points=points, options=payload.options)
    report = await compute_insights_with_geocoding(req)
    if parse_issues:
        report.quality.issues = list(report.quality.issues) + parse_issues
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
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"File too large: {len(data)} bytes; limit is {MAX_UPLOAD_BYTES}."
            ),
        )
    if not data:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    try:
        points = parse_file_bytes(file.filename or "", data)
    except ParseError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

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
