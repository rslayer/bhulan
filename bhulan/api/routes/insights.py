"""
HTTP routes for the public mobility-insights surface.

These endpoints are stateless: no MongoDB, no auth, no persistence. They
accept a list of GPS coordinates and return either a computed
:class:`~bhulan.analytics.insights.InsightsReport` or a validation summary
suitable for driving a client-side map.
"""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Body, HTTPException
from pydantic import BaseModel, Field

from bhulan.analytics.insights import (
    InsightsOptions,
    InsightsReport,
    InsightsRequest,
    PointIn,
    compute_insights,
)
from bhulan.analytics.parsers import ParseError, parse_any

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


def _materialize_points(
    structured: Optional[List[PointIn]], text: Optional[str]
) -> tuple[List[PointIn], List[str]]:
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
async def insights_endpoint(
    payload: RawInsightsRequest = Body(...),
) -> InsightsReport:
    """
    Compute mobility insights for a batch of GPS coordinates.

    Accepts either a structured ``points`` array (fast path) or raw ``text``
    that the server will parse using :func:`bhulan.analytics.parsers.parse_any`.
    """
    points, parse_issues = _materialize_points(payload.points, payload.text)
    req = InsightsRequest(points=points, options=payload.options)
    report = compute_insights(req)
    if parse_issues:
        report.quality.issues = list(report.quality.issues) + parse_issues
    return report


@router.post("/plot/validate", response_model=PlotResponse)
async def plot_validate_endpoint(payload: PlotRequest = Body(...)) -> PlotResponse:
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

    if rejected:
        issues.append(f"{rejected} point(s) had coordinates outside valid ranges")

    return PlotResponse(
        accepted=len(accepted),
        rejected=rejected,
        issues=issues,
        points=accepted,
    )
