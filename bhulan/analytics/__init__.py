"""
Analytics module for Bhulan.

Provides stateless mobility analytics over a list of GPS coordinates:
distance, speed, moving/idle segments, bounding box, and stop detection.

Nothing in this module touches MongoDB or any external service. It can be
used as a library or exposed over HTTP via :mod:`bhulan.api.routes.insights`.
"""

from bhulan.analytics.insights import (
    InsightsOptions,
    InsightsReport,
    InsightsRequest,
    compute_insights,
)

__all__ = [
    "InsightsRequest",
    "InsightsReport",
    "InsightsOptions",
    "compute_insights",
]
