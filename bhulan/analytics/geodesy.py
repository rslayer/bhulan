"""
Geodesy helpers.

Distance calculations on a sphere, vectorized and scalar variants. All
distances are in meters unless otherwise stated.
"""

from __future__ import annotations

import math
from typing import Sequence, Tuple

import numpy as np

EARTH_RADIUS_M = 6_371_000.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in meters."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2.0 * math.asin(math.sqrt(min(1.0, a)))
    return EARTH_RADIUS_M * c


def haversine_vec_m(lats: Sequence[float], lons: Sequence[float]) -> np.ndarray:
    """
    Pairwise consecutive-point distances for a track, in meters.

    Returns an array of length ``len(lats) - 1`` where element ``i`` is the
    distance from point ``i`` to point ``i + 1``. If the input has fewer than
    two points, an empty array is returned.
    """
    lats_a = np.asarray(lats, dtype=np.float64)
    lons_a = np.asarray(lons, dtype=np.float64)
    if lats_a.size < 2:
        return np.array([], dtype=np.float64)

    phi1 = np.radians(lats_a[:-1])
    phi2 = np.radians(lats_a[1:])
    dphi = phi2 - phi1
    dlambda = np.radians(lons_a[1:] - lons_a[:-1])
    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
    a = np.clip(a, 0.0, 1.0)
    c = 2.0 * np.arcsin(np.sqrt(a))
    distances: np.ndarray = EARTH_RADIUS_M * c
    return distances


def bounding_box(
    lats: Sequence[float], lons: Sequence[float]
) -> Tuple[float, float, float, float]:
    """Return ``(min_lat, min_lon, max_lat, max_lon)`` for the given points."""
    if not lats or not lons:
        raise ValueError("bounding_box requires at least one point")
    lats_a = np.asarray(lats, dtype=np.float64)
    lons_a = np.asarray(lons, dtype=np.float64)
    return (
        float(np.min(lats_a)),
        float(np.min(lons_a)),
        float(np.max(lats_a)),
        float(np.max(lons_a)),
    )


def latlon_to_xy_m(
    lats: Sequence[float], lons: Sequence[float]
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Project lat/lon to local-tangent-plane meters around the track's centroid.

    Good enough for stop-detection clustering over any single track where
    points are within ~100 km of each other. Returns ``(x_m, y_m)`` arrays.
    """
    lats_a = np.asarray(lats, dtype=np.float64)
    lons_a = np.asarray(lons, dtype=np.float64)
    if lats_a.size == 0:
        return np.array([], dtype=np.float64), np.array([], dtype=np.float64)

    lat0 = float(np.mean(lats_a))
    lon0 = float(np.mean(lons_a))
    cos_lat0 = math.cos(math.radians(lat0))
    meters_per_deg_lat = 111_320.0
    meters_per_deg_lon = 111_320.0 * cos_lat0

    y = (lats_a - lat0) * meters_per_deg_lat
    x = (lons_a - lon0) * meters_per_deg_lon
    return x, y
