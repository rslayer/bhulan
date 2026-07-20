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
    """Return ``(min_lat, min_lon, max_lat, max_lon)`` for the given points.

    The box is the *minimal-longitude-span* box. For the common case (points
    within a <180° longitude window) this is the naive ``min/max`` and
    ``min_lon <= max_lon`` as usual. When the points straddle the antimeridian
    (±180°) — e.g. GPS jitter around Fiji or the Aleutians — the minimal box
    crosses ±180° and is reported with ``min_lon > max_lon``: the box runs east
    from ``min_lon``, over +180° / −180°, to ``max_lon`` (the "short way
    round"). A consumer must treat ``min_lon > max_lon`` as an
    antimeridian-crossing box rather than an empty/inverted one. See
    ``spec/adrs/0004-antimeridian-projection-and-bbox.md``.
    """
    if not lats or not lons:
        raise ValueError("bounding_box requires at least one point")
    lats_a = np.asarray(lats, dtype=np.float64)
    lons_a = np.asarray(lons, dtype=np.float64)

    min_lat = float(np.min(lats_a))
    max_lat = float(np.max(lats_a))

    raw_min = float(np.min(lons_a))
    raw_max = float(np.max(lons_a))
    raw_span = raw_max - raw_min

    # Only a track whose raw longitude span exceeds 180° can possibly be tighter
    # when measured the other way around the globe, so a straddle is only even
    # considered past that gate. This mirrors ``latlon_to_xy_m``'s straddle test
    # (``raw_max - raw_min > 180``) exactly, which matters for byte-identity: for
    # a same-sign track the ``+360`` shift below reintroduces ~1e-13° of float
    # noise, and comparing the noisy shifted span against the raw span would
    # occasionally (and pointlessly) pick the shifted box for a track that never
    # goes near ±180°. Gating on the raw span keeps every non-straddling track on
    # the exact naive min/max (byte-identical to before).
    if raw_span > 180.0:
        # Longitudes shifted into [0, 360) so points clustered near ±180° become
        # contiguous; prefer the shifted box only when it is strictly tighter.
        shifted = np.where(lons_a < 0.0, lons_a + 360.0, lons_a)
        shifted_min = float(np.min(shifted))
        shifted_max = float(np.max(shifted))
        if shifted_max - shifted_min < raw_span:
            # Crosses the antimeridian: fold the [0, 360) edges back to (-180, 180].
            min_lon = ((shifted_min + 180.0) % 360.0) - 180.0
            max_lon = ((shifted_max + 180.0) % 360.0) - 180.0
        else:
            min_lon = raw_min
            max_lon = raw_max
    else:
        min_lon = raw_min
        max_lon = raw_max

    return (min_lat, min_lon, max_lat, max_lon)


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

    # Reference longitude. For tracks that straddle the antimeridian (±180°) the
    # arithmetic mean of the raw longitudes lands on the *antipode* (e.g. the
    # mean of 179.99 and -179.99 is 0), which would leave every point ~180° from
    # the reference. Detect the straddle (raw span > 180°) and take the mean over
    # longitudes shifted into [0, 360) so the reference sits inside the real
    # cluster. Tracks that do not straddle keep the plain mean, so their
    # projection is byte-identical to before.
    raw_min = float(np.min(lons_a))
    raw_max = float(np.max(lons_a))
    cos_lat0 = math.cos(math.radians(lat0))
    meters_per_deg_lat = 111_320.0
    meters_per_deg_lon = 111_320.0 * cos_lat0

    if raw_max - raw_min > 180.0:
        shifted = np.where(lons_a < 0.0, lons_a + 360.0, lons_a)
        lon0 = float(np.mean(shifted))
        # Normalise the signed longitude difference to (-180, 180] before
        # scaling to metres, so a point on the far side of ±180° from ``lon0``
        # uses the short way round (~metres) instead of the long way
        # (~40 000 km).
        dlon = ((lons_a - lon0 + 180.0) % 360.0) - 180.0
    else:
        # No antimeridian straddle: the modulo is a mathematical no-op here, so
        # skip it and keep the projection byte-identical to prior releases.
        lon0 = float(np.mean(lons_a))
        dlon = lons_a - lon0

    y = (lats_a - lat0) * meters_per_deg_lat
    x = dlon * meters_per_deg_lon
    return x, y
