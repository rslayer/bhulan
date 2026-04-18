"""Tests for :mod:`bhulan.analytics.geodesy`."""

from __future__ import annotations

import math

import numpy as np
import pytest

from bhulan.analytics.geodesy import (
    EARTH_RADIUS_M,
    bounding_box,
    haversine_m,
    haversine_vec_m,
    latlon_to_xy_m,
)


def test_haversine_zero_when_identical():
    assert haversine_m(37.0, -122.0, 37.0, -122.0) == 0.0


def test_haversine_one_degree_latitude_is_about_111_km():
    d = haversine_m(0.0, 0.0, 1.0, 0.0)
    assert d == pytest.approx(111_195, rel=1e-3)


def test_haversine_vec_matches_scalar():
    lats = [12.97, 12.98, 13.00]
    lons = [77.59, 77.60, 77.62]
    vec = haversine_vec_m(lats, lons)
    expected = [
        haversine_m(lats[0], lons[0], lats[1], lons[1]),
        haversine_m(lats[1], lons[1], lats[2], lons[2]),
    ]
    assert vec == pytest.approx(expected, rel=1e-9)


def test_haversine_vec_empty_for_short_inputs():
    assert haversine_vec_m([], []).size == 0
    assert haversine_vec_m([1.0], [2.0]).size == 0


def test_bounding_box_basic():
    lats = [10.0, 11.5, 9.75]
    lons = [20.0, 21.5, 19.0]
    mn_lat, mn_lon, mx_lat, mx_lon = bounding_box(lats, lons)
    assert (mn_lat, mn_lon, mx_lat, mx_lon) == (9.75, 19.0, 11.5, 21.5)


def test_bounding_box_raises_on_empty():
    with pytest.raises(ValueError):
        bounding_box([], [])


def test_latlon_to_xy_projection_is_small_for_nearby_points():
    lats = [12.9716, 12.9720, 12.9725]
    lons = [77.5946, 77.5950, 77.5955]
    x, y = latlon_to_xy_m(lats, lons)
    assert np.max(np.abs(x)) < 500
    assert np.max(np.abs(y)) < 500


def test_haversine_symmetry():
    a = haversine_m(1.0, 2.0, 3.0, 4.0)
    b = haversine_m(3.0, 4.0, 1.0, 2.0)
    assert a == pytest.approx(b)


def test_haversine_half_circumference():
    d = haversine_m(0.0, 0.0, 0.0, 180.0)
    assert d == pytest.approx(math.pi * EARTH_RADIUS_M, rel=1e-6)
