"""Tests for :mod:`bhulan.analytics.parsers`."""

from __future__ import annotations

import json

import pytest

from bhulan.analytics.parsers import (
    ParseError,
    parse_any,
    parse_csv_text,
    parse_geojson,
    parse_json,
    parse_plain_text,
)


def test_parse_csv_with_headers():
    text = "lat,lon,ts\n12.97,77.59,2025-01-01T10:00:00Z\n12.98,77.60,2025-01-01T10:01:00Z\n"
    pts = parse_csv_text(text)
    assert len(pts) == 2
    assert pts[0].lat == 12.97
    assert pts[0].lon == 77.59
    assert pts[0].ts_utc is not None


def test_parse_csv_without_headers():
    text = "12.97,77.59\n12.98,77.60\n"
    pts = parse_csv_text(text)
    assert len(pts) == 2


def test_parse_csv_tolerates_extra_columns():
    text = "latitude,longitude,speed_kmh,ts\n12.97,77.59,36,2025-01-01T10:00:00Z\n"
    pts = parse_csv_text(text)
    assert len(pts) == 1
    assert pts[0].speed_mps == pytest.approx(10.0, rel=1e-3)


def test_parse_plain_text_one_per_line():
    text = "12.97,77.59\n# comment\n12.98 77.60\n"
    pts = parse_plain_text(text)
    assert len(pts) == 2
    assert pts[1].lon == 77.60


def test_parse_json_array_of_objects():
    data = json.dumps(
        [
            {"lat": 1.0, "lon": 2.0, "timestamp": "2025-01-01T00:00:00Z"},
            {"latitude": 3.0, "longitude": 4.0},
        ]
    )
    pts = parse_json(data)
    assert len(pts) == 2
    assert pts[1].lat == 3.0


def test_parse_json_array_of_tuples():
    pts = parse_json("[[10, 20], [30, 40]]")
    assert len(pts) == 2
    assert pts[0].lon == 20


def test_parse_geojson_feature_collection():
    gj = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [77.59, 12.97]},
                "properties": {"ts": "2025-01-01T00:00:00Z"},
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[1, 2], [3, 4]],
                },
                "properties": {},
            },
        ],
    }
    pts = parse_geojson(gj)
    assert len(pts) == 3
    assert pts[0].lat == 12.97
    assert pts[0].lon == 77.59


def test_parse_any_dispatches_by_first_char():
    assert parse_any("[[1,2]]")[0].lon == 2
    assert parse_any("lat,lon\n1,2\n")[0].lat == 1
    assert parse_any("1,2\n3,4\n")[0].lat == 1


def test_parse_geojson_rejects_unknown_type():
    with pytest.raises(ParseError):
        parse_geojson({"type": "Wat"})


def test_parse_csv_skips_out_of_range_lat_lon():
    # A previous version of the parser would crash with a pydantic
    # ``ValidationError`` when lat>90 or lon>180 appeared in user input,
    # causing /v1/plot/validate to 500 instead of rejecting the row.
    text = "12.97,77.59\n91.0,0.0\n0.0,181.0\n-12.34,56.78\n"
    pts = parse_csv_text(text)
    assert [(p.lat, p.lon) for p in pts] == [(12.97, 77.59), (-12.34, 56.78)]


def test_parse_plain_text_skips_out_of_range_lat_lon():
    text = "12.97,77.59\n91.0,0.0\n-12.34,56.78\n"
    pts = parse_plain_text(text)
    assert [(p.lat, p.lon) for p in pts] == [(12.97, 77.59), (-12.34, 56.78)]


def test_parse_json_array_skips_out_of_range():
    pts = parse_json("[[12.97,77.59],[91.0,0.0],[-12.34,56.78]]")
    assert [(p.lat, p.lon) for p in pts] == [(12.97, 77.59), (-12.34, 56.78)]
