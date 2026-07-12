"""
Defect: A GeoJSON payload with structurally malformed ``coordinates``
(too few elements, a non-list value, or a non-numeric value) crashes the
server with an unhandled 500 instead of a clean 400/422.

Root cause: ``bhulan/analytics/parsers.py::_coords_from_geojson_geom``
(around line 199) indexes into ``coordinates`` and calls ``float(...)``
on the elements without any bounds/type checking:

    if gtype == "Point":
        return [(float(coords[1]), float(coords[0]))]

``parse_any`` -> ``parse_json`` -> ``parse_geojson`` only catches
``(json.JSONDecodeError, ParseError)`` around the call
(``bhulan/analytics/parsers.py::parse_any``, lines ~291-295). A bare
``IndexError`` (``coordinates: [1]`` -- too short), ``ValueError``
(``coordinates: "ab"`` -- string indexing then ``float("b")`` fails), or
``TypeError`` (``coordinates: 5`` -- not subscriptable/iterable) is none
of those, so it propagates all the way up through
``_materialize_points`` (``bhulan/api/routes/insights.py``, only wraps
``parse_any`` in ``except ParseError``) and out of the endpoint as an
unhandled 500.

Impact: any unauthenticated caller can crash ``/v1/insights``,
``/v1/plot/validate``, and ``/v1/compare`` by posting a ``text`` field
containing a GeoJSON object with malformed ``coordinates`` -- no
structured ``points`` array required, just free-text GeoJSON that the
service advertises support for (see the ``parsers.py`` module
docstring: "GeoJSON ``FeatureCollection`` of ``Point`` / ``LineString``
features").
"""

from __future__ import annotations

import json

from fastapi.testclient import TestClient


def test_insights_geojson_point_with_short_coordinates_returns_500(client: TestClient):
    body = json.dumps({"type": "Point", "coordinates": [1]})
    r = client.post("/v1/insights", json={"text": body})
    assert r.status_code in (400, 422), (
        f"expected a clean 4xx for malformed GeoJSON coordinates, got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_insights_geojson_point_with_string_coordinates_returns_500(client: TestClient):
    body = json.dumps({"type": "Point", "coordinates": "ab"})
    r = client.post("/v1/insights", json={"text": body})
    assert r.status_code in (400, 422), (
        f"expected a clean 4xx for non-list GeoJSON coordinates, got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_insights_geojson_point_with_numeric_coordinates_returns_500(client: TestClient):
    body = json.dumps({"type": "Point", "coordinates": 5})
    r = client.post("/v1/insights", json={"text": body})
    assert r.status_code in (400, 422), (
        f"expected a clean 4xx for non-iterable GeoJSON coordinates, got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_plot_validate_geojson_linestring_with_short_pairs_returns_500(
    client: TestClient,
):
    body = json.dumps({"type": "LineString", "coordinates": [[1], [2]]})
    r = client.post("/v1/plot/validate", json={"text": body})
    assert r.status_code in (400, 422), (
        f"expected a clean 4xx for malformed LineString coordinate pairs, "
        f"got {r.status_code}: {r.text[:200]}"
    )


def test_compare_geojson_point_with_short_coordinates_returns_500(client: TestClient):
    body = json.dumps({"type": "Point", "coordinates": [1]})
    r = client.post(
        "/v1/compare",
        json={
            "tracks": [
                {"text": body},
                {"points": [{"lat": 1.0, "lon": 1.0}]},
            ]
        },
    )
    assert r.status_code in (400, 422), (
        f"expected a clean 4xx for malformed GeoJSON coordinates, got "
        f"{r.status_code}: {r.text[:200]}"
    )
