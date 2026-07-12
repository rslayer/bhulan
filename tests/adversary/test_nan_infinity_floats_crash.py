"""
Defect: NaN / Infinity in a request body crash the server with a raw
500 instead of the 422 that ``PointIn.lat``/``lon`` (``ge``/``le``
constraints) is supposed to produce.

Root cause: Python's stdlib ``json`` module accepts the non-standard
``NaN``/``Infinity``/``-Infinity`` literals (and silently turns an
out-of-range numeral like ``1e400`` into ``inf``) when *parsing* a
request body. Pydantic then correctly rejects the value (NaN/inf fails
the ``ge``/``le`` bound checks) and raises a ``RequestValidationError``.
But FastAPI's default validation-error handler echoes the rejected value
back in the ``"input"`` field of the error detail, and Starlette's
``JSONResponse`` renders with ``allow_nan=False`` (strict RFC 8259 JSON)
-- so *building the 422 body itself* raises
``ValueError: Out of range float values are not JSON compliant`` deep
inside Starlette, which surfaces to the client as an unhandled 500.

Impact: any public, unauthenticated caller can turn a would-be 422 into
a 500 on ``/v1/insights``, ``/v1/plot/validate``, and ``/v1/compare`` by
sending a single ``NaN``/``Infinity`` float anywhere pydantic would
otherwise reject it with a range error. This also affects fields with no
range constraint at all (e.g. ``options.stop_radius_m`` still has
``gt=0``), turning any bounded numeric field into a crash vector.
"""

from __future__ import annotations

from fastapi.testclient import TestClient


def test_insights_nan_lat_returns_500_instead_of_422(client: TestClient):
    r = client.post(
        "/v1/insights",
        content='{"points":[{"lat": NaN, "lon": 1.0}]}',
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 422, (
        f"expected a clean 422 validation error for NaN latitude, got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_insights_infinity_lat_returns_500_instead_of_422(client: TestClient):
    r = client.post(
        "/v1/insights",
        content='{"points":[{"lat": Infinity, "lon": 1.0}]}',
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 422, (
        f"expected a clean 422 validation error for +Infinity latitude, got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_insights_overflowing_numeral_returns_500_instead_of_422(client: TestClient):
    """A finite-looking but too-large JSON numeral (``1e400``) overflows to
    ``inf`` during stdlib JSON parsing before pydantic ever sees it -- same
    crash, no exotic ``NaN``/``Infinity`` literal required.
    """
    r = client.post(
        "/v1/insights",
        content='{"points":[{"lat": 1e400, "lon": 1.0}]}',
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 422, (
        f"expected a clean 422 validation error for an overflowing numeral, "
        f"got {r.status_code}: {r.text[:200]}"
    )


def test_plot_validate_nan_lat_returns_500_instead_of_422(client: TestClient):
    r = client.post(
        "/v1/plot/validate",
        content='{"points":[{"lat": NaN, "lon": 1.0}]}',
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 422, (
        f"expected a clean 422 validation error for NaN latitude, got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_compare_nan_lat_returns_500_instead_of_422(client: TestClient):
    r = client.post(
        "/v1/compare",
        content=(
            '{"tracks":[{"points":[{"lat": NaN, "lon":1.0}]},'
            '{"points":[{"lat":1.0,"lon":1.0}]}]}'
        ),
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 422, (
        f"expected a clean 422 validation error for NaN latitude, got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_insights_nan_in_options_field_returns_500_instead_of_422(client: TestClient):
    """Any bounded numeric field triggers the same crash, not just lat/lon."""
    r = client.post(
        "/v1/insights",
        content=(
            '{"points":[{"lat":1.0,"lon":1.0}],"options":{"stop_radius_m": NaN}}'
        ),
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 422, (
        f"expected a clean 422 validation error for NaN stop_radius_m, got "
        f"{r.status_code}: {r.text[:200]}"
    )
