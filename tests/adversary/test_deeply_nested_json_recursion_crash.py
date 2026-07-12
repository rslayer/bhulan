"""
Defect: A deeply-nested JSON array in ``points`` crashes the server with
an unhandled 500 (``RecursionError``) instead of a clean 422.

Root cause: stdlib ``json.loads`` happily parses arbitrarily deep
nesting (e.g. ``[[[[...[1]...]]]]`` 1000+ levels deep). Pydantic then
correctly rejects the value -- a deeply nested list is not a valid
``PointIn`` -- and raises a ``RequestValidationError`` whose ``"input"``
field embeds the same deeply-nested structure. FastAPI's default
validation-error handler serializes that error body with
``fastapi.encoders.jsonable_encoder``, which recurses once per nesting
level. Past ~1000 levels (Python's default recursion limit) this raises
``RecursionError: maximum recursion depth exceeded`` while *building the
422 response itself*, which surfaces to the client as an unhandled 500.

This is the same class of bug as the NaN/Infinity crash (see
``test_nan_infinity_floats_crash.py``): the input is correctly rejected
by pydantic, but rendering the rejection's error body is what actually
crashes.

Impact: any unauthenticated caller can 500 ``/v1/insights``,
``/v1/plot/validate``, and ``/v1/compare`` with a small (a few KB),
cheap-to-generate payload -- ``depth`` nested single-element JSON arrays
cost only ``O(depth)`` bytes to send.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

_NESTING_DEPTH = 2000


def _nested_json_array(depth: int) -> str:
    return "[" * depth + "1" + "]" * depth


def test_insights_deeply_nested_points_array_returns_500(client: TestClient):
    body = '{"points": ' + _nested_json_array(_NESTING_DEPTH) + "}"
    r = client.post(
        "/v1/insights", content=body.encode(), headers={"content-type": "application/json"}
    )
    assert r.status_code == 422, (
        f"expected a clean 422 for a deeply nested points array, got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_plot_validate_deeply_nested_points_array_returns_500(client: TestClient):
    body = '{"points": ' + _nested_json_array(_NESTING_DEPTH) + "}"
    r = client.post(
        "/v1/plot/validate",
        content=body.encode(),
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 422, (
        f"expected a clean 422 for a deeply nested points array, got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_compare_deeply_nested_points_array_returns_500(client: TestClient):
    nested = _nested_json_array(_NESTING_DEPTH)
    body = (
        '{"tracks": [{"points": '
        + nested
        + '}, {"points": [{"lat": 1.0, "lon": 1.0}]}]}'
    )
    r = client.post(
        "/v1/compare", content=body.encode(), headers={"content-type": "application/json"}
    )
    assert r.status_code == 422, (
        f"expected a clean 422 for a deeply nested points array, got "
        f"{r.status_code}: {r.text[:200]}"
    )
