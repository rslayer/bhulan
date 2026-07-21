"""
Defect: A ``speed_mps`` value that is non-finite -- or merely finite-but-huge --
crashes the server with an unhandled 500 while serializing an *otherwise
successful* response.

Root cause: ``PointIn.speed_mps`` (``bhulan/analytics/insights.py``) originally
had only a ``ge=0`` floor and no ceiling / finiteness guard. JSON ``1e400``
parses to ``inf`` (``inf >= 0`` is True, so it was accepted), and even a finite
value around ``1e308`` overflows to ``inf`` when the analytics multiply the
track's ``max_speed_mps`` by ``MS_TO_KMH`` (3.6) to report ``max_speed_kmh``
(``compute_insights`` line ~431, ``build_trip`` line ~264). The non-finite float
then reaches Starlette's ``JSONResponse``, which serializes with
``allow_nan=False`` and raises ``ValueError: Out of range float values are not
JSON compliant: inf`` while *building the 200 body* -- surfacing to the client
as an unhandled 500.

This is the same "non-finite float reaches the JSON encoder" class as the
NaN/Infinity defect (``test_nan_infinity_floats_crash.py``), but reached through
a *successful* response rather than a rejected one, so the validation-error
scrubber in ``bhulan/api/app.py`` does not cover it.

Fix / expected behavior: an implausible ``speed_mps`` (non-finite, or above
``MAX_SPEED_MPS``) is treated as *missing* rather than rejected -- one bad
telemetry field must not fail the whole request, and the analytics already
derive speed from distance/time for points carrying no ``speed_mps``. Values
that are merely invalid rather than implausible (negative, non-numeric) still
produce the usual 422.

Impact: any unauthenticated caller could 500 ``/v1/insights``,
``/v1/plot/validate``, and ``/v1/compare`` (structured ``points`` path) and the
``text`` parse path with a one-point payload carrying a pathological
``speed_mps``.
"""

from __future__ import annotations

import math

from fastapi.testclient import TestClient

# Values that must not reach the JSON encoder as a non-finite float, as the raw
# JSON number literals a real client sends (the server parses each with the
# stdlib decoder, so ``1e400`` becomes ``inf`` server-side):
#   1e400  -> parsed as inf outright
#   1e308  -> finite on input, but 1e308 * 3.6 overflows to inf
#   5e307  -> just past the overflow threshold
#   2e7    -> finite and serializable, but beyond MAX_SPEED_MPS (implausible)
# They are sent as raw request bytes rather than via ``json=`` because a strict
# httpx encoder (``allow_nan=False``) refuses to serialize ``inf`` client-side,
# which would fail the test before it ever reached the server — a harness quirk,
# not the behaviour under test.
_BAD_SPEEDS = ["1e400", "1e308", "5e307", "2e7"]


def _post_raw(client: TestClient, url: str, body: str):
    return client.post(url, content=body.encode(), headers={"content-type": "application/json"})


def _assert_finite_numbers(obj, ctx: str) -> None:
    """Every float anywhere in the response body must be JSON-finite."""
    if isinstance(obj, float):
        assert math.isfinite(obj), f"non-finite float {obj!r} in response for {ctx}"
    elif isinstance(obj, dict):
        for v in obj.values():
            _assert_finite_numbers(v, ctx)
    elif isinstance(obj, list):
        for v in obj:
            _assert_finite_numbers(v, ctx)


def test_insights_implausible_speed_is_ignored_not_rejected(client: TestClient):
    for speed in _BAD_SPEEDS:
        r = _post_raw(
            client,
            "/v1/insights",
            '{"points": [{"lat": 1.0, "lon": 1.0, "speed_mps": %s}]}' % speed,
        )
        assert r.status_code == 200, (
            f"speed_mps={speed} on /v1/insights should be ignored (200), "
            f"got {r.status_code}: {r.text[:200]}"
        )
        _assert_finite_numbers(r.json(), f"/v1/insights speed_mps={speed}")


def test_plot_validate_implausible_speed_is_dropped(client: TestClient):
    for speed in _BAD_SPEEDS:
        r = _post_raw(
            client,
            "/v1/plot/validate",
            '{"points": [{"lat": 1.0, "lon": 1.0, "speed_mps": %s}]}' % speed,
        )
        assert r.status_code == 200, (
            f"speed_mps={speed} on /v1/plot/validate should be ignored (200), "
            f"got {r.status_code}: {r.text[:200]}"
        )
        body = r.json()
        _assert_finite_numbers(body, f"/v1/plot/validate speed_mps={speed}")
        # The point itself survives; only the bad speed is dropped.
        assert body["points"][0]["speed_mps"] is None, (
            f"expected implausible speed_mps={speed} to be dropped to null, "
            f"got {body['points'][0]['speed_mps']!r}"
        )


def test_compare_implausible_speed_is_ignored(client: TestClient):
    for speed in _BAD_SPEEDS:
        r = _post_raw(
            client,
            "/v1/compare",
            '{"tracks": [{"points": [{"lat": 1.0, "lon": 1.0, "speed_mps": %s}]}, '
            '{"points": [{"lat": 2.0, "lon": 2.0}]}]}' % speed,
        )
        assert r.status_code == 200, (
            f"speed_mps={speed} on /v1/compare should be ignored (200), "
            f"got {r.status_code}: {r.text[:200]}"
        )
        _assert_finite_numbers(r.json(), f"/v1/compare speed_mps={speed}")


def test_insights_text_path_implausible_speed_is_clean(client: TestClient):
    # The CSV/text path parses "1e400" to inf via float(); the bad speed must be
    # dropped rather than 500ing or losing the point.
    r = client.post(
        "/v1/insights",
        json={"text": "lat,lon,ts,speed\n1.0,1.0,,1e400\n"},
    )
    assert r.status_code == 200, (
        f"non-finite speed via text expected 200, got {r.status_code}: {r.text[:200]}"
    )
    _assert_finite_numbers(r.json(), "/v1/insights text path")


def test_analytics_still_derive_speed_when_reported_speed_is_dropped(
    client: TestClient,
):
    """Dropping a bad speed must not blind the analytics.

    With ``speed_mps`` ignored, the pipeline falls back to distance/time the
    same way it does for points that never carried a speed at all.
    """
    r = _post_raw(
        client,
        "/v1/insights",
        '{"points": ['
        '{"lat": 1.0, "lon": 1.0, "ts_utc": "2025-01-01T00:00:00Z", "speed_mps": 1e400}, '
        '{"lat": 1.001, "lon": 1.001, "ts_utc": "2025-01-01T00:00:10Z", "speed_mps": 1e400}]}',
    )
    assert r.status_code == 200, r.text[:200]
    summary = r.json()["summary"]
    assert summary["max_speed_kmh"] > 0, (
        "expected a distance/time-derived speed after the reported speed was "
        f"dropped, got {summary['max_speed_kmh']!r}"
    )
    assert math.isfinite(summary["max_speed_kmh"])


def test_invalid_speeds_still_rejected(client: TestClient):
    """Implausible != invalid. Negative / non-numeric speeds keep their 422."""
    for bad in (-5, "abc"):
        r = client.post(
            "/v1/insights",
            json={"points": [{"lat": 1.0, "lon": 1.0, "speed_mps": bad}]},
        )
        assert r.status_code == 422, (
            f"speed_mps={bad!r} should still be a validation error, "
            f"got {r.status_code}: {r.text[:200]}"
        )
