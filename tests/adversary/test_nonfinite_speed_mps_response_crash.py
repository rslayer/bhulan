"""
Defect: A ``speed_mps`` value that is non-finite -- or merely finite-but-huge --
crashes the server with an unhandled 500 while serializing an *otherwise
successful* response, instead of a clean 4xx (structured input) or a dropped
row (parsed text/file input).

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
scrubber in ``bhulan/api/app.py`` does not cover it. ``speed_mps`` is the only
unbounded caller-supplied float that feeds ``max_speed_mps``, so a single finite
``le`` bound closes every downstream overflow site.

Impact: any unauthenticated caller can 500 ``/v1/insights``,
``/v1/plot/validate``, and ``/v1/compare`` (structured ``points`` path) and the
``text`` parse path with a one-point payload carrying a pathological
``speed_mps``.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

# Values that must not reach the JSON encoder as a non-finite float:
#   1e400  -> parsed as inf outright
#   1e308  -> finite on input, but 1e308 * 3.6 overflows to inf
#   5e307  -> just past the overflow threshold
_BAD_SPEEDS = [1e400, 1e308, 5e307]


def test_insights_structured_nonfinite_speed_is_clean(client: TestClient):
    for speed in _BAD_SPEEDS:
        r = client.post(
            "/v1/insights",
            json={"points": [{"lat": 1.0, "lon": 1.0, "speed_mps": speed}]},
        )
        assert r.status_code in (200, 400, 422), (
            f"speed_mps={speed!r} on /v1/insights expected a clean 2xx/4xx, "
            f"got {r.status_code}: {r.text[:200]}"
        )


def test_plot_validate_structured_nonfinite_speed_is_clean(client: TestClient):
    for speed in _BAD_SPEEDS:
        r = client.post(
            "/v1/plot/validate",
            json={"points": [{"lat": 1.0, "lon": 1.0, "speed_mps": speed}]},
        )
        assert r.status_code in (200, 400, 422), (
            f"speed_mps={speed!r} on /v1/plot/validate expected a clean 2xx/4xx, "
            f"got {r.status_code}: {r.text[:200]}"
        )


def test_compare_structured_nonfinite_speed_is_clean(client: TestClient):
    for speed in _BAD_SPEEDS:
        r = client.post(
            "/v1/compare",
            json={
                "tracks": [
                    {"points": [{"lat": 1.0, "lon": 1.0, "speed_mps": speed}]},
                    {"points": [{"lat": 2.0, "lon": 2.0}]},
                ]
            },
        )
        assert r.status_code in (200, 400, 422), (
            f"speed_mps={speed!r} on /v1/compare expected a clean 2xx/4xx, "
            f"got {r.status_code}: {r.text[:200]}"
        )


def test_insights_text_path_nonfinite_speed_is_clean(client: TestClient):
    # The CSV/text path parses "1e400" to inf via float(); a bad speed column
    # must drop the row (or be handled), never 500.
    r = client.post(
        "/v1/insights",
        json={"text": "lat,lon,ts,speed\n1.0,1.0,,1e400\n"},
    )
    assert r.status_code in (200, 400, 422), (
        f"non-finite speed via text expected a clean 2xx/4xx, "
        f"got {r.status_code}: {r.text[:200]}"
    )
