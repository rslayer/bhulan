"""
Defect: A timestamp near ``datetime.min``/``datetime.max`` combined with
a non-UTC timezone offset crashes the server with an unhandled 500
instead of a clean 4xx.

Root cause: ``bhulan/analytics/mobility.py::_to_utc`` (line 48) calls
``ts.astimezone(timezone.utc)`` on every point's timestamp with no
guard. Python's ``datetime.astimezone`` raises ``OverflowError: date
value out of range`` when shifting a timestamp already at the extreme
edge of the representable range past that edge -- e.g.
``0001-01-01T00:00:00+14:00`` needs to subtract 14 hours to reach UTC,
which underflows past ``datetime.min`` (year 1).

``PointIn.ts_utc`` (``bhulan/analytics/insights.py``) has no plausibility
bounds on the datetime value (unlike the ingestion-only
``TrackPoint.validate_timestamp`` in ``bhulan/models/canonical.py``,
which rejects timestamps before 1970 or more than 2 days in the future
-- that validator does not apply to the public ``/v1`` surface). The
``OverflowError`` is not caught anywhere between ``_to_utc`` and the
route handler, so it surfaces as a raw 500.

Impact: a single point with a valid ISO-8601 timestamp (accepted by
pydantic's ``datetime`` parsing, no ``ge``/``le`` bound exists for
``ts_utc``) crashes ``/v1/insights`` and ``/v1/compare``.
``/v1/plot/validate`` is unaffected because it never calls
``prepare_track``/``_to_utc``.
"""

from __future__ import annotations

from fastapi.testclient import TestClient


def test_insights_year_one_with_positive_offset_crashes(client: TestClient):
    """A single point is enough -- no second point needed to trigger the sort."""
    r = client.post(
        "/v1/insights",
        json={"points": [{"lat": 1.0, "lon": 1.0, "ts_utc": "0001-01-01T00:00:00+14:00"}]},
    )
    assert r.status_code in (200, 400, 422), (
        f"expected the request to be handled cleanly, got an unhandled "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_insights_year_9999_with_negative_offset_crashes(client: TestClient):
    r = client.post(
        "/v1/insights",
        json={
            "points": [
                {"lat": 1.0, "lon": 1.0, "ts_utc": "9999-12-31T23:59:59-14:00"}
            ]
        },
    )
    assert r.status_code in (200, 400, 422), (
        f"expected the request to be handled cleanly, got an unhandled "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_compare_year_one_with_positive_offset_crashes(client: TestClient):
    r = client.post(
        "/v1/compare",
        json={
            "tracks": [
                {
                    "points": [
                        {
                            "lat": 1.0,
                            "lon": 1.0,
                            "ts_utc": "0001-01-01T00:00:00+14:00",
                        }
                    ]
                },
                {"points": [{"lat": 2.0, "lon": 2.0}]},
            ]
        },
    )
    assert r.status_code in (200, 400, 422), (
        f"expected the request to be handled cleanly, got an unhandled "
        f"{r.status_code}: {r.text[:200]}"
    )
