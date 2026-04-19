"""Regression: /v1/compare returns 422 (not 500) when a track exceeds MAX_POINTS."""

from fastapi.testclient import TestClient

from bhulan.analytics.insights import MAX_POINTS
from bhulan.api.app import app


def test_compare_oversized_track_returns_422() -> None:
    client = TestClient(app)
    oversized = [{"lat": 12.9, "lon": 77.6}] * (MAX_POINTS + 1)
    small = [
        {"lat": 12.97, "lon": 77.59},
        {"lat": 12.98, "lon": 77.60},
    ]
    payload = {
        "tracks": [
            {"label": "big", "points": oversized},
            {"label": "small", "points": small},
        ],
        "options": {},
    }
    res = client.post("/v1/compare", json=payload)
    # FastAPI's request-body validator catches the per-track cap first
    # (via InsightsRequest._cap_length) because the CompareTrack.points
    # field is typed as List[PointIn]. If that indirection ever changes
    # and the cap only runs inside the endpoint, the explicit try/except
    # still converts ValidationError → 422. Either path is a user
    # error; 500 is not acceptable.
    assert res.status_code == 422, res.text
    body = res.json()
    # Track label or MAX_POINTS mention is enough to prove the message
    # carries actionable info rather than a generic "internal error".
    assert "big" in str(body) or str(MAX_POINTS) in str(body), body
