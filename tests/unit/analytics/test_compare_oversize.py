"""Regression: /v1/compare rejects oversized tracks without returning 500."""

from fastapi.testclient import TestClient

from bhulan.analytics.insights import MAX_POINTS
from bhulan.api.app import app


def test_compare_oversized_track_returns_413() -> None:
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
    # The public-demo guard should reject this before the analytics
    # pipeline allocates large intermediate structures. Either way, 500
    # is not acceptable for a user-size error.
    assert res.status_code == 413, res.text
    body = res.json()
    # Track label or MAX_POINTS mention is enough to prove the message
    # carries actionable info rather than a generic "internal error".
    assert "big" in str(body) or str(MAX_POINTS) in str(body), body
