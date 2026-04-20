"""Tests for :mod:`bhulan.api.routes.compare`."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from bhulan.api.app import app

client = TestClient(app)


def _track_points(n: int, start_lat: float, lon: float, start_min: float = 0.0):
    t0 = datetime(2025, 1, 1, 9, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=start_min)
    return [
        {
            "lat": start_lat + i * 0.0001,
            "lon": lon,
            "ts_utc": (t0 + timedelta(seconds=i)).isoformat(),
        }
        for i in range(n)
    ]


def test_compare_requires_two_tracks():
    # Single track should fail validation with a 422.
    resp = client.post(
        "/v1/compare",
        json={"tracks": [{"label": "solo", "points": _track_points(5, 12.97, 77.59)}]},
    )
    assert resp.status_code == 422


def test_compare_happy_path():
    resp = client.post(
        "/v1/compare",
        json={
            "tracks": [
                {"label": "Monday", "points": _track_points(30, 12.97, 77.59)},
                {"label": "Tuesday", "points": _track_points(30, 12.97, 77.59)},
            ],
            "options": {
                "stop_radius_m": 50.0,
                "min_stop_minutes": 5.0,
                "hotspot_min_samples": 3,
            },
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["tracks"]) == 2
    labels = [t["label"] for t in body["tracks"]]
    assert labels == ["Monday", "Tuesday"]
    for t in body["tracks"]:
        assert "report" in t
        assert "summary" in t["report"]
        assert "trips" in t["report"]
        assert "hotspots" in t["report"]
        assert len(t["points"]) == 30
    # Same coordinates in both tracks → pooled sample density spikes the
    # overlapping area, so at least one shared hotspot should be detected.
    assert len(body["shared_hotspots"]) >= 1
    h = body["shared_hotspots"][0]
    assert h["sample_count"] >= 30


def test_compare_text_track_parses():
    resp = client.post(
        "/v1/compare",
        json={
            "tracks": [
                {"label": "A", "text": "12.97,77.59\n12.98,77.60\n12.99,77.61"},
                {"label": "B", "text": "13.00,77.62\n13.01,77.63"},
            ]
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["tracks"]) == 2
    assert len(body["tracks"][0]["points"]) == 3
    assert len(body["tracks"][1]["points"]) == 2


def test_compare_rejects_empty_track():
    resp = client.post(
        "/v1/compare",
        json={
            "tracks": [
                {"label": "empty", "text": "   "},
                {"label": "B", "points": _track_points(5, 12.97, 77.59)},
            ]
        },
    )
    assert resp.status_code == 400
    assert "empty" in resp.json()["detail"].lower() or "empty" in resp.text.lower()


def test_compare_autolabels_missing_labels():
    resp = client.post(
        "/v1/compare",
        json={
            "tracks": [
                {"points": _track_points(5, 12.97, 77.59)},
                {"points": _track_points(5, 12.98, 77.60)},
            ]
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["tracks"][0]["label"] == "Track 1"
    assert body["tracks"][1]["label"] == "Track 2"
