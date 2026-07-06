"""Integration tests for the ``/v1`` insights + plot routes."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from bhulan.config.settings import settings

pytest.importorskip("fastapi")


@pytest.fixture(scope="module")
def client():
    import bhulan.storage.mongo_repo as mongo_repo

    class _FakeCollection:
        def create_index(self, *_args, **_kwargs):
            pass

        def find_one(self, *_args, **_kwargs):
            return None

    class _FakeRepo:
        collection = _FakeCollection()

        def create_indexes(self):
            pass

    class _FakeJobs:
        collection = _FakeCollection()

    mongo_repo.MongoTrackPointRepository = lambda *a, **kw: _FakeRepo()  # type: ignore[assignment]
    mongo_repo.MongoJobRegistry = lambda *a, **kw: _FakeJobs()  # type: ignore[assignment]

    from bhulan.api.app import app

    return TestClient(app)


def _track_payload():
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    pts = []
    for i in range(30):
        pts.append(
            {"lat": i * 0.0001, "lon": 0.0, "ts_utc": (t0 + timedelta(seconds=i)).isoformat()}
        )
    stop_start = t0 + timedelta(seconds=30)
    for i in range(600):
        pts.append(
            {
                "lat": 0.003 + (i % 3) * 0.00001,
                "lon": (i % 5) * 0.00001,
                "ts_utc": (stop_start + timedelta(seconds=i)).isoformat(),
            }
        )
    return pts


def test_insights_endpoint_detects_stop(client: TestClient):
    r = client.post(
        "/v1/insights",
        json={
            "points": _track_payload(),
            "options": {"stop_radius_m": 50, "min_stop_minutes": 5},
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["point_count"] == 630
    assert len(body["stops"]) >= 1


def test_insights_endpoint_accepts_pasted_text(client: TestClient):
    r = client.post(
        "/v1/insights",
        json={
            "text": "lat,lon\n12.97,77.59\n12.98,77.60\n",
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["accepted_point_count"] == 2


def test_plot_validate_echoes_clean_points(client: TestClient):
    r = client.post(
        "/v1/plot/validate",
        json={"text": "1,2\n3,4\n"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["accepted"] == 2
    assert body["rejected"] == 0
    assert body["points"][0]["lat"] == 1.0


def test_plot_validate_rejects_empty_request(client: TestClient):
    r = client.post("/v1/plot/validate", json={})
    assert r.status_code == 400


def test_insights_rejects_oversized_text(client: TestClient, monkeypatch):
    monkeypatch.setattr(settings, "MAX_PUBLIC_TEXT_BYTES", 10)

    r = client.post(
        "/v1/insights",
        json={"text": "lat,lon\n12.97,77.59\n"},
    )

    assert r.status_code == 413
    assert "too large" in r.text


def test_plot_validate_rejects_too_many_points(client: TestClient, monkeypatch):
    monkeypatch.setattr(settings, "MAX_PUBLIC_POINTS", 1)

    r = client.post(
        "/v1/plot/validate",
        json={
            "points": [
                {"lat": 12.97, "lon": 77.59},
                {"lat": 12.98, "lon": 77.60},
            ]
        },
    )

    assert r.status_code == 413
    assert "too many points" in r.text


def test_insights_rejects_geocoding_when_disabled(client: TestClient, monkeypatch):
    monkeypatch.setattr(settings, "ENABLE_REVERSE_GEOCODING", False)

    r = client.post(
        "/v1/insights",
        json={
            "points": [{"lat": 12.97, "lon": 77.59}],
            "options": {"geocode_stops": True},
        },
    )

    assert r.status_code == 403
    assert "Reverse geocoding is disabled" in r.text
