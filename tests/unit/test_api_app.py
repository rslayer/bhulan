"""Focused tests for FastAPI app-level legacy ingestion helpers."""

import json

from fastapi.testclient import TestClient

import bhulan.api.app as app_module
from bhulan.config.settings import settings


class _FakeMongoClient:
    def server_info(self):
        return {"ok": 1}


class _FakeDatabase:
    client = _FakeMongoClient()


class _FakeCollection:
    database = _FakeDatabase()


class _FakeTrackRepo:
    collection = _FakeCollection()

    def __init__(self):
        self.points = []

    def upsert_batch(self, points):
        self.points.extend(points)
        return len(points)

    def count_by_ingest_id(self, ingest_id):
        return sum(1 for p in self.points if p.ingest_id == ingest_id)


class _FakeJobRegistry:
    def __init__(self):
        self.jobs = {}

    def create_job(self, ingest_id, source, params):
        self.jobs[ingest_id] = {
            "ingest_id": ingest_id,
            "source": source,
            "params": params,
            "status": "running",
            "stats": {},
            "error_sample": {},
        }

    def update_job_status(self, ingest_id, status, stats=None, error_sample=None):
        self.jobs[ingest_id].update(
            {
                "status": status,
                "stats": stats or {},
                "error_sample": error_sample or {},
            }
        )

    def get_job(self, ingest_id):
        return self.jobs.get(ingest_id)


def _install_fakes(monkeypatch):
    repo = _FakeTrackRepo()
    jobs = _FakeJobRegistry()
    monkeypatch.setattr(app_module, "track_repo", repo)
    monkeypatch.setattr(app_module, "job_registry", jobs)
    monkeypatch.setattr(settings, "API_KEY", None)
    monkeypatch.setattr(settings, "MAX_BATCH_SIZE", 1000)
    return repo, jobs


def test_custom_mapping_header_is_applied(monkeypatch):
    repo, _jobs = _install_fakes(monkeypatch)
    client = TestClient(app_module.app)

    mapping = {
        "field_map": {
            "vehicle": "device_id",
            "when": "ts_utc",
            "y": "lat",
            "x": "lon",
        },
        "vendor": "custom-feed",
    }
    response = client.post(
        "/ingest/trackpoints",
        json={"vehicle": "V-1", "when": "2024-05-01T12:00:00Z", "y": 1, "x": 2},
        headers={"X-Bhulan-Mapping": json.dumps(mapping)},
    )

    assert response.status_code == 200
    assert response.json()["accepted"] == 1
    assert repo.points[0].device_id == "V-1"
    assert repo.points[0].src == "custom-feed"


def test_invalid_custom_mapping_returns_400(monkeypatch):
    _install_fakes(monkeypatch)
    client = TestClient(app_module.app)

    response = client.post(
        "/ingest/trackpoints",
        json={"device_id": "V-1", "timestamp": "2024-05-01T12:00:00Z", "lat": 1, "lon": 2},
        headers={"X-Bhulan-Mapping": "not json"},
    )

    assert response.status_code == 400


def test_ingest_respects_max_batch_size(monkeypatch):
    _install_fakes(monkeypatch)
    monkeypatch.setattr(settings, "MAX_BATCH_SIZE", 1)
    client = TestClient(app_module.app)

    response = client.post(
        "/ingest/trackpoints",
        json=[
            {"device_id": "V-1", "timestamp": "2024-05-01T12:00:00Z", "lat": 1, "lon": 2},
            {"device_id": "V-1", "timestamp": "2024-05-01T12:01:00Z", "lat": 1, "lon": 2},
        ],
    )

    assert response.status_code == 413


def test_legacy_read_endpoints_use_api_key_when_configured(monkeypatch):
    _install_fakes(monkeypatch)
    monkeypatch.setattr(settings, "API_KEY", "secret")
    client = TestClient(app_module.app)

    assert client.get("/config").status_code == 401
    assert client.get("/metrics").status_code == 401
    assert client.get("/jobs/missing").status_code == 401
    assert client.get("/config", headers={"X-API-Key": "secret"}).status_code == 200


def test_capabilities_reflect_public_demo_mode(monkeypatch):
    monkeypatch.setattr(settings, "BHULAN_AUTH_ENABLED", False)
    client = TestClient(app_module.app)

    response = client.get("/v1/capabilities")

    assert response.status_code == 200
    assert response.json() == {
        "auth_enabled": False,
        "history_enabled": False,
        "reverse_geocoding_enabled": False,
        "public_demo": True,
    }


def test_capabilities_reflect_auth_enabled(monkeypatch):
    monkeypatch.setattr(settings, "BHULAN_AUTH_ENABLED", True)
    client = TestClient(app_module.app)

    response = client.get("/v1/capabilities")

    assert response.status_code == 200
    assert response.json() == {
        "auth_enabled": True,
        "history_enabled": True,
        "reverse_geocoding_enabled": False,
        "public_demo": False,
    }


def test_metrics_are_prometheus_text(monkeypatch):
    _install_fakes(monkeypatch)
    client = TestClient(app_module.app)

    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "bhulan_up 1" in response.text
    assert "bhulan_mongo_available 1" in response.text
