"""
Integration tests for webhook API.

Tests the FastAPI webhook endpoints including ingestion, job status,
and health checks with real MongoDB interactions.
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime
from bhulan.api.app import app
from bhulan.storage.mongo_repo import MongoTrackPointRepository, MongoJobRegistry
from bhulan.config.settings import settings


@pytest.fixture
def client():
    """Create test client for FastAPI app."""
    return TestClient(app)


@pytest.fixture
def mongo_repo():
    """Create MongoDB repository for testing."""
    repo = MongoTrackPointRepository(
        mongo_uri="mongodb://localhost:27017",
        db_name="bhulan_test"
    )
    repo.create_indexes()
    yield repo
    repo.collection.drop()


@pytest.fixture
def job_registry():
    """Create job registry for testing."""
    registry = MongoJobRegistry(
        mongo_uri="mongodb://localhost:27017",
        db_name="bhulan_test"
    )
    yield registry
    registry.collection.drop()


@pytest.mark.integration
class TestHealthEndpoints:
    """Test health check endpoints."""
    
    def test_health_ready(self, client):
        """Test health check endpoint."""
        response = client.get("/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ready"
    
    def test_config_endpoint(self, client):
        """Test configuration endpoint."""
        response = client.get("/config")
        assert response.status_code == 200
        data = response.json()
        assert "mongo_db_name" in data
        assert "max_batch_size" in data


@pytest.mark.integration
class TestWebhookIngestion:
    """Test webhook ingestion endpoints."""
    
    def test_ingest_single_trackpoint(self, client, mongo_repo):
        """Test ingesting a single track point."""
        payload = {
            "device_id": "TRK-101",
            "timestamp": "2024-05-01T12:00:00Z",
            "lat": 37.7749,
            "lon": -122.4194,
            "speed_kph": 45.0,
            "heading": 90
        }
        
        response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=payload
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["accepted"] == 1
        assert data["rejected"] == 0
        assert "ingest_id" in data
        
        count = mongo_repo.count_by_ingest_id(data["ingest_id"])
        assert count == 1
    
    def test_ingest_multiple_trackpoints(self, client, mongo_repo):
        """Test ingesting multiple track points."""
        payload = [
            {
                "device_id": "TRK-101",
                "timestamp": "2024-05-01T12:00:00Z",
                "lat": 37.7749,
                "lon": -122.4194
            },
            {
                "device_id": "TRK-101",
                "timestamp": "2024-05-01T12:01:00Z",
                "lat": 37.7750,
                "lon": -122.4195
            },
            {
                "device_id": "TRK-101",
                "timestamp": "2024-05-01T12:02:00Z",
                "lat": 37.7751,
                "lon": -122.4196
            }
        ]
        
        response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=payload
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["accepted"] == 3
        assert data["rejected"] == 0
        
        count = mongo_repo.count_by_ingest_id(data["ingest_id"])
        assert count == 3
    
    def test_ingest_with_invalid_data(self, client, mongo_repo):
        """Test ingesting data with some invalid records."""
        payload = [
            {
                "device_id": "TRK-101",
                "timestamp": "2024-05-01T12:00:00Z",
                "lat": 37.7749,
                "lon": -122.4194
            },
            {
                "device_id": "TRK-101",
                "timestamp": "2024-05-01T12:01:00Z",
                "lat": 91.0,  # Invalid latitude
                "lon": -122.4195
            },
            {
                "device_id": "TRK-101",
                "timestamp": "2024-05-01T12:02:00Z",
                "lat": 37.7751,
                "lon": -122.4196
            }
        ]
        
        response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=payload
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["accepted"] == 2
        assert data["rejected"] == 1
        assert len(data["errors"]) == 1
        
        count = mongo_repo.count_by_ingest_id(data["ingest_id"])
        assert count == 2
    
    def test_ingest_with_custom_ingest_id(self, client, mongo_repo):
        """Test ingesting with custom ingest_id."""
        custom_id = "custom-test-id-123"
        
        payload = {
            "device_id": "TRK-101",
            "timestamp": "2024-05-01T12:00:00Z",
            "lat": 37.7749,
            "lon": -122.4194
        }
        
        response = client.post(
            f"/ingest/trackpoints?vendor=generic&ingest_id={custom_id}",
            json=payload
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["ingest_id"] == custom_id
        assert data["accepted"] == 1
    
    def test_ingest_geotab_vendor(self, client, mongo_repo):
        """Test ingesting with Geotab vendor adapter."""
        payload = {
            "id": "GEOTAB-001",
            "dateTime": "2024-05-01T12:00:00Z",
            "latitude": 37.7749,
            "longitude": -122.4194,
            "speed": 45.0,  # km/h
            "bearing": 90
        }
        
        response = client.post(
            "/ingest/trackpoints?vendor=geotab",
            json=payload
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["accepted"] == 1
        assert data["rejected"] == 0
        
        points = list(mongo_repo.collection.find({'ingest_id': data["ingest_id"]}))
        assert len(points) == 1
        assert points[0]['device_id'] == "GEOTAB-001"
        assert points[0]['src'] == 'geotab'
    
    def test_ingest_samsara_vendor(self, client, mongo_repo):
        """Test ingesting with Samsara vendor adapter."""
        payload = {
            "deviceId": "SAMSARA-001",
            "timestamp": 1714568400000,  # epoch milliseconds
            "latitude": 37.7749,
            "longitude": -122.4194,
            "speedKph": 45.0
        }
        
        response = client.post(
            "/ingest/trackpoints?vendor=samsara",
            json=payload
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["accepted"] == 1
        assert data["rejected"] == 0
        
        points = list(mongo_repo.collection.find({'ingest_id': data["ingest_id"]}))
        assert len(points) == 1
        assert points[0]['device_id'] == "SAMSARA-001"
        assert points[0]['src'] == 'samsara'


@pytest.mark.integration
class TestJobStatusEndpoint:
    """Test job status endpoint."""
    
    def test_get_job_status(self, client, mongo_repo, job_registry):
        """Test retrieving job status."""
        payload = {
            "device_id": "TRK-101",
            "timestamp": "2024-05-01T12:00:00Z",
            "lat": 37.7749,
            "lon": -122.4194
        }
        
        ingest_response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=payload
        )
        
        ingest_data = ingest_response.json()
        ingest_id = ingest_data["ingest_id"]
        
        status_response = client.get(f"/jobs/{ingest_id}")
        
        assert status_response.status_code == 200
        job_data = status_response.json()
        
        assert job_data["ingest_id"] == ingest_id
        assert job_data["source"] == "webhook"
        assert job_data["status"] == "succeeded"
        assert job_data["stats"]["accepted"] == 1
        assert job_data["stats"]["rejected"] == 0
        assert "point_count_in_db" in job_data
    
    def test_get_nonexistent_job(self, client):
        """Test retrieving non-existent job."""
        response = client.get("/jobs/nonexistent-id")
        assert response.status_code == 404


@pytest.mark.integration
class TestAPIAuthentication:
    """Test API authentication if configured."""
    
    def test_ingest_without_api_key_when_required(self, client, monkeypatch):
        """Test that requests without API key are rejected when required."""
        monkeypatch.setattr(settings, "API_KEY", "test-secret-key")
        
        payload = {
            "device_id": "TRK-101",
            "timestamp": "2024-05-01T12:00:00Z",
            "lat": 37.7749,
            "lon": -122.4194
        }
        
        response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=payload
        )
        
        assert response.status_code == 401
    
    def test_ingest_with_valid_api_key(self, client, mongo_repo, monkeypatch):
        """Test that requests with valid API key are accepted."""
        api_key = "test-secret-key"
        monkeypatch.setattr(settings, "API_KEY", api_key)
        
        payload = {
            "device_id": "TRK-101",
            "timestamp": "2024-05-01T12:00:00Z",
            "lat": 37.7749,
            "lon": -122.4194
        }
        
        response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=payload,
            headers={"X-API-Key": api_key}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["accepted"] == 1


@pytest.mark.integration
class TestDataPersistence:
    """Test data persistence and retrieval."""
    
    def test_geojson_location_created(self, client, mongo_repo):
        """Test that GeoJSON location is created for geospatial queries."""
        payload = {
            "device_id": "TRK-101",
            "timestamp": "2024-05-01T12:00:00Z",
            "lat": 37.7749,
            "lon": -122.4194
        }
        
        response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=payload
        )
        
        data = response.json()
        
        points = list(mongo_repo.collection.find({'ingest_id': data["ingest_id"]}))
        assert len(points) == 1
        
        point = points[0]
        assert 'loc' in point
        assert point['loc']['type'] == 'Point'
        assert point['loc']['coordinates'] == [-122.4194, 37.7749]  # [lon, lat]
    
    def test_raw_data_preserved(self, client, mongo_repo):
        """Test that original raw data is preserved."""
        payload = {
            "device_id": "TRK-101",
            "timestamp": "2024-05-01T12:00:00Z",
            "lat": 37.7749,
            "lon": -122.4194,
            "custom_field": "custom_value"
        }
        
        response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=payload
        )
        
        data = response.json()
        
        points = list(mongo_repo.collection.find({'ingest_id': data["ingest_id"]}))
        assert len(points) == 1
        
        point = points[0]
        assert 'raw' in point
        assert 'original' in point['raw']
        assert point['raw']['original']['custom_field'] == 'custom_value'
