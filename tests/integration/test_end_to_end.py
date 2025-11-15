"""
End-to-end integration tests.

Tests complete workflows from data ingestion through to retrieval,
simulating real-world usage scenarios.
"""

import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
from bhulan.api.app import app
from bhulan.ingestion.files import ingest_file
from bhulan.storage.mongo_repo import MongoTrackPointRepository, MongoJobRegistry


TEST_DATA_DIR = Path(__file__).parent.parent / "data"


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
@pytest.mark.e2e
class TestCompleteIngestionWorkflow:
    """Test complete ingestion workflows."""
    
    def test_file_to_api_workflow(self, client, mongo_repo, job_registry):
        """Test ingesting file then querying via API."""
        csv_path = TEST_DATA_DIR / "golden_urban.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        file_result = ingest_file(
            str(csv_path),
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        assert file_result.accepted > 0
        
        job_response = client.get(f"/jobs/{file_result.ingest_id}")
        assert job_response.status_code == 200
        
        job_data = job_response.json()
        assert job_data['status'] == 'succeeded'
        assert job_data['stats']['accepted'] == file_result.accepted
        
        count = mongo_repo.count_by_ingest_id(file_result.ingest_id)
        assert count == file_result.accepted
    
    def test_webhook_to_query_workflow(self, client, mongo_repo):
        """Test ingesting via webhook then querying data."""
        payload = [
            {
                "device_id": "TRK-E2E-001",
                "timestamp": "2024-05-01T12:00:00Z",
                "lat": 37.7749,
                "lon": -122.4194,
                "speed_kph": 45.0
            },
            {
                "device_id": "TRK-E2E-001",
                "timestamp": "2024-05-01T12:01:00Z",
                "lat": 37.7750,
                "lon": -122.4195,
                "speed_kph": 48.0
            }
        ]
        
        ingest_response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=payload
        )
        
        assert ingest_response.status_code == 200
        ingest_data = ingest_response.json()
        assert ingest_data['accepted'] == 2
        
        job_response = client.get(f"/jobs/{ingest_data['ingest_id']}")
        assert job_response.status_code == 200
        
        device_id = "TRK-E2E-001"
        start_time = datetime(2024, 5, 1, 11, 59, 0)
        end_time = datetime(2024, 5, 1, 12, 2, 0)
        
        points = mongo_repo.get_by_device_and_time(device_id, start_time, end_time)
        assert len(points) == 2
        
        assert points[0]['ts_utc'] < points[1]['ts_utc']
    
    def test_multi_source_ingestion(self, client, mongo_repo, job_registry):
        """Test ingesting from multiple sources into same database."""
        csv_path = TEST_DATA_DIR / "golden_urban.csv"
        
        if csv_path.exists():
            file_result = ingest_file(
                str(csv_path),
                vendor='generic',
                repo=mongo_repo,
                job_registry=job_registry
            )
            file_count = file_result.accepted
        else:
            file_count = 0
        
        webhook_payload = {
            "device_id": "TRK-WEBHOOK-001",
            "timestamp": "2024-05-01T14:00:00Z",
            "lat": 40.7128,
            "lon": -74.0060
        }
        
        webhook_response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=webhook_payload
        )
        
        assert webhook_response.status_code == 200
        webhook_data = webhook_response.json()
        webhook_count = webhook_data['accepted']
        
        total_count = mongo_repo.collection.count_documents({})
        assert total_count == file_count + webhook_count
    
    def test_idempotent_replay(self, client, mongo_repo, job_registry):
        """Test that replaying same data doesn't create duplicates."""
        csv_path = TEST_DATA_DIR / "golden_urban.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        result1 = ingest_file(
            str(csv_path),
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        initial_count = result1.accepted
        
        result2 = ingest_file(
            str(csv_path),
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        total_count = mongo_repo.collection.count_documents({})
        assert total_count == initial_count


@pytest.mark.integration
@pytest.mark.e2e
class TestDataQualityWorkflow:
    """Test data quality validation workflows."""
    
    def test_quality_flagging_workflow(self, client, mongo_repo, job_registry):
        """Test that data quality issues are flagged correctly."""
        csv_path = TEST_DATA_DIR / "jitter_spikes.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        result = ingest_file(
            str(csv_path),
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        assert result.accepted > 0
        
        points = list(mongo_repo.collection.find({'ingest_id': result.ingest_id}))
        
        flagged_points = [
            p for p in points
            if 'raw' in p and 'meta' in p['raw'] and 'quality_flags' in p['raw']['meta']
        ]
        
        assert len(flagged_points) > 0
    
    def test_validation_error_reporting(self, client, mongo_repo):
        """Test that validation errors are properly reported."""
        payload = [
            {
                "device_id": "TRK-001",
                "timestamp": "2024-05-01T12:00:00Z",
                "lat": 37.7749,
                "lon": -122.4194
            },
            {
                "device_id": "TRK-002",
                "timestamp": "2024-05-01T12:00:00Z",
                "lat": 91.0,  # Invalid
                "lon": -122.4194
            },
            {
                "device_id": "TRK-003",
                "timestamp": "2024-05-01T12:00:00Z",
                "lat": 37.7749,
                "lon": 181.0  # Invalid
            },
            {
                "device_id": "TRK-004",
                "timestamp": "invalid-date",  # Invalid
                "lat": 37.7749,
                "lon": -122.4194
            }
        ]
        
        response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=payload
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data['accepted'] == 1
        assert data['rejected'] == 3
        assert len(data['errors']) == 3
        
        for idx, error in data['errors'].items():
            assert len(error) > 0  # Should have error message


@pytest.mark.integration
@pytest.mark.e2e
class TestVendorAdapterWorkflow:
    """Test vendor-specific ingestion workflows."""
    
    def test_geotab_workflow(self, client, mongo_repo):
        """Test complete Geotab ingestion workflow."""
        payload = [
            {
                "id": "GEOTAB-DEVICE-001",
                "dateTime": "2024-05-01T12:00:00Z",
                "latitude": 37.7749,
                "longitude": -122.4194,
                "speed": 45.0,  # km/h
                "bearing": 90
            },
            {
                "id": "GEOTAB-DEVICE-001",
                "dateTime": "2024-05-01T12:01:00Z",
                "latitude": 37.7750,
                "longitude": -122.4195,
                "speed": 48.0,
                "bearing": 92
            }
        ]
        
        response = client.post(
            "/ingest/trackpoints?vendor=geotab",
            json=payload
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data['accepted'] == 2
        
        points = list(mongo_repo.collection.find({'ingest_id': data['ingest_id']}))
        
        for point in points:
            assert point['device_id'] == "GEOTAB-DEVICE-001"
            assert point['src'] == 'geotab'
            if point.get('speed_mps'):
                assert 0 < point['speed_mps'] < 50  # Reasonable m/s range
    
    def test_samsara_workflow(self, client, mongo_repo):
        """Test complete Samsara ingestion workflow."""
        payload = {
            "deviceId": "SAMSARA-DEVICE-001",
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
        assert data['accepted'] == 1
        
        points = list(mongo_repo.collection.find({'ingest_id': data['ingest_id']}))
        
        assert len(points) == 1
        point = points[0]
        assert point['device_id'] == "SAMSARA-DEVICE-001"
        assert point['src'] == 'samsara'


@pytest.mark.integration
@pytest.mark.e2e
class TestPerformanceWorkflow:
    """Test performance-related workflows."""
    
    def test_large_batch_ingestion(self, client, mongo_repo):
        """Test ingesting a large batch of data."""
        payload = [
            {
                "device_id": f"TRK-{i % 10}",
                "timestamp": f"2024-05-01T12:{i // 60:02d}:{i % 60:02d}Z",
                "lat": 37.7749 + (i % 100) * 0.0001,
                "lon": -122.4194,
                "speed_kph": 40.0 + (i % 20)
            }
            for i in range(100)
        ]
        
        response = client.post(
            "/ingest/trackpoints?vendor=generic",
            json=payload
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data['accepted'] == 100
        assert data['rejected'] == 0
        
        count = mongo_repo.count_by_ingest_id(data['ingest_id'])
        assert count == 100
    
    def test_concurrent_device_ingestion(self, client, mongo_repo):
        """Test ingesting data from multiple devices concurrently."""
        for device_num in range(5):
            payload = [
                {
                    "device_id": f"TRK-CONCURRENT-{device_num:03d}",
                    "timestamp": f"2024-05-01T12:00:{i:02d}Z",
                    "lat": 37.7749 + device_num * 0.01,
                    "lon": -122.4194,
                    "speed_kph": 45.0
                }
                for i in range(10)
            ]
            
            response = client.post(
                "/ingest/trackpoints?vendor=generic",
                json=payload
            )
            
            assert response.status_code == 200
            data = response.json()
            assert data['accepted'] == 10
        
        total_count = mongo_repo.collection.count_documents({})
        assert total_count == 50  # 5 devices * 10 points each
