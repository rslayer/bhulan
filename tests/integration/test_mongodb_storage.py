"""
Integration tests for MongoDB storage layer.

Tests the MongoDB repository implementation including CRUD operations,
indexing, deduplication, and geospatial queries.
"""

import pytest
from datetime import datetime, timedelta
from bhulan.storage.mongo_repo import MongoTrackPointRepository, MongoJobRegistry
from bhulan.models.canonical import TrackPoint


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


@pytest.fixture
def sample_trackpoint():
    """Create a sample track point for testing."""
    return TrackPoint(
        device_id="TRK-TEST-001",
        ts_utc=datetime(2024, 5, 1, 12, 0, 0),
        lat=37.7749,
        lon=-122.4194,
        speed_mps=15.0,
        heading_deg=90.0,
        alt_m=10.0,
        hdop=1.5,
        src="test",
        ingest_id="test-ingest-id",
        seq_no=0
    )


@pytest.mark.integration
class TestTrackPointRepository:
    """Test TrackPoint repository operations."""
    
    def test_upsert_single_point(self, mongo_repo, sample_trackpoint):
        """Test inserting a single track point."""
        count = mongo_repo.upsert_batch([sample_trackpoint])
        
        assert count == 1
        
        db_count = mongo_repo.collection.count_documents({})
        assert db_count == 1
    
    def test_upsert_multiple_points(self, mongo_repo):
        """Test inserting multiple track points."""
        points = [
            TrackPoint(
                device_id="TRK-001",
                ts_utc=datetime(2024, 5, 1, 12, i, 0),
                lat=37.7749 + i * 0.001,
                lon=-122.4194,
                ingest_id="test-batch",
                seq_no=i
            )
            for i in range(10)
        ]
        
        count = mongo_repo.upsert_batch(points)
        
        assert count == 10
        
        db_count = mongo_repo.collection.count_documents({})
        assert db_count == 10
    
    def test_deduplication(self, mongo_repo, sample_trackpoint):
        """Test that duplicate points are not inserted twice."""
        count1 = mongo_repo.upsert_batch([sample_trackpoint])
        assert count1 == 1
        
        count2 = mongo_repo.upsert_batch([sample_trackpoint])
        
        db_count = mongo_repo.collection.count_documents({})
        assert db_count == 1
    
    def test_exists_check(self, mongo_repo, sample_trackpoint):
        """Test checking if a point exists."""
        point_hash = sample_trackpoint.compute_hash()
        
        assert not mongo_repo.exists(point_hash)
        
        mongo_repo.upsert_batch([sample_trackpoint])
        
        assert mongo_repo.exists(point_hash)
    
    def test_count_by_ingest_id(self, mongo_repo):
        """Test counting points by ingest_id."""
        ingest_id = "test-ingest-123"
        
        points = [
            TrackPoint(
                device_id="TRK-001",
                ts_utc=datetime(2024, 5, 1, 12, i, 0),
                lat=37.7749,
                lon=-122.4194,
                ingest_id=ingest_id,
                seq_no=i
            )
            for i in range(5)
        ]
        
        mongo_repo.upsert_batch(points)
        
        count = mongo_repo.count_by_ingest_id(ingest_id)
        assert count == 5
    
    def test_get_by_device_and_time(self, mongo_repo):
        """Test retrieving points by device and time range."""
        device_id = "TRK-001"
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        
        points = [
            TrackPoint(
                device_id=device_id,
                ts_utc=base_time + timedelta(minutes=i),
                lat=37.7749,
                lon=-122.4194,
                ingest_id="test",
                seq_no=i
            )
            for i in range(10)
        ]
        
        mongo_repo.upsert_batch(points)
        
        start_time = base_time + timedelta(minutes=2)
        end_time = base_time + timedelta(minutes=7)
        
        results = mongo_repo.get_by_device_and_time(device_id, start_time, end_time)
        
        assert len(results) == 6  # Minutes 2-7 inclusive
        
        timestamps = [r['ts_utc'] for r in results]
        assert timestamps == sorted(timestamps)
    
    def test_geojson_location(self, mongo_repo, sample_trackpoint):
        """Test that GeoJSON location is created correctly."""
        mongo_repo.upsert_batch([sample_trackpoint])
        
        doc = mongo_repo.collection.find_one({'device_id': sample_trackpoint.device_id})
        
        assert 'loc' in doc
        assert doc['loc']['type'] == 'Point'
        assert doc['loc']['coordinates'] == [sample_trackpoint.lon, sample_trackpoint.lat]
    
    def test_indexes_created(self, mongo_repo):
        """Test that required indexes are created."""
        indexes = list(mongo_repo.collection.list_indexes())
        index_names = [idx['name'] for idx in indexes]
        
        
        assert any('_hash' in name for name in index_names)
        assert any('loc' in name for name in index_names)
        assert any('device_id' in name for name in index_names)
        assert any('ingest_id' in name for name in index_names)


@pytest.mark.integration
class TestJobRegistry:
    """Test job registry operations."""
    
    def test_create_job(self, job_registry):
        """Test creating a job record."""
        ingest_id = "test-job-123"
        
        job_registry.create_job(
            ingest_id=ingest_id,
            source="test",
            params={"test_param": "value"}
        )
        
        job = job_registry.get_job(ingest_id)
        
        assert job is not None
        assert job['ingest_id'] == ingest_id
        assert job['source'] == "test"
        assert job['status'] == 'running'
        assert 'started_at' in job
    
    def test_update_job_status(self, job_registry):
        """Test updating job status."""
        ingest_id = "test-job-456"
        
        job_registry.create_job(
            ingest_id=ingest_id,
            source="test",
            params={}
        )
        
        stats = {
            'read': 100,
            'accepted': 95,
            'rejected': 5
        }
        
        job_registry.update_job_status(
            ingest_id=ingest_id,
            status='succeeded',
            stats=stats,
            error_sample={0: "Sample error"}
        )
        
        job = job_registry.get_job(ingest_id)
        
        assert job['status'] == 'succeeded'
        assert job['stats']['accepted'] == 95
        assert job['stats']['rejected'] == 5
        assert 'finished_at' in job
        assert job['error_sample'] == {0: "Sample error"}
    
    def test_get_nonexistent_job(self, job_registry):
        """Test retrieving non-existent job."""
        job = job_registry.get_job("nonexistent-id")
        assert job is None
    
    def test_job_lifecycle(self, job_registry):
        """Test complete job lifecycle."""
        ingest_id = "test-lifecycle-789"
        
        job_registry.create_job(
            ingest_id=ingest_id,
            source="file",
            params={"file_path": "/test/data.csv"}
        )
        
        job = job_registry.get_job(ingest_id)
        assert job['status'] == 'running'
        assert job['finished_at'] is None
        
        job_registry.update_job_status(
            ingest_id=ingest_id,
            status='succeeded',
            stats={'read': 50, 'accepted': 50, 'rejected': 0}
        )
        
        job = job_registry.get_job(ingest_id)
        assert job['status'] == 'succeeded'
        assert job['finished_at'] is not None
        assert job['stats']['accepted'] == 50


@pytest.mark.integration
class TestGeospatialQueries:
    """Test geospatial query capabilities."""
    
    def test_near_query(self, mongo_repo):
        """Test finding points near a location."""
        base_lat = 37.7749
        base_lon = -122.4194
        
        points = []
        for i in range(-2, 3):
            for j in range(-2, 3):
                point = TrackPoint(
                    device_id=f"TRK-{i}-{j}",
                    ts_utc=datetime(2024, 5, 1, 12, 0, 0),
                    lat=base_lat + i * 0.01,
                    lon=base_lon + j * 0.01,
                    ingest_id="test-geo"
                )
                points.append(point)
        
        mongo_repo.upsert_batch(points)
        
        query = {
            'loc': {
                '$near': {
                    '$geometry': {
                        'type': 'Point',
                        'coordinates': [base_lon, base_lat]
                    },
                    '$maxDistance': 1500  # meters
                }
            }
        }
        
        results = list(mongo_repo.collection.find(query))
        
        assert len(results) > 0
        assert len(results) <= 25  # All points in grid
    
    def test_bounding_box_query(self, mongo_repo):
        """Test finding points within a bounding box."""
        points = [
            TrackPoint(
                device_id="TRK-SF",
                ts_utc=datetime(2024, 5, 1, 12, 0, 0),
                lat=37.7749,
                lon=-122.4194,
                ingest_id="test-bbox"
            ),
            TrackPoint(
                device_id="TRK-LA",
                ts_utc=datetime(2024, 5, 1, 12, 0, 0),
                lat=34.0522,
                lon=-118.2437,
                ingest_id="test-bbox"
            ),
            TrackPoint(
                device_id="TRK-NY",
                ts_utc=datetime(2024, 5, 1, 12, 0, 0),
                lat=40.7128,
                lon=-74.0060,
                ingest_id="test-bbox"
            )
        ]
        
        mongo_repo.upsert_batch(points)
        
        query = {
            'loc': {
                '$geoWithin': {
                    '$box': [
                        [-125, 32],  # Southwest corner
                        [-114, 42]   # Northeast corner
                    ]
                }
            }
        }
        
        results = list(mongo_repo.collection.find(query))
        
        assert len(results) == 2
        device_ids = [r['device_id'] for r in results]
        assert 'TRK-SF' in device_ids
        assert 'TRK-LA' in device_ids
        assert 'TRK-NY' not in device_ids


@pytest.mark.integration
class TestBatchOperations:
    """Test batch operation performance and correctness."""
    
    def test_large_batch_insert(self, mongo_repo):
        """Test inserting a large batch of points."""
        points = [
            TrackPoint(
                device_id=f"TRK-{i % 10}",
                ts_utc=datetime(2024, 5, 1, 12, 0, 0) + timedelta(seconds=i),
                lat=37.7749 + (i % 100) * 0.0001,
                lon=-122.4194,
                ingest_id="test-large-batch",
                seq_no=i
            )
            for i in range(1000)
        ]
        
        count = mongo_repo.upsert_batch(points)
        
        assert count == 1000
        
        db_count = mongo_repo.collection.count_documents({})
        assert db_count == 1000
    
    def test_batch_with_duplicates(self, mongo_repo):
        """Test batch insert with some duplicates."""
        points = []
        for i in range(10):
            point = TrackPoint(
                device_id="TRK-001",
                ts_utc=datetime(2024, 5, 1, 12, 0, 0),
                lat=37.7749,
                lon=-122.4194,
                ingest_id="test-dup-batch",
                seq_no=i
            )
            points.append(point)
        
        count = mongo_repo.upsert_batch(points)
        
        db_count = mongo_repo.collection.count_documents({})
        assert db_count == 1
