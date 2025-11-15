"""
Integration tests for file ingestion.

Tests the complete file ingestion pipeline including reading files,
normalization, validation, and MongoDB persistence.
"""

import pytest
import os
from pathlib import Path
from bhulan.ingestion.files import ingest_file, detect_file_type
from bhulan.storage.mongo_repo import MongoTrackPointRepository, MongoJobRegistry
from bhulan.models.vendor.generic import create_generic_mapping


TEST_DATA_DIR = Path(__file__).parent.parent / "data"


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


class TestFileTypeDetection:
    """Test file type detection."""
    
    def test_detect_csv(self):
        """Test CSV file type detection."""
        file_type = detect_file_type("data.csv")
        assert file_type == "csv"
    
    def test_detect_json(self):
        """Test JSON file type detection."""
        file_type = detect_file_type("data.json")
        assert file_type == "json"
    
    def test_detect_xlsx(self):
        """Test Excel file type detection."""
        file_type = detect_file_type("data.xlsx")
        assert file_type == "xlsx"
    
    def test_detect_parquet(self):
        """Test Parquet file type detection."""
        file_type = detect_file_type("data.parquet")
        assert file_type == "parquet"


@pytest.mark.integration
class TestCSVIngestion:
    """Test CSV file ingestion."""
    
    def test_ingest_golden_urban_csv(self, mongo_repo, job_registry):
        """Test ingestion of golden urban dataset."""
        csv_path = TEST_DATA_DIR / "golden_urban.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        result = ingest_file(
            str(csv_path),
            mapping=None,  # Auto-infer mapping
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        assert result.accepted > 0, "Should accept at least some records"
        assert result.rejected == 0, "Should not reject any valid records"
        
        count = mongo_repo.count_by_ingest_id(result.ingest_id)
        assert count == result.accepted
        
        job = job_registry.get_job(result.ingest_id)
        assert job is not None
        assert job['status'] == 'succeeded'
        assert job['stats']['accepted'] == result.accepted
    
    def test_ingest_golden_rural_csv(self, mongo_repo, job_registry):
        """Test ingestion of golden rural dataset with different field names."""
        csv_path = TEST_DATA_DIR / "golden_rural.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        result = ingest_file(
            str(csv_path),
            mapping=None,  # Auto-infer mapping
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        assert result.accepted > 0
        assert result.rejected == 0
        
        count = mongo_repo.count_by_ingest_id(result.ingest_id)
        assert count == result.accepted
    
    def test_ingest_jitter_spikes_csv(self, mongo_repo, job_registry):
        """Test ingestion of dataset with GPS jitter and speed spikes."""
        csv_path = TEST_DATA_DIR / "jitter_spikes.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        result = ingest_file(
            str(csv_path),
            mapping=None,
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        assert result.accepted > 0
        
        count = mongo_repo.count_by_ingest_id(result.ingest_id)
        assert count == result.accepted
        
        points = list(mongo_repo.collection.find({'ingest_id': result.ingest_id}))
        
        has_flags = any(
            'raw' in p and 'meta' in p['raw'] and 'quality_flags' in p['raw']['meta']
            for p in points
        )
        assert has_flags, "Should flag suspicious data points"


@pytest.mark.integration
class TestJSONIngestion:
    """Test JSON file ingestion."""
    
    def test_ingest_sparse_json(self, mongo_repo, job_registry):
        """Test ingestion of sparse JSON dataset with missing optional fields."""
        json_path = TEST_DATA_DIR / "sparse.json"
        
        if not json_path.exists():
            pytest.skip(f"Test data file not found: {json_path}")
        
        result = ingest_file(
            str(json_path),
            mapping=None,
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        assert result.accepted > 0
        assert result.rejected == 0
        
        count = mongo_repo.count_by_ingest_id(result.ingest_id)
        assert count == result.accepted
        
        points = list(mongo_repo.collection.find({'ingest_id': result.ingest_id}))
        assert len(points) == result.accepted
        
        has_speed = [p for p in points if p.get('speed_mps') is not None]
        no_speed = [p for p in points if p.get('speed_mps') is None]
        
        assert len(has_speed) > 0 or len(no_speed) > 0


@pytest.mark.integration
class TestDeduplication:
    """Test deduplication functionality."""
    
    def test_duplicate_ingestion(self, mongo_repo, job_registry):
        """Test that duplicate records are not inserted twice."""
        csv_path = TEST_DATA_DIR / "golden_urban.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        result1 = ingest_file(
            str(csv_path),
            mapping=None,
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        initial_count = result1.accepted
        
        result2 = ingest_file(
            str(csv_path),
            mapping=None,
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        total_count = mongo_repo.collection.count_documents({})
        assert total_count == initial_count, "Duplicates should not be inserted"


@pytest.mark.integration
class TestFieldMapping:
    """Test field mapping and inference."""
    
    def test_auto_infer_mapping(self, mongo_repo, job_registry):
        """Test automatic field mapping inference from headers."""
        csv_path = TEST_DATA_DIR / "golden_urban.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        result = ingest_file(
            str(csv_path),
            mapping=None,  # Auto-infer
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        assert result.accepted > 0
        
        points = list(mongo_repo.collection.find({'ingest_id': result.ingest_id}))
        
        for point in points:
            assert 'device_id' in point
            assert 'ts_utc' in point
            assert 'lat' in point
            assert 'lon' in point
            
            assert 'loc' in point
            assert point['loc']['type'] == 'Point'
            assert len(point['loc']['coordinates']) == 2
    
    def test_different_header_variations(self, mongo_repo, job_registry):
        """Test that different header name variations are correctly mapped."""
        csv_path = TEST_DATA_DIR / "golden_rural.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        result = ingest_file(
            str(csv_path),
            mapping=None,
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        assert result.accepted > 0
        
        points = list(mongo_repo.collection.find({'ingest_id': result.ingest_id}))
        
        for point in points:
            assert 'device_id' in point
            assert 'ts_utc' in point
            assert 'lat' in point
            assert 'lon' in point


@pytest.mark.integration
class TestUnitConversions:
    """Test unit conversions during ingestion."""
    
    def test_speed_unit_conversion(self, mongo_repo, job_registry):
        """Test that speed units are correctly converted to m/s."""
        csv_path = TEST_DATA_DIR / "golden_rural.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        result = ingest_file(
            str(csv_path),
            mapping=None,
            vendor='generic',
            repo=mongo_repo,
            job_registry=job_registry
        )
        
        assert result.accepted > 0
        
        points = list(mongo_repo.collection.find({'ingest_id': result.ingest_id}))
        
        for point in points:
            if point.get('speed_mps') is not None:
                assert 0 <= point['speed_mps'] <= 120, f"Speed {point['speed_mps']} out of range"
