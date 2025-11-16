"""
Integration tests for speed smoothing with ingestion pipeline.

Tests the smoothing layer integrated with file ingestion and MongoDB storage.
"""

import pytest
from pathlib import Path
from datetime import datetime, timedelta
from bhulan.core.smoothing import SpeedSmoother, compute_speed_from_coordinates
from bhulan.storage.mongo_repo import MongoTrackPointRepository
from bhulan.models.canonical import TrackPoint


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


@pytest.mark.integration
class TestSmoothingWithStorage:
    """Test smoothing layer integrated with MongoDB storage."""
    
    def test_smooth_and_store_track(self, mongo_repo):
        """Test smoothing track points and storing to MongoDB."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        ingest_id = "test-smooth-001"
        
        points = []
        for i in range(20):
            point = TrackPoint(
                device_id="TRK-SMOOTH-001",
                ts_utc=base_time + timedelta(seconds=i),
                lat=37.7749 + i * 0.0001,
                lon=-122.4194,
                speed_mps=15.0 + (i % 3) * 2.0,  # Noisy speed
                ingest_id=ingest_id,
                seq_no=i
            )
            points.append(point)
        
        smoother = SpeedSmoother(method='kalman')
        
        point_dicts = [
            {
                'lat': p.lat,
                'lon': p.lon,
                'ts_utc': p.ts_utc,
                'speed_mps': p.speed_mps
            }
            for p in points
        ]
        
        smoothed_dicts = smoother.smooth(point_dicts, compute_missing=False)
        
        for i, point in enumerate(points):
            point.speed_mps = smoothed_dicts[i]['speed_mps']
            if point.raw is None:
                point.raw = {}
            point.raw['speed_original'] = smoothed_dicts[i]['speed_mps_original']
        
        count = mongo_repo.upsert_batch(points)
        assert count == 20
        
        stored_points = list(mongo_repo.collection.find({'ingest_id': ingest_id}))
        assert len(stored_points) == 20
        
        for sp in stored_points:
            assert 'speed_mps' in sp
            assert sp['speed_mps'] > 0
            assert 'raw' in sp
            assert 'speed_original' in sp['raw']
    
    def test_compute_and_smooth_missing_speeds(self, mongo_repo):
        """Test computing missing speeds and smoothing."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        ingest_id = "test-compute-smooth-001"
        
        points = []
        for i in range(15):
            point = TrackPoint(
                device_id="TRK-COMPUTE-001",
                ts_utc=base_time + timedelta(seconds=i),
                lat=37.7749 + i * 0.0001,
                lon=-122.4194,
                speed_mps=None,  # No speed
                ingest_id=ingest_id,
                seq_no=i
            )
            points.append(point)
        
        point_dicts = [
            {
                'lat': p.lat,
                'lon': p.lon,
                'ts_utc': p.ts_utc
            }
            for p in points
        ]
        
        smoother = SpeedSmoother(method='savgol')
        smoothed_dicts = smoother.smooth(point_dicts, compute_missing=True)
        
        for i, point in enumerate(points):
            point.speed_mps = smoothed_dicts[i]['speed_mps']
        
        count = mongo_repo.upsert_batch(points)
        assert count == 15
        
        stored_points = list(mongo_repo.collection.find({'ingest_id': ingest_id}))
        
        for sp in stored_points:
            assert 'speed_mps' in sp
            assert sp['speed_mps'] >= 0


@pytest.mark.integration
class TestSmoothingWithRealData:
    """Test smoothing with real test data files."""
    
    def test_smooth_golden_urban_data(self):
        """Test smoothing golden urban dataset."""
        import pandas as pd
        
        csv_path = TEST_DATA_DIR / "golden_urban.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        df = pd.read_csv(csv_path)
        
        points = []
        for _, row in df.iterrows():
            point = {
                'lat': row.get('latitude', row.get('lat')),
                'lon': row.get('longitude', row.get('lon')),
                'ts_utc': pd.to_datetime(row.get('timestamp', row.get('time'))),
            }
            
            if 'speed_kph' in row:
                point['speed_mps'] = row['speed_kph'] / 3.6
            elif 'speed_mph' in row:
                point['speed_mps'] = row['speed_mph'] * 0.44704
            
            points.append(point)
        
        smoother = SpeedSmoother(method='kalman')
        smoothed_points = smoother.smooth(points, compute_missing=True)
        
        assert len(smoothed_points) == len(points)
        
        for point in smoothed_points:
            assert 'speed_mps' in point
            assert point['speed_mps'] >= 0
    
    def test_smooth_jitter_spikes_data(self):
        """Test smoothing data with GPS jitter and speed spikes."""
        import pandas as pd
        
        csv_path = TEST_DATA_DIR / "jitter_spikes.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        df = pd.read_csv(csv_path)
        
        points = []
        for _, row in df.iterrows():
            point = {
                'lat': row.get('latitude', row.get('lat')),
                'lon': row.get('longitude', row.get('lon')),
                'ts_utc': pd.to_datetime(row.get('ts_utc', row.get('timestamp'))),
            }
            
            if 'speed_kph' in row:
                point['speed_mps'] = row['speed_kph'] / 3.6
            
            points.append(point)
        
        smoother = SpeedSmoother(
            method='savgol',
            savgol_window_length=5,
            savgol_polyorder=2
        )
        smoothed_points = smoother.smooth(points, compute_missing=True)
        
        assert len(smoothed_points) == len(points)
        
        original_speeds = [p['speed_mps_original'] for p in smoothed_points]
        smoothed_speeds = [p['speed_mps'] for p in smoothed_points]
        
        import numpy as np
        assert np.std(smoothed_speeds) < np.std(original_speeds)


@pytest.mark.integration
class TestSmoothingComparison:
    """Test comparing different smoothing methods."""
    
    def test_compare_kalman_vs_savgol(self):
        """Compare Kalman filter vs Savitzky-Golay filter."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        
        import numpy as np
        np.random.seed(42)
        
        points = []
        for i in range(30):
            true_speed = 15.0
            noisy_speed = true_speed + np.random.normal(0, 2.0)
            
            point = {
                'lat': 37.7749 + i * 0.0001,
                'lon': -122.4194,
                'ts_utc': base_time + timedelta(seconds=i),
                'speed_mps': noisy_speed
            }
            points.append(point)
        
        kalman_smoother = SpeedSmoother(method='kalman')
        kalman_smoothed = kalman_smoother.smooth(points.copy(), compute_missing=False)
        
        savgol_smoother = SpeedSmoother(method='savgol')
        savgol_smoothed = savgol_smoother.smooth(points.copy(), compute_missing=False)
        
        original_speeds = [p['speed_mps'] for p in points]
        kalman_speeds = [p['speed_mps'] for p in kalman_smoothed]
        savgol_speeds = [p['speed_mps'] for p in savgol_smoothed]
        
        original_std = np.std(original_speeds)
        kalman_std = np.std(kalman_speeds)
        savgol_std = np.std(savgol_speeds)
        
        assert kalman_std < original_std
        assert savgol_std < original_std
    
    def test_no_smoothing_preserves_data(self):
        """Test that 'none' method preserves original data."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        
        points = []
        for i in range(10):
            point = {
                'lat': 37.7749 + i * 0.0001,
                'lon': -122.4194,
                'ts_utc': base_time + timedelta(seconds=i),
                'speed_mps': 10.0 + i
            }
            points.append(point)
        
        smoother = SpeedSmoother(method='none')
        result = smoother.smooth(points, compute_missing=False)
        
        for i, point in enumerate(result):
            assert point['speed_mps'] == points[i]['speed_mps']


@pytest.mark.integration
class TestSmoothingEdgeCases:
    """Test smoothing with edge cases."""
    
    def test_smooth_single_point(self):
        """Test smoothing with single point."""
        point = {
            'lat': 37.7749,
            'lon': -122.4194,
            'ts_utc': datetime(2024, 5, 1, 12, 0, 0),
            'speed_mps': 15.0
        }
        
        smoother = SpeedSmoother(method='kalman')
        result = smoother.smooth([point], compute_missing=False)
        
        assert len(result) == 1
        assert 'speed_mps' in result[0]
    
    def test_smooth_two_points(self):
        """Test smoothing with two points."""
        points = [
            {
                'lat': 37.7749,
                'lon': -122.4194,
                'ts_utc': datetime(2024, 5, 1, 12, 0, 0),
                'speed_mps': 10.0
            },
            {
                'lat': 37.7750,
                'lon': -122.4195,
                'ts_utc': datetime(2024, 5, 1, 12, 0, 1),
                'speed_mps': 12.0
            }
        ]
        
        smoother = SpeedSmoother(method='savgol')
        result = smoother.smooth(points, compute_missing=False)
        
        assert len(result) == 2
        for point in result:
            assert 'speed_mps' in point
    
    def test_smooth_with_zero_speeds(self):
        """Test smoothing with zero speeds (stationary)."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        
        points = []
        for i in range(10):
            point = {
                'lat': 37.7749,  # Same location
                'lon': -122.4194,
                'ts_utc': base_time + timedelta(seconds=i),
                'speed_mps': 0.0
            }
            points.append(point)
        
        smoother = SpeedSmoother(method='kalman')
        result = smoother.smooth(points, compute_missing=False)
        
        for point in result:
            assert point['speed_mps'] < 1.0
    
    def test_smooth_with_high_variance(self):
        """Test smoothing with very high variance."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        
        import numpy as np
        np.random.seed(42)
        
        points = []
        for i in range(20):
            speed = 20.0 + np.random.normal(0, 10.0)
            speed = max(0, speed)  # Keep non-negative
            
            point = {
                'lat': 37.7749 + i * 0.0001,
                'lon': -122.4194,
                'ts_utc': base_time + timedelta(seconds=i),
                'speed_mps': speed
            }
            points.append(point)
        
        smoother = SpeedSmoother(
            method='kalman',
            kalman_measurement_variance=10.0  # Higher variance
        )
        result = smoother.smooth(points, compute_missing=False)
        
        original_speeds = [p['speed_mps'] for p in points]
        smoothed_speeds = [p['speed_mps'] for p in result]
        
        assert np.std(smoothed_speeds) < np.std(original_speeds) * 0.8
