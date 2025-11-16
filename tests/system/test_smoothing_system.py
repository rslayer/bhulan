"""
System tests for speed smoothing feature.

Tests end-to-end smoothing workflows with real GPS data files,
MongoDB storage, and various smoothing configurations.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from bhulan.core.smoothing import SpeedSmoother, haversine_distance
from bhulan.storage.mongo_repo import MongoTrackPointRepository, JobRegistry
from bhulan.models.canonical import TrackPoint


TEST_DATA_DIR = Path(__file__).parent.parent / "data"


@pytest.fixture
def mongo_repo():
    """Create MongoDB repository for testing."""
    try:
        repo = MongoTrackPointRepository(
            mongo_uri="mongodb://localhost:27017",
            db_name="bhulan_system_test"
        )
        repo.create_indexes()
        yield repo
        repo.collection.drop()
    except Exception as e:
        pytest.skip(f"MongoDB not available: {e}")


@pytest.fixture
def job_registry():
    """Create job registry for testing."""
    try:
        registry = JobRegistry(
            mongo_uri="mongodb://localhost:27017",
            db_name="bhulan_system_test"
        )
        yield registry
        registry.collection.drop()
    except Exception as e:
        pytest.skip(f"MongoDB not available: {e}")


@pytest.mark.system
class TestSmoothingEndToEnd:
    """End-to-end system tests for smoothing feature."""
    
    def test_smooth_golden_urban_complete_workflow(self, mongo_repo, job_registry):
        """Test complete workflow: load CSV -> compute speeds -> smooth -> store -> retrieve."""
        csv_path = TEST_DATA_DIR / "golden_urban.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        df = pd.read_csv(csv_path)
        ingest_id = "system-test-urban-001"
        
        points = []
        for idx, row in df.iterrows():
            point = TrackPoint(
                device_id=row.get('device', row.get('device_id', 'TRK-URBAN-001')),
                ts_utc=pd.to_datetime(row.get('timestamp', row.get('time'))),
                lat=row.get('latitude', row.get('lat')),
                lon=row.get('longitude', row.get('lon')),
                speed_mps=None,  # Will compute from coordinates
                ingest_id=ingest_id,
                seq_no=idx
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
        
        smoother = SpeedSmoother(method='kalman')
        smoothed_dicts = smoother.smooth(point_dicts, compute_missing=True)
        
        for i, point in enumerate(points):
            point.speed_mps = smoothed_dicts[i]['speed_mps']
            if point.raw is None:
                point.raw = {}
            point.raw['speed_original'] = smoothed_dicts[i]['speed_mps_original']
            point.raw['smoothing_method'] = 'kalman'
        
        count = mongo_repo.upsert_batch(points)
        assert count == len(points)
        
        job_registry.create_job(
            ingest_id=ingest_id,
            source='file',
            params={'file': str(csv_path), 'smoothing': 'kalman'}
        )
        job_registry.update_job(
            ingest_id=ingest_id,
            status='succeeded',
            stats={'read': len(points), 'accepted': len(points), 'rejected': 0}
        )
        
        stored_points = list(mongo_repo.collection.find({'ingest_id': ingest_id}))
        assert len(stored_points) == len(points)
        
        for sp in stored_points:
            assert 'speed_mps' in sp
            assert sp['speed_mps'] >= 0
            assert 'raw' in sp
            assert 'speed_original' in sp['raw']
            assert 'smoothing_method' in sp['raw']
        
        job = job_registry.get_job(ingest_id)
        assert job['status'] == 'succeeded'
        assert job['stats']['accepted'] == len(points)
    
    def test_smooth_jitter_spikes_savgol_workflow(self, mongo_repo):
        """Test smoothing GPS data with jitter and spikes using Savitzky-Golay filter."""
        csv_path = TEST_DATA_DIR / "jitter_spikes.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        df = pd.read_csv(csv_path)
        ingest_id = "system-test-jitter-001"
        
        points = []
        for idx, row in df.iterrows():
            speed_mps = None
            if 'speed_kph' in row:
                speed_mps = row['speed_kph'] / 3.6
            elif 'speed_mph' in row:
                speed_mps = row['speed_mph'] * 0.44704
            
            point = TrackPoint(
                device_id=row.get('device', 'TRK-JITTER-001'),
                ts_utc=pd.to_datetime(row.get('ts_utc', row.get('timestamp'))),
                lat=row.get('latitude', row.get('lat')),
                lon=row.get('longitude', row.get('lon')),
                speed_mps=speed_mps,
                ingest_id=ingest_id,
                seq_no=idx
            )
            points.append(point)
        
        point_dicts = [
            {
                'lat': p.lat,
                'lon': p.lon,
                'ts_utc': p.ts_utc,
                'speed_mps': p.speed_mps
            }
            for p in points
        ]
        
        smoother = SpeedSmoother(
            method='savgol',
            savgol_window_length=7,
            savgol_polyorder=2
        )
        smoothed_dicts = smoother.smooth(point_dicts, compute_missing=True)
        
        for i, point in enumerate(points):
            point.speed_mps = smoothed_dicts[i]['speed_mps']
            if point.raw is None:
                point.raw = {}
            point.raw['speed_original'] = smoothed_dicts[i]['speed_mps_original']
            point.raw['smoothing_method'] = 'savgol'
        
        count = mongo_repo.upsert_batch(points)
        assert count == len(points)
        
        original_speeds = [p.raw['speed_original'] for p in points]
        smoothed_speeds = [p.speed_mps for p in points]
        
        assert np.std(smoothed_speeds) < np.std(original_speeds)
    
    def test_compare_smoothing_methods_on_real_data(self, mongo_repo):
        """Compare Kalman vs Savitzky-Golay on real GPS data."""
        csv_path = TEST_DATA_DIR / "golden_rural.csv"
        
        if not csv_path.exists():
            pytest.skip(f"Test data file not found: {csv_path}")
        
        df = pd.read_csv(csv_path)
        
        point_dicts = []
        for _, row in df.iterrows():
            point = {
                'lat': row.get('latitude', row.get('lat')),
                'lon': row.get('longitude', row.get('lon')),
                'ts_utc': pd.to_datetime(row.get('timestamp', row.get('time')))
            }
            
            if 'speed_kph' in row:
                point['speed_mps'] = row['speed_kph'] / 3.6
            
            point_dicts.append(point)
        
        kalman_smoother = SpeedSmoother(method='kalman')
        kalman_result = kalman_smoother.smooth(point_dicts.copy(), compute_missing=True)
        
        savgol_smoother = SpeedSmoother(method='savgol')
        savgol_result = savgol_smoother.smooth(point_dicts.copy(), compute_missing=True)
        
        kalman_points = []
        for i, pd_dict in enumerate(kalman_result):
            point = TrackPoint(
                device_id='TRK-RURAL-KALMAN',
                ts_utc=pd_dict['ts_utc'],
                lat=pd_dict['lat'],
                lon=pd_dict['lon'],
                speed_mps=pd_dict['speed_mps'],
                ingest_id='system-test-kalman-001',
                seq_no=i
            )
            kalman_points.append(point)
        
        savgol_points = []
        for i, pd_dict in enumerate(savgol_result):
            point = TrackPoint(
                device_id='TRK-RURAL-SAVGOL',
                ts_utc=pd_dict['ts_utc'],
                lat=pd_dict['lat'],
                lon=pd_dict['lon'],
                speed_mps=pd_dict['speed_mps'],
                ingest_id='system-test-savgol-001',
                seq_no=i
            )
            savgol_points.append(point)
        
        kalman_count = mongo_repo.upsert_batch(kalman_points)
        savgol_count = mongo_repo.upsert_batch(savgol_points)
        
        assert kalman_count == len(kalman_points)
        assert savgol_count == len(savgol_points)
        
        original_speeds = [p['speed_mps_original'] for p in kalman_result]
        kalman_speeds = [p['speed_mps'] for p in kalman_result]
        savgol_speeds = [p['speed_mps'] for p in savgol_result]
        
        assert np.std(kalman_speeds) < np.std(original_speeds)
        assert np.std(savgol_speeds) < np.std(original_speeds)


@pytest.mark.system
class TestSmoothingWithMissingData:
    """System tests for smoothing with missing speed data."""
    
    def test_compute_speeds_from_coordinates_only(self, mongo_repo):
        """Test computing speeds from coordinates when no speed data available."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        base_lat = 37.7749
        base_lon = -122.4194
        
        points = []
        for i in range(50):
            lat_offset = i * 0.000135  # ~15m in latitude
            
            point = TrackPoint(
                device_id='TRK-NO-SPEED-001',
                ts_utc=base_time + timedelta(seconds=i),
                lat=base_lat + lat_offset,
                lon=base_lon,
                speed_mps=None,  # No speed data
                ingest_id='system-test-no-speed-001',
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
        
        smoother = SpeedSmoother(method='kalman')
        smoothed_dicts = smoother.smooth(point_dicts, compute_missing=True)
        
        for i, point in enumerate(points):
            point.speed_mps = smoothed_dicts[i]['speed_mps']
        
        count = mongo_repo.upsert_batch(points)
        assert count == len(points)
        
        stored_points = list(mongo_repo.collection.find({'ingest_id': 'system-test-no-speed-001'}))
        
        for sp in stored_points:
            assert 'speed_mps' in sp
            assert sp['speed_mps'] >= 0
        
        speeds = [sp['speed_mps'] for sp in stored_points[1:]]  # Skip first point
        avg_speed = np.mean(speeds)
        assert 10 < avg_speed < 20  # Should be around 15 m/s
    
    def test_mixed_missing_speeds(self, mongo_repo):
        """Test smoothing with some speeds present and some missing."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        
        points = []
        for i in range(30):
            point = TrackPoint(
                device_id='TRK-MIXED-001',
                ts_utc=base_time + timedelta(seconds=i),
                lat=37.7749 + i * 0.0001,
                lon=-122.4194,
                speed_mps=15.0 if i % 3 == 0 else None,  # Every 3rd point has speed
                ingest_id='system-test-mixed-001',
                seq_no=i
            )
            points.append(point)
        
        point_dicts = [
            {
                'lat': p.lat,
                'lon': p.lon,
                'ts_utc': p.ts_utc,
                'speed_mps': p.speed_mps
            }
            for p in points
        ]
        
        smoother = SpeedSmoother(method='savgol')
        smoothed_dicts = smoother.smooth(point_dicts, compute_missing=True)
        
        for i, point in enumerate(points):
            point.speed_mps = smoothed_dicts[i]['speed_mps']
        
        count = mongo_repo.upsert_batch(points)
        assert count == len(points)
        
        stored_points = list(mongo_repo.collection.find({'ingest_id': 'system-test-mixed-001'}))
        assert len(stored_points) == 30
        
        for sp in stored_points:
            assert 'speed_mps' in sp
            assert sp['speed_mps'] >= 0


@pytest.mark.system
class TestSmoothingPerformance:
    """System tests for smoothing performance with large datasets."""
    
    def test_smooth_large_dataset(self, mongo_repo):
        """Test smoothing performance with large GPS dataset (1000+ points)."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        
        num_points = 1000
        points = []
        
        np.random.seed(42)
        for i in range(num_points):
            true_speed = 20.0  # m/s
            noisy_speed = true_speed + np.random.normal(0, 3.0)
            
            point = TrackPoint(
                device_id='TRK-LARGE-001',
                ts_utc=base_time + timedelta(seconds=i),
                lat=37.7749 + i * 0.0002,
                lon=-122.4194,
                speed_mps=max(0, noisy_speed),
                ingest_id='system-test-large-001',
                seq_no=i
            )
            points.append(point)
        
        point_dicts = [
            {
                'lat': p.lat,
                'lon': p.lon,
                'ts_utc': p.ts_utc,
                'speed_mps': p.speed_mps
            }
            for p in points
        ]
        
        import time
        start_time = time.time()
        
        smoother = SpeedSmoother(method='kalman')
        smoothed_dicts = smoother.smooth(point_dicts, compute_missing=False)
        
        smoothing_time = time.time() - start_time
        
        for i, point in enumerate(points):
            point.speed_mps = smoothed_dicts[i]['speed_mps']
        
        store_start = time.time()
        count = mongo_repo.upsert_batch(points)
        store_time = time.time() - store_start
        
        assert count == num_points
        
        assert smoothing_time < 5.0  # Should complete in < 5 seconds
        assert store_time < 10.0  # Storage should complete in < 10 seconds
        
        original_speeds = [p['speed_mps_original'] for p in smoothed_dicts]
        smoothed_speeds = [p['speed_mps'] for p in smoothed_dicts]
        
        variance_reduction = 1 - (np.std(smoothed_speeds) / np.std(original_speeds))
        assert variance_reduction > 0.3  # At least 30% variance reduction


@pytest.mark.system
class TestSmoothingEdgeCases:
    """System tests for smoothing edge cases."""
    
    def test_smooth_sparse_data(self, mongo_repo):
        """Test smoothing with sparse GPS data (large time gaps)."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        
        points = []
        time_gaps = [0, 10, 25, 45, 70, 100, 135, 175, 220, 270]  # Irregular gaps
        
        for i, gap in enumerate(time_gaps):
            point = TrackPoint(
                device_id='TRK-SPARSE-001',
                ts_utc=base_time + timedelta(seconds=gap),
                lat=37.7749 + i * 0.001,
                lon=-122.4194,
                speed_mps=None,
                ingest_id='system-test-sparse-001',
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
        
        smoother = SpeedSmoother(method='kalman')
        smoothed_dicts = smoother.smooth(point_dicts, compute_missing=True)
        
        for i, point in enumerate(points):
            point.speed_mps = smoothed_dicts[i]['speed_mps']
        
        count = mongo_repo.upsert_batch(points)
        assert count == len(points)
        
        stored_points = list(mongo_repo.collection.find({'ingest_id': 'system-test-sparse-001'}))
        assert len(stored_points) == len(points)
        
        for sp in stored_points:
            assert 'speed_mps' in sp
            assert sp['speed_mps'] >= 0
    
    def test_smooth_stationary_vehicle(self, mongo_repo):
        """Test smoothing with stationary vehicle (zero speeds)."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        
        points = []
        for i in range(20):
            point = TrackPoint(
                device_id='TRK-STATIONARY-001',
                ts_utc=base_time + timedelta(seconds=i),
                lat=37.7749,  # Same location
                lon=-122.4194,
                speed_mps=0.0,
                ingest_id='system-test-stationary-001',
                seq_no=i
            )
            points.append(point)
        
        point_dicts = [
            {
                'lat': p.lat,
                'lon': p.lon,
                'ts_utc': p.ts_utc,
                'speed_mps': p.speed_mps
            }
            for p in points
        ]
        
        smoother = SpeedSmoother(method='kalman')
        smoothed_dicts = smoother.smooth(point_dicts, compute_missing=False)
        
        for i, point in enumerate(points):
            point.speed_mps = smoothed_dicts[i]['speed_mps']
        
        count = mongo_repo.upsert_batch(points)
        assert count == len(points)
        
        stored_points = list(mongo_repo.collection.find({'ingest_id': 'system-test-stationary-001'}))
        
        for sp in stored_points:
            assert sp['speed_mps'] < 1.0  # Should remain near zero
    
    def test_smooth_high_speed_variation(self, mongo_repo):
        """Test smoothing with high speed variation (acceleration/deceleration)."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        
        points = []
        speeds = []
        
        for i in range(10):
            speeds.append(i * 3.0)
        
        for i in range(10):
            speeds.append(30.0)
        
        for i in range(10):
            speeds.append(30.0 - i * 3.0)
        
        for i, speed in enumerate(speeds):
            point = TrackPoint(
                device_id='TRK-ACCEL-001',
                ts_utc=base_time + timedelta(seconds=i),
                lat=37.7749 + i * 0.0002,
                lon=-122.4194,
                speed_mps=speed,
                ingest_id='system-test-accel-001',
                seq_no=i
            )
            points.append(point)
        
        point_dicts = [
            {
                'lat': p.lat,
                'lon': p.lon,
                'ts_utc': p.ts_utc,
                'speed_mps': p.speed_mps
            }
            for p in points
        ]
        
        smoother = SpeedSmoother(method='savgol', savgol_window_length=5)
        smoothed_dicts = smoother.smooth(point_dicts, compute_missing=False)
        
        for i, point in enumerate(points):
            point.speed_mps = smoothed_dicts[i]['speed_mps']
        
        count = mongo_repo.upsert_batch(points)
        assert count == len(points)
        
        stored_points = list(mongo_repo.collection.find(
            {'ingest_id': 'system-test-accel-001'}
        ).sort('seq_no', 1))
        
        smoothed_speeds = [sp['speed_mps'] for sp in stored_points]
        
        for i in range(1, 10):
            assert smoothed_speeds[i] >= smoothed_speeds[i-1] - 1.0  # Allow small deviations
        
        for i in range(21, 29):
            assert smoothed_speeds[i] <= smoothed_speeds[i-1] + 1.0  # Allow small deviations
