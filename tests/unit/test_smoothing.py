"""
Unit tests for speed smoothing module.

Tests speed computation, Kalman filter, Savitzky-Golay filter, and
the configurable smoothing layer.
"""

import pytest
import numpy as np
from datetime import datetime, timedelta
from bhulan.core.smoothing import (
    haversine_distance,
    compute_speed_from_coordinates,
    KalmanFilter,
    savitzky_golay_filter,
    SpeedSmoother
)


class TestHaversineDistance:
    """Test haversine distance calculation."""
    
    def test_zero_distance(self):
        """Test distance between same point is zero."""
        distance = haversine_distance(37.7749, -122.4194, 37.7749, -122.4194)
        assert distance == 0.0
    
    def test_known_distance(self):
        """Test distance calculation with known values."""
        sf_lat, sf_lon = 37.7749, -122.4194
        la_lat, la_lon = 34.0522, -118.2437
        
        distance = haversine_distance(sf_lat, sf_lon, la_lat, la_lon)
        
        expected = 559000
        assert abs(distance - expected) / expected < 0.01
    
    def test_short_distance(self):
        """Test short distance calculation (1 km)."""
        lat1, lon1 = 37.7749, -122.4194
        lat2, lon2 = 37.7749 + 0.009, -122.4194  # ~1 km north
        
        distance = haversine_distance(lat1, lon1, lat2, lon2)
        
        assert 950 < distance < 1050
    
    def test_antipodal_points(self):
        """Test distance between antipodal points (opposite sides of Earth)."""
        distance = haversine_distance(90, 0, -90, 0)
        
        expected = 20000000
        assert abs(distance - expected) / expected < 0.01


class TestSpeedComputation:
    """Test speed computation from GPS coordinates."""
    
    def test_compute_speed_empty_list(self):
        """Test speed computation with empty list."""
        speeds = compute_speed_from_coordinates([])
        assert speeds == []
    
    def test_compute_speed_single_point(self):
        """Test speed computation with single point."""
        points = [
            {
                'lat': 37.7749,
                'lon': -122.4194,
                'ts_utc': datetime(2024, 5, 1, 12, 0, 0)
            }
        ]
        
        speeds = compute_speed_from_coordinates(points)
        assert len(speeds) == 1
        assert speeds[0] == 0.0  # First point has no previous point
    
    def test_compute_speed_two_points(self):
        """Test speed computation between two points."""
        points = [
            {
                'lat': 37.7749,
                'lon': -122.4194,
                'ts_utc': datetime(2024, 5, 1, 12, 0, 0)
            },
            {
                'lat': 37.7749 + 0.001,  # ~111 meters north
                'lon': -122.4194,
                'ts_utc': datetime(2024, 5, 1, 12, 0, 10)  # 10 seconds later
            }
        ]
        
        speeds = compute_speed_from_coordinates(points)
        
        assert len(speeds) == 2
        assert speeds[0] == 0.0
        assert 10 < speeds[1] < 12
    
    def test_compute_speed_constant_velocity(self):
        """Test speed computation for constant velocity movement."""
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        
        points = []
        for i in range(10):
            points.append({
                'lat': 37.7749 + i * 0.0001,  # Move north
                'lon': -122.4194,
                'ts_utc': base_time + timedelta(seconds=i)
            })
        
        speeds = compute_speed_from_coordinates(points)
        
        assert len(speeds) == 10
        assert speeds[0] == 0.0
        
        for speed in speeds[1:]:
            assert 8 < speed < 13  # Allow some variation due to haversine
    
    def test_compute_speed_zero_time_delta(self):
        """Test speed computation with zero time delta."""
        points = [
            {
                'lat': 37.7749,
                'lon': -122.4194,
                'ts_utc': datetime(2024, 5, 1, 12, 0, 0)
            },
            {
                'lat': 37.7750,
                'lon': -122.4195,
                'ts_utc': datetime(2024, 5, 1, 12, 0, 0)  # Same time
            }
        ]
        
        speeds = compute_speed_from_coordinates(points)
        
        assert len(speeds) == 2
        assert speeds[1] == 0.0  # Zero time delta should give zero speed


class TestKalmanFilter:
    """Test Kalman filter implementation."""
    
    def test_kalman_filter_initialization(self):
        """Test Kalman filter initialization."""
        kf = KalmanFilter(
            process_variance=1e-5,
            measurement_variance=1e-1,
            initial_estimate=10.0,
            initial_error=1.0
        )
        
        assert kf.process_variance == 1e-5
        assert kf.measurement_variance == 1e-1
        assert kf.estimate == 10.0
        assert kf.error_covariance == 1.0
    
    def test_kalman_filter_single_update(self):
        """Test single Kalman filter update."""
        kf = KalmanFilter(initial_estimate=0.0)
        
        filtered = kf.update(10.0)
        
        assert 0.0 < filtered < 10.0
    
    def test_kalman_filter_constant_signal(self):
        """Test Kalman filter with constant signal."""
        kf = KalmanFilter()
        
        measurements = [15.0] * 10
        filtered = kf.filter_series(measurements)
        
        assert len(filtered) == 10
        
        assert abs(filtered[-1] - 15.0) < 0.1
    
    def test_kalman_filter_noisy_signal(self):
        """Test Kalman filter with noisy signal."""
        kf = KalmanFilter(
            process_variance=1e-5,
            measurement_variance=1.0
        )
        
        np.random.seed(42)
        true_value = 20.0
        noise = np.random.normal(0, 1, 20)
        measurements = [true_value + n for n in noise]
        
        filtered = kf.filter_series(measurements)
        
        assert len(filtered) == 20
        
        assert abs(filtered[-1] - true_value) < abs(measurements[-1] - true_value)
    
    def test_kalman_filter_step_change(self):
        """Test Kalman filter response to step change."""
        kf = KalmanFilter()
        
        measurements = [10.0] * 10 + [20.0] * 10
        filtered = kf.filter_series(measurements)
        
        assert len(filtered) == 20
        
        assert filtered[0] < 12.0  # Start near 10
        assert filtered[-1] > 14.0  # Moving toward 20 (conservative tracking)
        assert filtered[-1] > filtered[9]  # Should be increasing after step
    
    def test_kalman_filter_empty_series(self):
        """Test Kalman filter with empty series."""
        kf = KalmanFilter()
        filtered = kf.filter_series([])
        assert filtered == []


class TestSavitzkyGolayFilter:
    """Test Savitzky-Golay filter implementation."""
    
    def test_savgol_filter_basic(self):
        """Test basic Savitzky-Golay filtering."""
        speeds = [10.0, 12.0, 11.0, 13.0, 12.0, 14.0, 13.0]
        
        smoothed = savitzky_golay_filter(speeds, window_length=5, polyorder=2)
        
        assert len(smoothed) == len(speeds)
        
        original_std = np.std(speeds)
        smoothed_std = np.std(smoothed)
        assert smoothed_std <= original_std
    
    def test_savgol_filter_noisy_signal(self):
        """Test Savitzky-Golay filter with noisy signal."""
        np.random.seed(42)
        
        t = np.linspace(0, 10, 50)
        true_signal = 10 + 5 * np.sin(t)
        
        noise = np.random.normal(0, 1, 50)
        noisy_signal = true_signal + noise
        
        smoothed = savitzky_golay_filter(noisy_signal.tolist(), window_length=11, polyorder=3)
        
        noisy_error = np.mean((noisy_signal - true_signal) ** 2)
        smoothed_error = np.mean((np.array(smoothed) - true_signal) ** 2)
        
        assert smoothed_error < noisy_error
    
    def test_savgol_filter_short_series(self):
        """Test Savitzky-Golay filter with series shorter than window."""
        speeds = [10.0, 12.0, 11.0]
        
        smoothed = savitzky_golay_filter(speeds, window_length=7, polyorder=2)
        
        assert smoothed == speeds
    
    def test_savgol_filter_even_window(self):
        """Test Savitzky-Golay filter with even window length."""
        speeds = [10.0, 12.0, 11.0, 13.0, 12.0, 14.0, 13.0, 15.0]
        
        smoothed = savitzky_golay_filter(speeds, window_length=4, polyorder=2)
        
        assert len(smoothed) == len(speeds)
    
    def test_savgol_filter_preserves_trends(self):
        """Test that Savitzky-Golay filter preserves trends."""
        speeds = list(range(10, 30))
        
        smoothed = savitzky_golay_filter(speeds, window_length=5, polyorder=2)
        
        assert len(smoothed) == len(speeds)
        
        for i in range(1, len(smoothed)):
            assert smoothed[i] >= smoothed[i-1] - 0.1  # Allow small deviations


class TestSpeedSmoother:
    """Test configurable speed smoother."""
    
    def test_speed_smoother_initialization(self):
        """Test speed smoother initialization."""
        smoother = SpeedSmoother(method='kalman')
        assert smoother.method == 'kalman'
        
        smoother = SpeedSmoother(method='savgol')
        assert smoother.method == 'savgol'
        
        smoother = SpeedSmoother(method='none')
        assert smoother.method == 'none'
    
    def test_speed_smoother_invalid_method(self):
        """Test speed smoother with invalid method."""
        with pytest.raises(ValueError, match="Unknown smoothing method"):
            SpeedSmoother(method='invalid')
    
    def test_speed_smoother_kalman(self):
        """Test speed smoother with Kalman filter."""
        smoother = SpeedSmoother(method='kalman')
        
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        points = [
            {
                'lat': 37.7749 + i * 0.0001,
                'lon': -122.4194,
                'ts_utc': base_time + timedelta(seconds=i),
                'speed_mps': 10.0 + np.random.normal(0, 2)
            }
            for i in range(20)
        ]
        
        smoothed_points = smoother.smooth(points, compute_missing=False)
        
        assert len(smoothed_points) == len(points)
        
        for point in smoothed_points:
            assert 'speed_mps' in point
            assert 'speed_mps_original' in point
        
        original_speeds = [p['speed_mps_original'] for p in smoothed_points]
        smoothed_speeds = [p['speed_mps'] for p in smoothed_points]
        
        assert np.std(smoothed_speeds) < np.std(original_speeds)
    
    def test_speed_smoother_savgol(self):
        """Test speed smoother with Savitzky-Golay filter."""
        smoother = SpeedSmoother(
            method='savgol',
            savgol_window_length=7,
            savgol_polyorder=2
        )
        
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        points = [
            {
                'lat': 37.7749 + i * 0.0001,
                'lon': -122.4194,
                'ts_utc': base_time + timedelta(seconds=i),
                'speed_mps': 15.0 + np.random.normal(0, 1)
            }
            for i in range(20)
        ]
        
        smoothed_points = smoother.smooth(points, compute_missing=False)
        
        assert len(smoothed_points) == len(points)
        
        original_speeds = [p['speed_mps_original'] for p in smoothed_points]
        smoothed_speeds = [p['speed_mps'] for p in smoothed_points]
        
        assert np.std(smoothed_speeds) < np.std(original_speeds)
    
    def test_speed_smoother_none(self):
        """Test speed smoother with no smoothing."""
        smoother = SpeedSmoother(method='none')
        
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        points = [
            {
                'lat': 37.7749 + i * 0.0001,
                'lon': -122.4194,
                'ts_utc': base_time + timedelta(seconds=i),
                'speed_mps': 10.0 + i
            }
            for i in range(10)
        ]
        
        smoothed_points = smoother.smooth(points, compute_missing=False)
        
        for i, point in enumerate(smoothed_points):
            assert point['speed_mps'] == point['speed_mps_original']
    
    def test_speed_smoother_compute_missing(self):
        """Test speed smoother with missing speed computation."""
        smoother = SpeedSmoother(method='kalman')
        
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        points = [
            {
                'lat': 37.7749 + i * 0.0001,
                'lon': -122.4194,
                'ts_utc': base_time + timedelta(seconds=i)
            }
            for i in range(10)
        ]
        
        smoothed_points = smoother.smooth(points, compute_missing=True)
        
        assert len(smoothed_points) == len(points)
        
        for point in smoothed_points:
            assert 'speed_mps' in point
            assert point['speed_mps'] >= 0
    
    def test_speed_smoother_mixed_missing(self):
        """Test speed smoother with some missing speeds."""
        smoother = SpeedSmoother(method='kalman')
        
        base_time = datetime(2024, 5, 1, 12, 0, 0)
        points = []
        
        for i in range(10):
            point = {
                'lat': 37.7749 + i * 0.0001,
                'lon': -122.4194,
                'ts_utc': base_time + timedelta(seconds=i)
            }
            
            if i % 2 == 0:
                point['speed_mps'] = 12.0
            
            points.append(point)
        
        smoothed_points = smoother.smooth(points, compute_missing=True)
        
        assert len(smoothed_points) == len(points)
        
        for point in smoothed_points:
            assert 'speed_mps' in point
    
    def test_speed_smoother_empty_points(self):
        """Test speed smoother with empty points list."""
        smoother = SpeedSmoother(method='kalman')
        
        smoothed_points = smoother.smooth([])
        assert smoothed_points == []
    
    def test_smooth_speed_series_directly(self):
        """Test smoothing speed series directly."""
        smoother = SpeedSmoother(method='kalman')
        
        speeds = [10.0 + np.random.normal(0, 2) for _ in range(20)]
        smoothed = smoother.smooth_speed_series(speeds)
        
        assert len(smoothed) == len(speeds)
        assert np.std(smoothed) < np.std(speeds)
