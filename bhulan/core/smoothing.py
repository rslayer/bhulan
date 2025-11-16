"""
Speed smoothing layer for GPS track data.

Provides configurable smoothing algorithms including Kalman filter and
Savitzky-Golay filter for GPS speed time-series data.
"""

import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from scipy.signal import savgol_filter
from scipy.spatial.distance import euclidean


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points on Earth.
    
    Uses the Haversine formula to compute distance in meters.
    
    Args:
        lat1: Latitude of first point in decimal degrees
        lon1: Longitude of first point in decimal degrees
        lat2: Latitude of second point in decimal degrees
        lon2: Longitude of second point in decimal degrees
        
    Returns:
        Distance in meters
    """
    R = 6371000
    
    lat1_rad = np.radians(lat1)
    lat2_rad = np.radians(lat2)
    delta_lat = np.radians(lat2 - lat1)
    delta_lon = np.radians(lon2 - lon1)
    
    a = np.sin(delta_lat / 2) ** 2 + \
        np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
    distance = R * c
    return distance


def compute_speed_from_coordinates(
    points: List[Dict[str, Any]]
) -> List[float]:
    """
    Compute speed from GPS coordinates using haversine distance.
    
    Speed is computed as distance / time_delta between consecutive points.
    First point gets speed of 0.
    
    Args:
        points: List of track points with 'lat', 'lon', 'ts_utc' fields
        
    Returns:
        List of computed speeds in meters per second
    """
    if not points:
        return []
    
    speeds = [0.0]  # First point has no previous point
    
    for i in range(1, len(points)):
        prev_point = points[i - 1]
        curr_point = points[i]
        
        distance = haversine_distance(
            prev_point['lat'], prev_point['lon'],
            curr_point['lat'], curr_point['lon']
        )
        
        if isinstance(prev_point['ts_utc'], datetime):
            time_delta = (curr_point['ts_utc'] - prev_point['ts_utc']).total_seconds()
        else:
            time_delta = curr_point['ts_utc'] - prev_point['ts_utc']
        
        if time_delta > 0:
            speed = distance / time_delta
        else:
            speed = 0.0
        
        speeds.append(speed)
    
    return speeds


class KalmanFilter:
    """
    1D Kalman filter for speed smoothing.
    
    Implements a simple Kalman filter for smoothing noisy speed measurements.
    """
    
    def __init__(
        self,
        process_variance: float = 1e-5,
        measurement_variance: float = 1e-1,
        initial_estimate: float = 0.0,
        initial_error: float = 1.0
    ):
        """
        Initialize Kalman filter.
        
        Args:
            process_variance: Process noise variance (Q)
            measurement_variance: Measurement noise variance (R)
            initial_estimate: Initial state estimate
            initial_error: Initial estimation error covariance
        """
        self.process_variance = process_variance
        self.measurement_variance = measurement_variance
        self.estimate = initial_estimate
        self.error_covariance = initial_error
    
    def update(self, measurement: float) -> float:
        """
        Update filter with new measurement.
        
        Args:
            measurement: New speed measurement
            
        Returns:
            Filtered speed estimate
        """
        predicted_estimate = self.estimate
        predicted_error = self.error_covariance + self.process_variance
        
        kalman_gain = predicted_error / (predicted_error + self.measurement_variance)
        self.estimate = predicted_estimate + kalman_gain * (measurement - predicted_estimate)
        self.error_covariance = (1 - kalman_gain) * predicted_error
        
        return self.estimate
    
    def filter_series(self, measurements: List[float]) -> List[float]:
        """
        Filter entire time series.
        
        Args:
            measurements: List of speed measurements
            
        Returns:
            List of filtered speed estimates
        """
        if not measurements:
            return []
        
        self.estimate = measurements[0] if measurements else 0.0
        self.error_covariance = 1.0
        
        filtered = []
        for measurement in measurements:
            filtered.append(self.update(measurement))
        
        return filtered


def savitzky_golay_filter(
    speeds: List[float],
    window_length: int = 5,
    polyorder: int = 2
) -> List[float]:
    """
    Apply Savitzky-Golay filter to speed time series.
    
    The Savitzky-Golay filter is a digital filter that can be applied to a set
    of data points for smoothing. It fits successive sub-sets of adjacent data
    points with a low-degree polynomial by the method of linear least squares.
    
    Args:
        speeds: List of speed measurements
        window_length: Length of the filter window (must be odd and > polyorder)
        polyorder: Order of the polynomial used to fit the samples
        
    Returns:
        List of smoothed speeds
    """
    if len(speeds) < window_length:
        return speeds
    
    if window_length % 2 == 0:
        window_length += 1
    
    if window_length <= polyorder:
        window_length = polyorder + 2
        if window_length % 2 == 0:
            window_length += 1
    
    speeds_array = np.array(speeds)
    smoothed = savgol_filter(speeds_array, window_length, polyorder)
    
    return smoothed.tolist()


class SpeedSmoother:
    """
    Configurable speed smoothing layer.
    
    Supports multiple smoothing algorithms:
    - Kalman filter
    - Savitzky-Golay filter
    - None (no smoothing)
    """
    
    def __init__(
        self,
        method: str = 'kalman',
        kalman_process_variance: float = 1e-5,
        kalman_measurement_variance: float = 1e-1,
        savgol_window_length: int = 5,
        savgol_polyorder: int = 2
    ):
        """
        Initialize speed smoother.
        
        Args:
            method: Smoothing method ('kalman', 'savgol', or 'none')
            kalman_process_variance: Process variance for Kalman filter
            kalman_measurement_variance: Measurement variance for Kalman filter
            savgol_window_length: Window length for Savitzky-Golay filter
            savgol_polyorder: Polynomial order for Savitzky-Golay filter
        """
        self.method = method.lower()
        
        if self.method not in ['kalman', 'savgol', 'none']:
            raise ValueError(f"Unknown smoothing method: {method}")
        
        self.kalman_process_variance = kalman_process_variance
        self.kalman_measurement_variance = kalman_measurement_variance
        
        self.savgol_window_length = savgol_window_length
        self.savgol_polyorder = savgol_polyorder
    
    def smooth(
        self,
        points: List[Dict[str, Any]],
        compute_missing: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Smooth speed time series for track points.
        
        Args:
            points: List of track points with 'lat', 'lon', 'ts_utc', and optionally 'speed_mps'
            compute_missing: If True, compute speed from coordinates when missing
            
        Returns:
            List of track points with smoothed 'speed_mps' field
        """
        if not points:
            return []
        
        speeds = []
        for i, point in enumerate(points):
            if 'speed_mps' in point and point['speed_mps'] is not None:
                speeds.append(point['speed_mps'])
            elif compute_missing and i > 0:
                prev_point = points[i - 1]
                distance = haversine_distance(
                    prev_point['lat'], prev_point['lon'],
                    point['lat'], point['lon']
                )
                
                if isinstance(prev_point['ts_utc'], datetime):
                    time_delta = (point['ts_utc'] - prev_point['ts_utc']).total_seconds()
                else:
                    time_delta = point['ts_utc'] - prev_point['ts_utc']
                
                speed = distance / time_delta if time_delta > 0 else 0.0
                speeds.append(speed)
            else:
                speeds.append(0.0)
        
        if self.method == 'kalman':
            kalman = KalmanFilter(
                process_variance=self.kalman_process_variance,
                measurement_variance=self.kalman_measurement_variance
            )
            smoothed_speeds = kalman.filter_series(speeds)
        elif self.method == 'savgol':
            smoothed_speeds = savitzky_golay_filter(
                speeds,
                window_length=self.savgol_window_length,
                polyorder=self.savgol_polyorder
            )
        else:  # 'none'
            smoothed_speeds = speeds
        
        result = []
        for i, point in enumerate(points):
            smoothed_point = point.copy()
            smoothed_point['speed_mps'] = smoothed_speeds[i]
            smoothed_point['speed_mps_original'] = speeds[i]
            result.append(smoothed_point)
        
        return result
    
    def smooth_speed_series(self, speeds: List[float]) -> List[float]:
        """
        Smooth a speed time series directly.
        
        Args:
            speeds: List of speed measurements in m/s
            
        Returns:
            List of smoothed speeds
        """
        if not speeds:
            return []
        
        if self.method == 'kalman':
            kalman = KalmanFilter(
                process_variance=self.kalman_process_variance,
                measurement_variance=self.kalman_measurement_variance
            )
            return kalman.filter_series(speeds)
        elif self.method == 'savgol':
            return savitzky_golay_filter(
                speeds,
                window_length=self.savgol_window_length,
                polyorder=self.savgol_polyorder
            )
        else:  # 'none'
            return speeds
