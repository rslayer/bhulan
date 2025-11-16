# Speed Smoothing Layer

## Overview

The speed smoothing layer provides configurable algorithms for smoothing GPS speed time-series data. It supports automatic speed computation from GPS coordinates when speed data is missing, and offers two smoothing algorithms: Kalman filter and Savitzky-Golay filter.

## Features

- **Speed Computation**: Automatically compute speed from GPS coordinates using haversine distance when speed data is missing
- **Kalman Filter**: Optimal recursive filter for real-time smoothing with configurable process and measurement variance
- **Savitzky-Golay Filter**: Polynomial smoothing filter that preserves trends and features
- **Configurable**: Easy switching between smoothing methods with tunable parameters
- **Preserves Original Data**: Stores both smoothed and original speeds for comparison

## Usage

### Basic Usage

```python
from bhulan.core.smoothing import SpeedSmoother
from datetime import datetime, timedelta

# Create track points
points = [
    {
        'lat': 37.7749 + i * 0.0001,
        'lon': -122.4194,
        'ts_utc': datetime(2024, 5, 1, 12, 0, i),
        'speed_mps': 15.0 + random.uniform(-2, 2)  # Noisy speed
    }
    for i in range(20)
]

# Create smoother with Kalman filter
smoother = SpeedSmoother(method='kalman')

# Apply smoothing
smoothed_points = smoother.smooth(points, compute_missing=False)

# Access smoothed speeds
for point in smoothed_points:
    print(f"Original: {point['speed_mps_original']:.2f} m/s")
    print(f"Smoothed: {point['speed_mps']:.2f} m/s")
```

### Computing Missing Speeds

```python
# Track points without speed data
points = [
    {
        'lat': 37.7749 + i * 0.0001,
        'lon': -122.4194,
        'ts_utc': datetime(2024, 5, 1, 12, 0, i)
        # No speed_mps field
    }
    for i in range(20)
]

# Compute speeds from coordinates and smooth
smoother = SpeedSmoother(method='kalman')
smoothed_points = smoother.smooth(points, compute_missing=True)

# All points now have computed and smoothed speeds
```

### Using Savitzky-Golay Filter

```python
# Create smoother with Savitzky-Golay filter
smoother = SpeedSmoother(
    method='savgol',
    savgol_window_length=7,  # Window size (must be odd)
    savgol_polyorder=2       # Polynomial order
)

smoothed_points = smoother.smooth(points, compute_missing=True)
```

### Smoothing Speed Series Directly

```python
# If you already have a speed time series
speeds = [10.5, 12.3, 11.8, 13.2, 12.1, 14.5, 13.8]

smoother = SpeedSmoother(method='kalman')
smoothed_speeds = smoother.smooth_speed_series(speeds)
```

## Smoothing Methods

### Kalman Filter

The Kalman filter is an optimal recursive estimator that works well for real-time applications. It's particularly effective when:
- You have continuous streaming data
- You want to minimize mean squared error
- You have some knowledge of process and measurement noise

**Parameters:**
- `kalman_process_variance` (default: 1e-5): Process noise variance (Q). Higher values allow more rapid changes.
- `kalman_measurement_variance` (default: 1e-1): Measurement noise variance (R). Higher values trust measurements less.

**Example:**
```python
smoother = SpeedSmoother(
    method='kalman',
    kalman_process_variance=1e-5,    # Low process noise
    kalman_measurement_variance=0.5  # Moderate measurement noise
)
```

**When to use:**
- Real-time GPS tracking
- Noisy but relatively smooth speed profiles
- When you want optimal filtering with known noise characteristics

### Savitzky-Golay Filter

The Savitzky-Golay filter fits successive sub-sets of adjacent data points with a polynomial. It's particularly effective when:
- You have batch data (not real-time)
- You want to preserve trends and features
- You have sudden changes or spikes in the data

**Parameters:**
- `savgol_window_length` (default: 5): Length of the filter window (must be odd and > polyorder)
- `savgol_polyorder` (default: 2): Order of the polynomial used to fit the samples

**Example:**
```python
smoother = SpeedSmoother(
    method='savgol',
    savgol_window_length=11,  # Larger window = more smoothing
    savgol_polyorder=3        # Higher order = preserve more features
)
```

**When to use:**
- Post-processing of GPS tracks
- Data with sudden spikes or outliers
- When you want to preserve acceleration patterns
- When you need to smooth derivatives (acceleration)

### No Smoothing

```python
smoother = SpeedSmoother(method='none')
```

Use this when you want to compute missing speeds but not apply any smoothing.

## Speed Computation

When `compute_missing=True`, the smoother automatically computes speed from GPS coordinates using the haversine formula:

```python
from bhulan.core.smoothing import haversine_distance, compute_speed_from_coordinates

# Compute distance between two points
distance = haversine_distance(
    lat1=37.7749, lon1=-122.4194,
    lat2=37.7750, lon2=-122.4195
)
print(f"Distance: {distance:.2f} meters")

# Compute speeds for entire track
points = [...]  # List of points with lat, lon, ts_utc
speeds = compute_speed_from_coordinates(points)
```

**Formula:**
- Distance: Haversine formula for great circle distance
- Speed: distance / time_delta (in meters per second)
- First point: Speed is set to 0 (no previous point)

## Integration with Ingestion Pipeline

The smoothing layer can be integrated into the ingestion pipeline:

```python
from bhulan.ingestion.files import ingest_file
from bhulan.core.smoothing import SpeedSmoother
from bhulan.storage.mongo_repo import MongoTrackPointRepository

# Ingest file
result = ingest_file('gps_data.csv')

# Retrieve points from MongoDB
repo = MongoTrackPointRepository()
points = repo.get_by_device_and_time(
    device_id='TRK-001',
    start_time=start,
    end_time=end
)

# Convert to dict format
point_dicts = [
    {
        'lat': p['lat'],
        'lon': p['lon'],
        'ts_utc': p['ts_utc'],
        'speed_mps': p.get('speed_mps')
    }
    for p in points
]

# Apply smoothing
smoother = SpeedSmoother(method='kalman')
smoothed = smoother.smooth(point_dicts, compute_missing=True)

# Update points with smoothed speeds
for i, point in enumerate(points):
    point['speed_mps'] = smoothed[i]['speed_mps']
    point['speed_mps_original'] = smoothed[i]['speed_mps_original']

# Store back to MongoDB
# (implementation depends on your update strategy)
```

## Performance Considerations

- **Kalman Filter**: O(n) time complexity, very efficient for real-time streaming
- **Savitzky-Golay Filter**: O(n) time complexity, requires full dataset
- **Speed Computation**: O(n) time complexity using haversine formula

For large datasets (>100k points), consider:
- Processing in batches
- Using Kalman filter for real-time applications
- Using Savitzky-Golay for batch post-processing

## Examples

### Example 1: Smooth Noisy GPS Track

```python
import numpy as np
from bhulan.core.smoothing import SpeedSmoother
from datetime import datetime, timedelta

# Create noisy GPS track
base_time = datetime(2024, 5, 1, 12, 0, 0)
true_speed = 15.0  # m/s

points = []
for i in range(50):
    noisy_speed = true_speed + np.random.normal(0, 2.0)
    points.append({
        'lat': 37.7749 + i * 0.0001,
        'lon': -122.4194,
        'ts_utc': base_time + timedelta(seconds=i),
        'speed_mps': noisy_speed
    })

# Apply Kalman smoothing
smoother = SpeedSmoother(method='kalman')
smoothed = smoother.smooth(points)

# Compare variance
original_std = np.std([p['speed_mps'] for p in points])
smoothed_std = np.std([p['speed_mps'] for p in smoothed])

print(f"Original std: {original_std:.2f} m/s")
print(f"Smoothed std: {smoothed_std:.2f} m/s")
print(f"Reduction: {(1 - smoothed_std/original_std)*100:.1f}%")
```

### Example 2: Handle Speed Spikes

```python
# Track with speed spikes
points = [
    {'lat': 37.7749, 'lon': -122.4194, 'ts_utc': t0, 'speed_mps': 15.0},
    {'lat': 37.7750, 'lon': -122.4195, 'ts_utc': t1, 'speed_mps': 16.0},
    {'lat': 37.7751, 'lon': -122.4196, 'ts_utc': t2, 'speed_mps': 80.0},  # Spike!
    {'lat': 37.7752, 'lon': -122.4197, 'ts_utc': t3, 'speed_mps': 15.5},
    {'lat': 37.7753, 'lon': -122.4198, 'ts_utc': t4, 'speed_mps': 16.5},
]

# Savitzky-Golay handles spikes better
smoother = SpeedSmoother(method='savgol', savgol_window_length=5)
smoothed = smoother.smooth(points)

# Spike should be reduced
print(f"Original spike: {points[2]['speed_mps']} m/s")
print(f"Smoothed spike: {smoothed[2]['speed_mps']:.2f} m/s")
```

### Example 3: Compare Methods

```python
# Compare Kalman vs Savitzky-Golay
kalman_smoother = SpeedSmoother(method='kalman')
savgol_smoother = SpeedSmoother(method='savgol')

kalman_result = kalman_smoother.smooth(points)
savgol_result = savgol_smoother.smooth(points)

# Analyze results
for i in range(len(points)):
    print(f"Point {i}:")
    print(f"  Original: {points[i]['speed_mps']:.2f}")
    print(f"  Kalman:   {kalman_result[i]['speed_mps']:.2f}")
    print(f"  Savgol:   {savgol_result[i]['speed_mps']:.2f}")
```

## Testing

Run the smoothing tests:

```bash
# Unit tests
pytest tests/unit/test_smoothing.py -v

# Integration tests
pytest tests/integration/test_smoothing_integration.py -v
```

## API Reference

### `haversine_distance(lat1, lon1, lat2, lon2)`
Calculate great circle distance between two points.

### `compute_speed_from_coordinates(points)`
Compute speed from GPS coordinates for a list of points.

### `KalmanFilter(process_variance, measurement_variance, initial_estimate, initial_error)`
1D Kalman filter for speed smoothing.

### `savitzky_golay_filter(speeds, window_length, polyorder)`
Apply Savitzky-Golay filter to speed time series.

### `SpeedSmoother(method, **kwargs)`
Configurable speed smoothing layer.

**Methods:**
- `smooth(points, compute_missing)`: Smooth track points
- `smooth_speed_series(speeds)`: Smooth speed series directly

## References

- Kalman, R. E. (1960). "A New Approach to Linear Filtering and Prediction Problems"
- Savitzky, A.; Golay, M. J. E. (1964). "Smoothing and Differentiation of Data by Simplified Least Squares Procedures"
- Haversine formula: https://en.wikipedia.org/wiki/Haversine_formula
