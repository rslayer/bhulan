#!/usr/bin/env python3
"""
Test helpers for system tests.

Provides synthetic data generators, stubs for external dependencies,
and utility functions for system testing.
"""

import sys
import types
from datetime import datetime, timedelta


def setup_stubs():
    """Setup stubs for external dependencies (geopy, xlrd, requests, pymongo)"""
    
    from tests.system.fake_db import FakeMongoClient
    
    pymongo = types.ModuleType('pymongo')
    pymongo.MongoClient = FakeMongoClient
    sys.modules['pymongo'] = pymongo
    
    geopy = types.ModuleType('geopy')
    geocoders = types.ModuleType('geopy.geocoders')
    
    class MockNominatim:
        def __init__(self, *args, **kwargs):
            pass
        
        def reverse(self, coords, timeout=10):
            class MockLocation:
                address = "Test Address, Test City"
            return MockLocation()
    
    geocoders.Nominatim = MockNominatim
    sys.modules['geopy'] = geopy
    sys.modules['geopy.geocoders'] = geocoders
    
    xlrd = types.ModuleType('xlrd')
    xlrd.xldate_as_tuple = lambda value, datemode: (2014, 8, 11, 0, 0, 0)
    sys.modules['xlrd'] = xlrd
    
    requests = types.ModuleType('requests')
    
    class MockResponse:
        text = "Mock response"
    
    def mock_post(*args, **kwargs):
        return MockResponse()
    
    requests.post = mock_post
    sys.modules['requests'] = requests
    
    gridfs = types.ModuleType('gridfs')
    
    class MockGridFS:
        def __init__(self, *args, **kwargs):
            pass
        
        def exists(self, *args, **kwargs):
            return False
        
        def get_last_version(self, *args, **kwargs):
            return None
        
        def put(self, *args, **kwargs):
            pass
        
        def delete(self, *args, **kwargs):
            pass
    
    gridfs.GridFS = MockGridFS
    sys.modules['gridfs'] = gridfs


def generate_gps_route(truck_id, date_num, num_stops=3):
    """
    Generate synthetic GPS route with realistic stops.
    
    Args:
        truck_id: Truck identifier
        date_num: Date number for the route
        num_stops: Number of stops to generate
    
    Returns:
        List of TruckPoint-compatible dictionaries
    """
    from constants import (TRUCK_ID_KEY, TIME_KEY, VELOCITY_KEY, LAT_KEY, LON_KEY,
                          DATE_NUM_KEY, PATENT_KEY, DIRECTION_KEY, TEMPERATURE_KEY,
                          COMMUNE_KEY, TIMESTAMP_KEY)
    
    points = []
    base_lat = 37.4419  # Palo Alto area
    base_lon = -122.1430
    current_time = datetime(2014, 8, 11, 8, 0, 0)
    
    for stop_idx in range(num_stops):
        stop_lat = base_lat + (stop_idx * 0.01)
        stop_lon = base_lon + (stop_idx * 0.01)
        
        num_points_at_stop = 15 + (stop_idx * 2)
        stop_duration_minutes = 12 + (stop_idx * 3)  # 12, 15, 18 minutes
        
        for point_idx in range(num_points_at_stop):
            jitter_lat = (point_idx % 3 - 1) * 0.00002
            jitter_lon = (point_idx % 3 - 1) * 0.00002
            
            point = {
                TRUCK_ID_KEY: truck_id,
                TIME_KEY: current_time.strftime("%H:%M:%S"),
                VELOCITY_KEY: 0,  # Stopped
                LAT_KEY: stop_lat + jitter_lat,
                LON_KEY: stop_lon + jitter_lon,
                DATE_NUM_KEY: date_num,
                PATENT_KEY: f"TEST-{truck_id}",
                DIRECTION_KEY: 0,
                TEMPERATURE_KEY: 20.0,
                COMMUNE_KEY: "Test Commune",
                TIMESTAMP_KEY: current_time.isoformat()
            }
            points.append(point)
            
            current_time += timedelta(seconds=45)
        
        if stop_idx < num_stops - 1:
            travel_points = 5
            travel_duration_minutes = 5
            
            for travel_idx in range(travel_points):
                progress = (travel_idx + 1) / travel_points
                travel_lat = stop_lat + (0.01 * progress)
                travel_lon = stop_lon + (0.01 * progress)
                
                point = {
                    TRUCK_ID_KEY: truck_id,
                    TIME_KEY: current_time.strftime("%H:%M:%S"),
                    VELOCITY_KEY: 30 + (travel_idx * 5),  # Moving
                    LAT_KEY: travel_lat,
                    LON_KEY: travel_lon,
                    DATE_NUM_KEY: date_num,
                    PATENT_KEY: f"TEST-{truck_id}",
                    DIRECTION_KEY: 45,
                    TEMPERATURE_KEY: 20.0,
                    COMMUNE_KEY: "Test Commune",
                    TIMESTAMP_KEY: current_time.isoformat()
                }
                points.append(point)
                
                current_time += timedelta(seconds=60)
    
    return points


def generate_single_stop_route(truck_id, date_num, duration_minutes=15):
    """
    Generate a simple route with a single stop for focused testing.
    
    Args:
        truck_id: Truck identifier
        date_num: Date number for the route
        duration_minutes: Duration of the stop in minutes
    
    Returns:
        List of TruckPoint-compatible dictionaries
    """
    from constants import (TRUCK_ID_KEY, TIME_KEY, VELOCITY_KEY, LAT_KEY, LON_KEY,
                          DATE_NUM_KEY, PATENT_KEY, DIRECTION_KEY, TEMPERATURE_KEY,
                          COMMUNE_KEY, TIMESTAMP_KEY)
    
    points = []
    stop_lat = 37.4419
    stop_lon = -122.1430
    current_time = datetime(2014, 8, 11, 9, 0, 0)
    
    num_points = duration_minutes + 1
    
    for i in range(num_points):
        jitter_lat = ((i % 3) - 1) * 0.00002
        jitter_lon = ((i % 3) - 1) * 0.00002
        
        point = {
            TRUCK_ID_KEY: truck_id,
            TIME_KEY: current_time.strftime("%H:%M:%S"),
            VELOCITY_KEY: 0,
            LAT_KEY: stop_lat + jitter_lat,
            LON_KEY: stop_lon + jitter_lon,
            DATE_NUM_KEY: date_num,
            PATENT_KEY: f"TEST-{truck_id}",
            DIRECTION_KEY: 0,
            TEMPERATURE_KEY: 20.0,
            COMMUNE_KEY: "Test Commune",
            TIMESTAMP_KEY: current_time.isoformat()
        }
        points.append(point)
        
        current_time += timedelta(minutes=1)
    
    return points


def assert_stop_near(stop, expected_lat, expected_lon, tolerance=0.001):
    """
    Assert that a stop's centroid is near expected coordinates.
    
    Args:
        stop: Stop dictionary with 'point' key
        expected_lat: Expected latitude
        expected_lon: Expected longitude
        tolerance: Acceptable distance tolerance in degrees (~100m)
    """
    point = stop['point']
    lat_diff = abs(point.lat - expected_lat)
    lon_diff = abs(point.lon - expected_lon)
    
    assert lat_diff < tolerance, f"Latitude {point.lat} not near {expected_lat} (diff: {lat_diff})"
    assert lon_diff < tolerance, f"Longitude {point.lon} not near {expected_lon} (diff: {lon_diff})"


def assert_duration_near(actual_minutes, expected_minutes, tolerance=2):
    """
    Assert that duration is near expected value.
    
    Args:
        actual_minutes: Actual duration in minutes
        expected_minutes: Expected duration in minutes
        tolerance: Acceptable tolerance in minutes
    """
    diff = abs(actual_minutes - expected_minutes)
    assert diff <= tolerance, f"Duration {actual_minutes} not near {expected_minutes} (diff: {diff})"
