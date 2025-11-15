#!/usr/bin/env python3
"""
System tests for stop detection workflow.

Tests the end-to-end stop detection pipeline:
- GPS points → findStops() → detected stops with centroids, durations, radii
"""

import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from tests.system.test_helpers import setup_stubs, generate_gps_route, generate_single_stop_route, assert_stop_near, assert_duration_near
from tests.system.fake_db import FakeMongoClient

setup_stubs()

import mongo
from classes import TruckPoint
from processVehicles import findStops, findStopsAll, getGPSFrequency
from init import MIN_STOP_TIME, CONSTRAINT


class TestStopDetection(unittest.TestCase):
    """Test stop detection algorithm with synthetic GPS data"""
    
    def setUp(self):
        """Setup fake database for each test"""
        self.fake_client = FakeMongoClient()
        mongo.client = self.fake_client
        self.db = 'test_db'
    
    def tearDown(self):
        """Clean up after each test"""
        self.fake_client.reset()
    
    def test_single_stop_detection(self):
        """Test detection of a single stop with 15 minute duration"""
        truck_id = "TRUCK-001"
        date_num = 223
        
        points = generate_single_stop_route(truck_id, date_num, duration_minutes=15)
        
        TruckPoint.saveItems([point for point in points], self.db)
        
        stops = findStops(truck_id, date_num, self.db, constraint=CONSTRAINT)
        
        self.assertEqual(len(stops), 1, "Should detect exactly 1 stop")
        
        stop = stops[0]
        self.assertIn('point', stop)
        self.assertIn('radius', stop)
        self.assertIn('startStop', stop)
        
        assert_stop_near(stop, 37.4419, -122.1430, tolerance=0.001)
        
        start_time, end_time = stop['startStop']
        self.assertIsNotNone(start_time)
        self.assertIsNotNone(end_time)
    
    def test_multiple_stops_detection(self):
        """Test detection of multiple stops in a route"""
        truck_id = "TRUCK-002"
        date_num = 224
        
        points = generate_gps_route(truck_id, date_num, num_stops=3)
        
        TruckPoint.saveItems([point for point in points], self.db)
        
        stops = findStops(truck_id, date_num, self.db, constraint=CONSTRAINT)
        
        self.assertEqual(len(stops), 3, f"Should detect 3 stops, found {len(stops)}")
        
        for i in range(len(stops) - 1):
            stop1 = stops[i]['point']
            stop2 = stops[i + 1]['point']
            
            lat_diff = abs(stop1.lat - stop2.lat)
            lon_diff = abs(stop1.lon - stop2.lon)
            
            self.assertGreater(lat_diff + lon_diff, 0.005, 
                             f"Stops {i} and {i+1} should be spatially separated")
    
    def test_short_stop_filtered_out(self):
        """Test that stops shorter than MIN_STOP_TIME are filtered out"""
        truck_id = "TRUCK-003"
        date_num = 225
        
        points = generate_single_stop_route(truck_id, date_num, duration_minutes=8)
        
        TruckPoint.saveItems([point for point in points], self.db)
        
        stops = findStops(truck_id, date_num, self.db, constraint=CONSTRAINT)
        
        self.assertEqual(len(stops), 0, 
                        f"Stops < {MIN_STOP_TIME} minutes should be filtered out")
    
    def test_stop_at_threshold(self):
        """Test that stops exactly at MIN_STOP_TIME threshold are detected"""
        truck_id = "TRUCK-004"
        date_num = 226
        
        points = generate_single_stop_route(truck_id, date_num, duration_minutes=10)
        
        TruckPoint.saveItems([point for point in points], self.db)
        
        stops = findStops(truck_id, date_num, self.db, constraint=CONSTRAINT)
        
        self.assertEqual(len(stops), 1, 
                        f"Stops >= {MIN_STOP_TIME} minutes should be detected")
    
    def test_empty_dataset(self):
        """Test stop detection with no GPS points"""
        truck_id = "TRUCK-005"
        date_num = 227
        
        
        stops = findStops(truck_id, date_num, self.db, constraint=CONSTRAINT)
        
        self.assertEqual(len(stops), 0, "Empty dataset should return no stops")
    
    def test_single_point_dataset(self):
        """Test stop detection with only one GPS point"""
        truck_id = "TRUCK-006"
        date_num = 228
        
        points = generate_single_stop_route(truck_id, date_num, duration_minutes=1)[:1]
        
        TruckPoint.saveItems(points, self.db)
        
        stops = findStops(truck_id, date_num, self.db, constraint=CONSTRAINT)
        
        self.assertEqual(len(stops), 0, "Single point should not form a stop")
    
    def test_stop_radius_calculation(self):
        """Test that stop radius is calculated correctly"""
        truck_id = "TRUCK-007"
        date_num = 229
        
        points = generate_single_stop_route(truck_id, date_num, duration_minutes=15)
        
        TruckPoint.saveItems(points, self.db)
        
        stops = findStops(truck_id, date_num, self.db, constraint=CONSTRAINT)
        
        self.assertEqual(len(stops), 1)
        
        stop = stops[0]
        radius = stop['radius']
        
        self.assertGreater(radius, 0, "Radius should be positive")
        self.assertLess(radius, 1.0, "Radius should be < 1 km for tight cluster")
    
    def test_findStopsAll_multiple_trucks(self):
        """Test findStopsAll with multiple trucks and dates"""
        trucks = ["TRUCK-A", "TRUCK-B"]
        dates = [230, 231]
        
        for truck_id in trucks:
            for date_num in dates:
                points = generate_gps_route(truck_id, date_num, num_stops=2)
                TruckPoint.saveItems(points, self.db)
        
        all_stops = findStopsAll(self.db, constraint=CONSTRAINT, 
                                 trucks=trucks, datenums=dates)
        
        self.assertEqual(len(all_stops), 8, 
                        f"Should detect 8 stops (2 trucks × 2 dates × 2 stops), found {len(all_stops)}")
        
        for stop_data in all_stops:
            self.assertEqual(len(stop_data), 7, "Each stop should have 7 fields")
            date_num, truck_id, lat, lon, radius, start_time, end_time = stop_data
            
            self.assertIn(truck_id, trucks)
            self.assertIn(date_num, dates)
            self.assertIsInstance(lat, float)
            self.assertIsInstance(lon, float)
            self.assertGreater(radius, 0)


class TestGPSFrequency(unittest.TestCase):
    """Test GPS frequency calculation"""
    
    def setUp(self):
        """Setup fake database for each test"""
        self.fake_client = FakeMongoClient()
        mongo.client = self.fake_client
        self.db = 'test_db'
    
    def tearDown(self):
        """Clean up after each test"""
        self.fake_client.reset()
    
    def test_gps_frequency_calculation(self):
        """Test GPS frequency calculation with regular sampling"""
        truck_id = "TRUCK-FREQ"
        date_num = 240
        
        points = generate_single_stop_route(truck_id, date_num, duration_minutes=15)
        
        TruckPoint.saveItems(points, self.db)
        
        freq = getGPSFrequency(truck_id, date_num, self.db)
        
        self.assertIsNotNone(freq)
        self.assertGreater(freq, 50, "Frequency should be > 50 seconds")
        self.assertLess(freq, 70, "Frequency should be < 70 seconds")
    
    def test_gps_frequency_insufficient_points(self):
        """Test GPS frequency with insufficient points"""
        truck_id = "TRUCK-FREQ2"
        date_num = 241
        
        points = generate_single_stop_route(truck_id, date_num, duration_minutes=1)[:1]
        
        TruckPoint.saveItems(points, self.db)
        
        freq = getGPSFrequency(truck_id, date_num, self.db)
        
        self.assertIsNone(freq, "Should return None for < 2 points")


if __name__ == '__main__':
    unittest.main()
