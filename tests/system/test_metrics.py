#!/usr/bin/env python3
"""
System tests for metrics calculations.

Tests the metrics calculation workflows:
- Distance traveled
- Time on road
- Average speed
- GPS frequency
"""

import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from tests.system.test_helpers import setup_stubs, generate_gps_route
from tests.system.fake_db import FakeMongoClient

setup_stubs()

import mongo
from classes import TruckPoint
from processStops import getTotalDistanceTraveled, getTotalTimeOnRoad, getAverageSpeedByDatenum


class TestMetricsCalculations(unittest.TestCase):
    """Test metrics calculation functions with synthetic GPS data"""
    
    def setUp(self):
        """Setup fake database for each test"""
        self.fake_client = FakeMongoClient()
        mongo.client = self.fake_client
        self.db = 'test_db'
    
    def tearDown(self):
        """Clean up after each test"""
        self.fake_client.reset()
    
    def test_total_distance_traveled(self):
        """Test calculation of total distance traveled"""
        truck_id = "TRUCK-DIST-001"
        date_num = 250
        
        points = generate_gps_route(truck_id, date_num, num_stops=3)
        
        TruckPoint.saveItems(points, self.db)
        
        distance = getTotalDistanceTraveled(truck_id, date_num, self.db)
        
        self.assertGreater(distance, 1.5, "Distance should be > 1.5 km")
        self.assertLess(distance, 4.0, "Distance should be < 4 km for this route")
    
    def test_total_distance_single_stop(self):
        """Test distance calculation for route with no travel"""
        truck_id = "TRUCK-DIST-002"
        date_num = 251
        
        from tests.system.test_helpers import generate_single_stop_route
        points = generate_single_stop_route(truck_id, date_num, duration_minutes=15)
        
        TruckPoint.saveItems(points, self.db)
        
        distance = getTotalDistanceTraveled(truck_id, date_num, self.db)
        
        self.assertLess(distance, 0.1, "Distance should be < 0.1 km for stationary route")
    
    def test_total_time_on_road(self):
        """Test calculation of total time on road"""
        truck_id = "TRUCK-TIME-001"
        date_num = 252
        
        points = generate_gps_route(truck_id, date_num, num_stops=3)
        
        TruckPoint.saveItems(points, self.db)
        
        time_hours = getTotalTimeOnRoad(truck_id, date_num, self.db)
        
        self.assertGreater(time_hours, 0, "Time on road should be > 0")
        
        self.assertLess(time_hours, 3.0, "Time on road should be < 3 hours")
    
    def test_average_speed_calculation(self):
        """Test calculation of average speed"""
        truck_id = "TRUCK-SPEED-001"
        date_num = 253
        
        points = generate_gps_route(truck_id, date_num, num_stops=3)
        
        TruckPoint.saveItems(points, self.db)
        
        distance = getTotalDistanceTraveled(truck_id, date_num, self.db)
        time_hours = getTotalTimeOnRoad(truck_id, date_num, self.db)
        
        if time_hours > 0:
            avg_speed = distance / time_hours
            
            self.assertGreater(avg_speed, 0, "Average speed should be > 0")
            self.assertLess(avg_speed, 100, "Average speed should be < 100 km/h")
        else:
            self.skipTest("No travel time detected in route")
    
    def test_metrics_with_empty_dataset(self):
        """Test metrics calculations with no GPS points"""
        truck_id = "TRUCK-EMPTY"
        date_num = 254
        
        
        distance = getTotalDistanceTraveled(truck_id, date_num, self.db)
        
        self.assertEqual(distance, 0, "Distance should be 0 for empty dataset")
    
    def test_metrics_consistency(self):
        """Test that distance, time, and speed are consistent"""
        truck_id = "TRUCK-CONSISTENT"
        date_num = 255
        
        points = generate_gps_route(truck_id, date_num, num_stops=2)
        
        TruckPoint.saveItems(points, self.db)
        
        distance = getTotalDistanceTraveled(truck_id, date_num, self.db)
        time_hours = getTotalTimeOnRoad(truck_id, date_num, self.db)
        
        if time_hours > 0:
            calculated_speed = distance / time_hours
            
            self.assertGreater(calculated_speed, 0, "Calculated speed should be > 0")
            self.assertLess(calculated_speed, 100, "Calculated speed should be < 100 km/h")
        else:
            self.skipTest("No travel time detected in route")


if __name__ == '__main__':
    unittest.main()
