#!/usr/bin/env python3
"""
Unit tests for util.py module

Tests cover distance calculations, time helpers, and utility functions.
External dependencies (geopy, xlrd) are stubbed to avoid network calls and missing packages.
"""

import unittest
import sys
import types
from datetime import datetime, timedelta

geopy = types.ModuleType('geopy')
geocoders = types.ModuleType('geopy.geocoders')

class DummyNominatim(object):
    def __init__(self, *args, **kwargs):
        pass
    
    def reverse(self, coords, timeout=10):
        class DummyLocation:
            address = "Stubbed Address"
        return DummyLocation()

geocoders.Nominatim = DummyNominatim
sys.modules['geopy'] = geopy
sys.modules['geopy.geocoders'] = geocoders

xlrd = types.ModuleType('xlrd')
xlrd.xldate_as_tuple = lambda value, datemode: (1900, 1, 1, 0, 0, 0)
sys.modules['xlrd'] = xlrd

from util import (
    kilDist, mileDist, meterDist, findArc, euclidean,
    getMeters, getCoord, getLineForItems, getTimeDeltas,
    addIfKey, getIfKey, getLat, getLon
)


class TestDistanceFunctions(unittest.TestCase):
    """Test geodesic distance calculation functions"""
    
    def test_kilDist_zero_distance(self):
        """Same point should return 0 distance"""
        point1 = {'lat': 37.4419, 'lon': -122.1430}
        point2 = {'lat': 37.4419, 'lon': -122.1430}
        distance = kilDist(point1, point2)
        self.assertEqual(distance, 0)
    
    def test_kilDist_one_degree_latitude(self):
        """One degree of latitude is approximately 111.2 km"""
        point1 = {'lat': 0.0, 'lon': 0.0}
        point2 = {'lat': 1.0, 'lon': 0.0}
        distance = kilDist(point1, point2)
        self.assertAlmostEqual(distance, 111.19, delta=0.6)
    
    def test_kilDist_symmetry(self):
        """Distance should be symmetric: d(A,B) = d(B,A)"""
        point1 = {'lat': 37.4419, 'lon': -122.1430}
        point2 = {'lat': 37.4467, 'lon': -122.1589}
        dist1 = kilDist(point1, point2)
        dist2 = kilDist(point2, point1)
        self.assertAlmostEqual(dist1, dist2, places=10)
    
    def test_mileDist_one_degree_latitude(self):
        """One degree of latitude is approximately 69.1 miles"""
        point1 = {'lat': 0.0, 'lon': 0.0}
        point2 = {'lat': 1.0, 'lon': 0.0}
        distance = mileDist(point1, point2)
        self.assertAlmostEqual(distance, 69.09, delta=0.4)
    
    def test_meterDist_relation_to_kilDist(self):
        """meterDist should equal kilDist * 1000"""
        point1 = {'lat': 37.4419, 'lon': -122.1430}
        point2 = {'lat': 37.4467, 'lon': -122.1589}
        km_dist = kilDist(point1, point2)
        m_dist = meterDist(point1, point2)
        self.assertAlmostEqual(m_dist, km_dist * 1000, delta=0.001)
    
    def test_findArc_zero_for_same_point(self):
        """Arc should be 0 for identical points"""
        point1 = {'lat': 37.4419, 'lon': -122.1430}
        point2 = {'lat': 37.4419, 'lon': -122.1430}
        arc = findArc(point1, point2)
        self.assertEqual(arc, 0)
    
    def test_findArc_positive_for_different_points(self):
        """Arc should be positive for different points"""
        point1 = {'lat': 37.4419, 'lon': -122.1430}
        point2 = {'lat': 37.4467, 'lon': -122.1589}
        arc = findArc(point1, point2)
        self.assertGreater(arc, 0)
    
    def test_euclidean_basic(self):
        """Test basic Euclidean distance calculation"""
        point1 = {'lat': 0.0, 'lon': 0.0}
        point2 = {'lat': 3.0, 'lon': 4.0}
        dist_squared = euclidean(point1, point2, dist=False)
        self.assertEqual(dist_squared, 25.0)
        dist = euclidean(point1, point2, dist=True)
        self.assertEqual(dist, 5.0)
    
    def test_euclidean_zero_distance(self):
        """Euclidean distance for same point should be 0"""
        point1 = {'lat': 37.4419, 'lon': -122.1430}
        point2 = {'lat': 37.4419, 'lon': -122.1430}
        dist = euclidean(point1, point2, dist=True)
        self.assertEqual(dist, 0.0)


class TestCoordinateConversion(unittest.TestCase):
    """Test coordinate conversion functions"""
    
    def test_getMeters_basic(self):
        """Test conversion from coordinate to meters"""
        coord = 1.0
        meters = getMeters(coord)
        self.assertEqual(meters, 100000)
    
    def test_getCoord_basic(self):
        """Test conversion from meters to coordinate"""
        meters = 100000
        coord = getCoord(meters)
        self.assertEqual(coord, 1.0)
    
    def test_getMeters_getCoord_inverse(self):
        """getCoord should be inverse of getMeters"""
        original_coord = 37.4419
        meters = getMeters(original_coord)
        recovered_coord = getCoord(meters)
        self.assertAlmostEqual(recovered_coord, original_coord, places=10)
    
    def test_getMeters_zero(self):
        """Zero coordinate should give zero meters"""
        self.assertEqual(getMeters(0), 0)
    
    def test_getCoord_zero(self):
        """Zero meters should give zero coordinate"""
        self.assertEqual(getCoord(0), 0.0)


class TestHelperFunctions(unittest.TestCase):
    """Test utility helper functions"""
    
    def test_getLineForItems_basic(self):
        """Test CSV line generation from list"""
        items = ["a", "b", "c"]
        line = getLineForItems(items)
        self.assertEqual(line, "a,b,c\n")
    
    def test_getLineForItems_single_item(self):
        """Test CSV line with single item"""
        items = ["single"]
        line = getLineForItems(items)
        self.assertEqual(line, "single\n")
    
    def test_getLineForItems_empty(self):
        """Test CSV line with empty list"""
        items = []
        line = getLineForItems(items)
        self.assertEqual(line, "")
    
    def test_getLineForItems_numbers(self):
        """Test CSV line with numbers"""
        items = [1, 2, 3]
        line = getLineForItems(items)
        self.assertEqual(line, "1,2,3\n")
    
    def test_getTimeDeltas_basic(self):
        """Test time string to timedelta conversion"""
        time_str = "01:30:00"
        td = getTimeDeltas(time_str)
        self.assertEqual(td.total_seconds(), 5400)  # 1.5 hours = 5400 seconds
    
    def test_getTimeDeltas_zero(self):
        """Test zero time conversion"""
        time_str = "00:00:00"
        td = getTimeDeltas(time_str)
        self.assertEqual(td.total_seconds(), 0)
    
    def test_getTimeDeltas_full_day(self):
        """Test 24 hour time conversion"""
        time_str = "24:00:00"
        td = getTimeDeltas(time_str)
        self.assertEqual(td.total_seconds(), 86400)  # 24 hours = 86400 seconds
    
    def test_addIfKey_new_key(self):
        """Test adding item to new key in dict"""
        struct = {}
        addIfKey(struct, 'key1', 'value1')
        self.assertEqual(struct, {'key1': ['value1']})
    
    def test_addIfKey_existing_key(self):
        """Test adding item to existing key in dict"""
        struct = {'key1': ['value1']}
        addIfKey(struct, 'key1', 'value2')
        self.assertEqual(struct, {'key1': ['value1', 'value2']})
    
    def test_getIfKey_existing_key(self):
        """Test getting value for existing key"""
        struct = {'key1': 'value1'}
        result = getIfKey(struct, 'key1')
        self.assertEqual(result, 'value1')
    
    def test_getIfKey_missing_key_default_none(self):
        """Test getting value for missing key returns None by default"""
        struct = {'key1': 'value1'}
        result = getIfKey(struct, 'key2')
        self.assertIsNone(result)
    
    def test_getIfKey_missing_key_custom_default(self):
        """Test getting value for missing key with custom default"""
        struct = {'key1': 'value1'}
        result = getIfKey(struct, 'key2', default='default_value')
        self.assertEqual(result, 'default_value')
    
    def test_getLat_from_dict(self):
        """Test getting latitude from dict"""
        point = {'lat': 37.4419, 'lon': -122.1430}
        lat = getLat(point)
        self.assertEqual(lat, 37.4419)
    
    def test_getLon_from_dict(self):
        """Test getting longitude from dict"""
        point = {'lat': 37.4419, 'lon': -122.1430}
        lon = getLon(point)
        self.assertEqual(lon, -122.1430)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions"""
    
    def test_kilDist_antipodal_points(self):
        """Test distance between antipodal points (opposite sides of Earth)"""
        point1 = {'lat': 90.0, 'lon': 0.0}
        point2 = {'lat': -90.0, 'lon': 0.0}
        distance = kilDist(point1, point2)
        expected = 3.14159265359 * 6373
        self.assertAlmostEqual(distance, expected, delta=10)
    
    def test_kilDist_across_dateline(self):
        """Test distance calculation across international date line"""
        point1 = {'lat': 0.0, 'lon': 179.0}
        point2 = {'lat': 0.0, 'lon': -179.0}
        distance = kilDist(point1, point2)
        self.assertLess(distance, 250)
        self.assertGreater(distance, 200)
    
    def test_findArc_handles_floating_point_precision(self):
        """Test that findArc handles floating point precision issues"""
        point1 = {'lat': 37.44190000000001, 'lon': -122.14300000000001}
        point2 = {'lat': 37.44190000000002, 'lon': -122.14300000000002}
        arc = findArc(point1, point2)
        self.assertGreaterEqual(arc, 0)


if __name__ == '__main__':
    unittest.main()
