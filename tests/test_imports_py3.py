#!/usr/bin/env python3
"""
Smoke test to verify all modules can be imported under Python 3.

This test stubs external dependencies (pymongo, geopy, xlrd) to avoid
requiring database connections or network calls.
"""

import unittest
import sys
import types

pymongo = types.ModuleType('pymongo')

class MockMongoClient:
    def __init__(self, *args, **kwargs):
        pass
    
    def __getitem__(self, key):
        return MockDatabase()

class MockDatabase:
    def __getitem__(self, key):
        return MockCollection()

class MockCollection:
    def find(self, *args, **kwargs):
        return []
    
    def find_one(self, *args, **kwargs):
        return None
    
    def save(self, *args, **kwargs):
        pass
    
    def insert(self, *args, **kwargs):
        pass
    
    def remove(self, *args, **kwargs):
        pass
    
    def distinct(self, *args, **kwargs):
        return []

pymongo.MongoClient = MockMongoClient
sys.modules['pymongo'] = pymongo

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

geopy = types.ModuleType('geopy')
geocoders = types.ModuleType('geopy.geocoders')

class MockNominatim:
    def __init__(self, *args, **kwargs):
        pass
    
    def reverse(self, coords, timeout=10):
        class MockLocation:
            address = "Stubbed Address"
        return MockLocation()

geocoders.Nominatim = MockNominatim
sys.modules['geopy'] = geopy
sys.modules['geopy.geocoders'] = geocoders

xlrd = types.ModuleType('xlrd')
xlrd.xldate_as_tuple = lambda value, datemode: (1900, 1, 1, 0, 0, 0)

class MockWorkbook:
    def sheet_by_name(self, name):
        return MockWorksheet()

class MockWorksheet:
    nrows = 1
    ncols = 1
    
    def row(self, index):
        class MockCell:
            value = 0
        return [MockCell() for _ in range(11)]

xlrd.open_workbook = lambda filename: MockWorkbook()
sys.modules['xlrd'] = xlrd

requests = types.ModuleType('requests')

class MockResponse:
    text = "Mock response"

def mock_post(*args, **kwargs):
    return MockResponse()

requests.post = mock_post
sys.modules['requests'] = requests


class TestPython3Imports(unittest.TestCase):
    """Test that all modules can be imported under Python 3"""
    
    def test_import_constants(self):
        """Test importing constants module"""
        try:
            import constants
            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"constants.py has Python 2 syntax errors: {e}")
    
    def test_import_util(self):
        """Test importing util module"""
        try:
            import util
            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"util.py has Python 2 syntax errors: {e}")
    
    def test_import_mongo(self):
        """Test importing mongo module"""
        try:
            import mongo
            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"mongo.py has Python 2 syntax errors: {e}")
    
    def test_import_classes(self):
        """Test importing classes module"""
        try:
            import classes
            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"classes.py has Python 2 syntax errors: {e}")
    
    def test_import_computed(self):
        """Test importing computed module"""
        try:
            import computed
            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"computed.py has Python 2 syntax errors: {e}")
    
    def test_import_processStops(self):
        """Test importing processStops module"""
        try:
            import processStops
            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"processStops.py has Python 2 syntax errors: {e}")
    
    def test_import_processVehicles(self):
        """Test importing processVehicles module"""
        try:
            import processVehicles
            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"processVehicles.py has Python 2 syntax errors: {e}")
    
    def test_import_inputOutput(self):
        """Test importing inputOutput module"""
        try:
            import inputOutput
            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"inputOutput.py has Python 2 syntax errors: {e}")


if __name__ == '__main__':
    unittest.main()
