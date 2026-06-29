#!/usr/bin/env python3
"""
Smoke test to verify all modules can be imported under Python 3.

This test stubs external dependencies (pymongo, geopy, xlrd) to avoid
requiring database connections or network calls.
"""

import os
import sys
import types
import unittest

# The legacy modules now live under legacy/ at the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "legacy"))

pymongo = types.ModuleType("pymongo")


class MockUpdateOne:
    def __init__(self, query, update, upsert=False):
        self.query = query
        self.update = update
        self.upsert = upsert


class MockBulkWriteError(Exception):
    def __init__(self, details=None):
        super().__init__(details or {})
        self.details = details or {}


class MockMongoClient:
    def __init__(self, *args, **kwargs):
        pass

    def __getitem__(self, key):
        return MockDatabase()


class MockDatabase:
    def __getitem__(self, key):
        return MockCollection()


class MockCollection:
    def create_index(self, *args, **kwargs):
        return None

    def count_documents(self, *args, **kwargs):
        return 0

    def bulk_write(self, *args, **kwargs):
        class Result:
            upserted_count = 0
            modified_count = 0

        return Result()

    def update_one(self, *args, **kwargs):
        return None

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


pymongo.ASCENDING = 1
pymongo.GEOSPHERE = "2dsphere"
pymongo.MongoClient = MockMongoClient
pymongo.UpdateOne = MockUpdateOne
pymongo_errors = types.ModuleType("pymongo.errors")
pymongo_errors.BulkWriteError = MockBulkWriteError
sys.modules["pymongo"] = pymongo
sys.modules["pymongo.errors"] = pymongo_errors

gridfs = types.ModuleType("gridfs")


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
sys.modules["gridfs"] = gridfs

geopy = types.ModuleType("geopy")
geocoders = types.ModuleType("geopy.geocoders")


class MockNominatim:
    def __init__(self, *args, **kwargs):
        pass

    def reverse(self, coords, timeout=10):
        class MockLocation:
            address = "Stubbed Address"

        return MockLocation()


geocoders.Nominatim = MockNominatim
sys.modules["geopy"] = geopy
sys.modules["geopy.geocoders"] = geocoders

xlrd = types.ModuleType("xlrd")
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
sys.modules["xlrd"] = xlrd

requests = types.ModuleType("requests")


class MockResponse:
    text = "Mock response"


def mock_post(*args, **kwargs):
    return MockResponse()


requests.post = mock_post
sys.modules["requests"] = requests


class TestPython3Imports(unittest.TestCase):
    """Test that all modules can be imported under Python 3"""

    def test_import_constants(self):
        """Test importing constants module"""
        try:
            import constants  # noqa: F401

            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"constants.py has Python 2 syntax errors: {e}")

    def test_import_util(self):
        """Test importing util module"""
        try:
            import util  # noqa: F401

            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"util.py has Python 2 syntax errors: {e}")

    def test_import_mongo(self):
        """Test importing mongo module"""
        try:
            import mongo  # noqa: F401

            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"mongo.py has Python 2 syntax errors: {e}")

    def test_import_classes(self):
        """Test importing classes module"""
        try:
            import classes  # noqa: F401

            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"classes.py has Python 2 syntax errors: {e}")

    def test_import_computed(self):
        """Test importing computed module"""
        try:
            import computed  # noqa: F401

            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"computed.py has Python 2 syntax errors: {e}")

    def test_import_process_stops(self):
        """Test importing processStops module"""
        try:
            import processStops  # noqa: F401

            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"processStops.py has Python 2 syntax errors: {e}")

    def test_import_process_vehicles(self):
        """Test importing processVehicles module"""
        try:
            import processVehicles  # noqa: F401

            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"processVehicles.py has Python 2 syntax errors: {e}")

    def test_import_input_output(self):
        """Test importing inputOutput module"""
        try:
            import inputOutput  # noqa: F401

            self.assertTrue(True)
        except SyntaxError as e:
            self.fail(f"inputOutput.py has Python 2 syntax errors: {e}")


if __name__ == "__main__":
    unittest.main()
