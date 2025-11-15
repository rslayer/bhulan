"""
Pytest configuration and shared fixtures.

Provides common fixtures and configuration for all tests.
"""

import pytest
import os


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test requiring MongoDB"
    )
    config.addinivalue_line(
        "markers", "e2e: mark test as end-to-end test"
    )
    config.addinivalue_line(
        "markers", "mongo: mark test as requiring MongoDB"
    )


@pytest.fixture(scope="session")
def mongodb_available():
    """Check if MongoDB is available for testing."""
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.server_info()
        return True
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    """Skip integration tests if MongoDB is not available."""
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.server_info()
        mongodb_available = True
    except Exception:
        mongodb_available = False
    
    if not mongodb_available:
        skip_mongo = pytest.mark.skip(reason="MongoDB not available")
        for item in items:
            if "integration" in item.keywords or "mongo" in item.keywords:
                item.add_marker(skip_mongo)
