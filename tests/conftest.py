"""
Pytest configuration and shared fixtures.

Provides common fixtures and configuration for all tests.
"""


import os

import pytest


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
    """Skip integration tests if MongoDB is not available.

    Skipping is a convenience for local runs without a Mongo instance. In an
    environment that is *supposed* to provide one (CI, where a service
    container is wired up), silently skipping would turn a broken service into
    a false green — the whole suite would "pass" while testing nothing. Set
    ``BHULAN_REQUIRE_MONGO=1`` there to make an unreachable MongoDB a hard
    error instead.
    """
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.server_info()
        mongodb_available = True
    except Exception as exc:
        mongodb_available = False
        mongo_error = exc

    if not mongodb_available:
        if os.environ.get("BHULAN_REQUIRE_MONGO") == "1":
            raise pytest.UsageError(
                "BHULAN_REQUIRE_MONGO=1 but MongoDB is not reachable at "
                f"mongodb://localhost:27017 ({mongo_error!r}). Refusing to "
                "silently skip the integration suite — fix the service before "
                "trusting this run."
            )
        skip_mongo = pytest.mark.skip(reason="MongoDB not available")
        for item in items:
            if "integration" in item.keywords or "mongo" in item.keywords:
                item.add_marker(skip_mongo)
