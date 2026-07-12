"""Shared fixtures for the robustness-harness adversary tests.

Same in-process setup as ``tests/integration/test_insights_api.py`` (fake
the Mongo repo, then import the app so no real MongoDB is needed), except
``raise_server_exceptions=False`` so an unhandled 500 shows up as an
ordinary response instead of blowing up the test process — that's what a
real deployed client actually sees.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytest.importorskip("fastapi")


@pytest.fixture(scope="module")
def client():
    import bhulan.storage.mongo_repo as mongo_repo

    class _FakeCollection:
        def create_index(self, *_args, **_kwargs):
            pass

        def find_one(self, *_args, **_kwargs):
            return None

    class _FakeRepo:
        collection = _FakeCollection()

        def create_indexes(self):
            pass

    class _FakeJobs:
        collection = _FakeCollection()

    mongo_repo.MongoTrackPointRepository = lambda *a, **kw: _FakeRepo()  # type: ignore[assignment]
    mongo_repo.MongoJobRegistry = lambda *a, **kw: _FakeJobs()  # type: ignore[assignment]

    from bhulan.api.app import app

    return TestClient(app, raise_server_exceptions=False)
