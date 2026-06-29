#!/usr/bin/env python3
"""
FakeDB - In-memory database implementation for system testing.

Mimics pymongo behaviors needed by bhulan without requiring a real MongoDB instance.
Supports: find, find_one, insert, save, remove, distinct, and cursor operations.
"""

import copy


class FakeCursor:
    """Mimics pymongo cursor with sort and limit support"""

    def __init__(self, items):
        self.items = list(items)
        self._sort_key = None
        self._sort_order = 1
        self._limit_count = None

    def sort(self, key, order=1):
        """Sort cursor results by key in ascending (1) or descending (-1) order"""
        self._sort_key = key
        self._sort_order = order
        return self

    def limit(self, count):
        """Limit cursor results to count items"""
        self._limit_count = count
        return self

    def __iter__(self):
        """Iterate over cursor results with sorting and limiting applied"""
        items = self.items

        if self._sort_key:
            items = sorted(
                items, key=lambda x: x.get(self._sort_key, 0), reverse=(self._sort_order == -1)
            )

        if self._limit_count is not None:
            items = items[: self._limit_count]

        return iter(items)

    def __len__(self):
        return len(self.items)


class FakeCollection:
    """Mimics pymongo collection with CRUD operations"""

    def __init__(self, name):
        self.name = name
        self.documents = []
        self._id_counter = 1

    def find(self, query=None, projection=None):
        """Find documents matching query"""
        if query is None:
            query = {}

        matching = []
        for doc in self.documents:
            if self._matches_query(doc, query):
                if projection:
                    doc_copy = self._apply_projection(doc, projection)
                else:
                    doc_copy = copy.deepcopy(doc)
                matching.append(doc_copy)

        return FakeCursor(matching)

    def find_one(self, query=None, projection=None):
        """Find first document matching query"""
        if query is None:
            query = {}

        for doc in self.documents:
            if self._matches_query(doc, query):
                if projection:
                    return self._apply_projection(doc, projection)
                return copy.deepcopy(doc)

        return None

    def create_index(self, *args, **kwargs):
        """Accept index declarations; uniqueness is not enforced by the fake."""
        return None

    def count_documents(self, query=None, limit=0):
        """Count documents matching query."""
        if query is None:
            query = {}
        count = sum(1 for doc in self.documents if self._matches_query(doc, query))
        return min(count, limit) if limit else count

    def insert_one(self, doc):
        """Insert a single document."""
        self._insert_one(doc)

    def update_one(self, query, update, upsert=False):
        """Apply a minimal $set update to the first matching document."""
        for doc in self.documents:
            if self._matches_query(doc, query):
                doc.update(update.get("$set", {}))
                return None
        if upsert:
            new_doc = copy.deepcopy(query)
            new_doc.update(update.get("$set", {}))
            self._insert_one(new_doc)
        return None

    def bulk_write(self, ops, ordered=False):
        """Apply minimal UpdateOne operations and return pymongo-like counts."""
        upserted = 0
        modified = 0
        for op in ops:
            before = len(self.documents)
            self.update_one(op.query, op.update, upsert=op.upsert)
            if len(self.documents) > before:
                upserted += 1
            else:
                modified += 1

        class Result:
            upserted_count = upserted
            modified_count = modified

        return Result()

    def insert(self, docs):
        """Insert one or more documents"""
        if isinstance(docs, list):
            for doc in docs:
                self._insert_one(doc)
        else:
            self._insert_one(docs)

    def _insert_one(self, doc):
        """Insert a single document"""
        doc_copy = copy.deepcopy(doc)
        if "_id" not in doc_copy:
            doc_copy["_id"] = self._id_counter
            self._id_counter += 1
        self.documents.append(doc_copy)

    def save(self, doc):
        """Save document (update if _id exists, insert otherwise)"""
        if "_id" in doc:
            for i, existing in enumerate(self.documents):
                if existing.get("_id") == doc["_id"]:
                    self.documents[i] = copy.deepcopy(doc)
                    return

        self._insert_one(doc)

    def remove(self, query=None):
        """Remove documents matching query"""
        if query is None:
            self.documents = []
        else:
            self.documents = [doc for doc in self.documents if not self._matches_query(doc, query)]

    def distinct(self, key):
        """Get distinct values for a key"""
        values = set()
        for doc in self.documents:
            if key in doc:
                values.add(doc[key])
        return list(values)

    def _matches_query(self, doc, query):
        """Check if document matches query"""
        for key, value in query.items():
            if key not in doc or doc[key] != value:
                return False
        return True

    def _apply_projection(self, doc, projection):
        """Apply projection to document"""
        if "_id" in projection and projection["_id"] == 0:
            result = {k: v for k, v in doc.items() if k != "_id"}
        else:
            result = copy.deepcopy(doc)
        return result


class FakeDatabase:
    """Mimics pymongo database"""

    def __init__(self, name):
        self.name = name
        self.collections = {}

    def __getitem__(self, collection_name):
        """Get or create collection"""
        if collection_name not in self.collections:
            self.collections[collection_name] = FakeCollection(collection_name)
        return self.collections[collection_name]


class FakeMongoClient:
    """Mimics pymongo MongoClient"""

    def __init__(self, *args, **kwargs):
        self.databases = {}

    def __getitem__(self, db_name):
        """Get or create database"""
        if db_name not in self.databases:
            self.databases[db_name] = FakeDatabase(db_name)
        return self.databases[db_name]

    def reset(self):
        """Reset all databases (for test cleanup)"""
        self.databases = {}
