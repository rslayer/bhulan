"""
MongoDB implementation of storage repositories.

Provides concrete implementations for TrackPoint and Job storage using MongoDB.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import ASCENDING, GEOSPHERE, MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from bhulan.config.settings import settings
from bhulan.models.canonical import TrackPoint
from bhulan.storage.base import JobRegistry, TrackPointRepository


class MongoTrackPointRepository(TrackPointRepository):
    """MongoDB implementation of TrackPoint repository."""

    def __init__(self, mongo_uri: Optional[str] = None, db_name: Optional[str] = None):
        """
        Initialize MongoDB connection.

        Args:
            mongo_uri: MongoDB connection URI (defaults to settings)
            db_name: Database name (defaults to settings)
        """
        self.mongo_uri = mongo_uri or settings.MONGO_URI
        self.db_name = db_name or settings.MONGO_DB_NAME
        self.client: MongoClient = MongoClient(self.mongo_uri)  # type: ignore[type-arg]
        self.db = self.client[self.db_name]
        self.collection = self.db['track_points']

    def upsert_batch(self, points: List[TrackPoint]) -> int:
        """
        Insert or update a batch of track points using bulk_write.

        Uses point hash for deduplication. Skips duplicates silently.
        Sends all operations in a single bulk_write call instead of
        issuing one update_one per document.

        Args:
            points: List of TrackPoint objects to persist

        Returns:
            Number of points successfully upserted
        """
        if not points:
            return 0

        ops = []
        for point in points:
            doc = point.to_mongo_doc()
            doc['_hash'] = point.compute_hash()
            ops.append(UpdateOne({'_hash': doc['_hash']}, {'$set': doc}, upsert=True))

        try:
            result = self.collection.bulk_write(ops, ordered=False)
            return result.upserted_count + result.modified_count
        except BulkWriteError as exc:
            details = exc.details
            write_errors = details.get('writeErrors', [])
            non_dup_errors = [e for e in write_errors if e.get('code') != 11000]
            if non_dup_errors:
                raise
            # All errors are duplicate-key — return partial success count
            n_upserted: int = details.get('nUpserted', 0)
            n_modified: int = details.get('nModified', 0)
            return n_upserted + n_modified

    def exists(self, point_hash: str) -> bool:
        """
        Check if a track point with given hash already exists.

        Args:
            point_hash: Deterministic hash of the track point

        Returns:
            True if point exists, False otherwise
        """
        return self.collection.count_documents({'_hash': point_hash}, limit=1) > 0

    def create_indexes(self) -> None:
        """Create necessary indexes for efficient querying."""
        self.collection.create_index('_hash', unique=True)

        self.collection.create_index([('loc', GEOSPHERE)])

        self.collection.create_index([
            ('device_id', ASCENDING),
            ('ts_utc', ASCENDING)
        ])

        self.collection.create_index('ingest_id')

    def get_by_device_and_time(
        self,
        device_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        Retrieve track points for a device within a time range.

        Args:
            device_id: Device identifier
            start_time: Start of time range
            end_time: End of time range

        Returns:
            List of track point documents
        """
        query = {
            'device_id': device_id,
            'ts_utc': {
                '$gte': start_time,
                '$lte': end_time
            }
        }
        return list(self.collection.find(query).sort('ts_utc', ASCENDING))

    def count_by_ingest_id(self, ingest_id: str) -> int:
        """
        Count track points for a specific ingestion job.

        Args:
            ingest_id: Ingestion job identifier

        Returns:
            Number of points for this ingestion job
        """
        return self.collection.count_documents({'ingest_id': ingest_id})


class MongoJobRegistry(JobRegistry):
    """MongoDB implementation of job registry."""

    def __init__(self, mongo_uri: Optional[str] = None, db_name: Optional[str] = None):
        """
        Initialize MongoDB connection.

        Args:
            mongo_uri: MongoDB connection URI (defaults to settings)
            db_name: Database name (defaults to settings)
        """
        self.mongo_uri = mongo_uri or settings.MONGO_URI
        self.db_name = db_name or settings.MONGO_DB_NAME
        self.client: MongoClient = MongoClient(self.mongo_uri)  # type: ignore[type-arg]
        self.db = self.client[self.db_name]
        self.collection = self.db['ingest_jobs']

        self.collection.create_index('ingest_id', unique=True)

    def create_job(
        self,
        ingest_id: str,
        source: str,
        params: Dict[str, Any]
    ) -> None:
        """
        Create a new ingestion job record.

        Args:
            ingest_id: Unique job identifier
            source: Source type (file/webhook/kafka/mqtt)
            params: Job parameters
        """
        job_doc = {
            'ingest_id': ingest_id,
            'source': source,
            'params': params,
            'status': 'running',
            'started_at': datetime.now(timezone.utc),
            'finished_at': None,
            'stats': {
                'read': 0,
                'accepted': 0,
                'rejected': 0
            },
            'error_sample': {}
        }
        self.collection.insert_one(job_doc)

    def update_job_status(
        self,
        ingest_id: str,
        status: str,
        stats: Optional[Dict[str, Any]] = None,
        error_sample: Optional[Dict[int, str]] = None
    ) -> None:
        """
        Update job status and statistics.

        Args:
            ingest_id: Job identifier
            status: Status (running/succeeded/failed/partial)
            stats: Statistics (read, accepted, rejected counts)
            error_sample: Sample of errors encountered
        """
        update_doc: Dict[str, Any] = {
            'status': status,
            'finished_at': datetime.now(timezone.utc) if status != 'running' else None
        }

        if stats:
            update_doc['stats'] = stats

        if error_sample:
            update_doc['error_sample'] = error_sample

        self.collection.update_one(
            {'ingest_id': ingest_id},
            {'$set': update_doc}
        )

    def get_job(self, ingest_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve job information.

        Args:
            ingest_id: Job identifier

        Returns:
            Job document or None if not found
        """
        return self.collection.find_one({'ingest_id': ingest_id}, {'_id': 0})
