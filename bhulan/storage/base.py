"""
Abstract base classes for storage repositories.

Defines interfaces for storage backends to implement.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from bhulan.models.canonical import TrackPoint


class TrackPointRepository(ABC):
    """Abstract repository for storing GPS track points."""
    
    @abstractmethod
    def upsert_batch(self, points: List[TrackPoint]) -> int:
        """
        Insert or update a batch of track points.
        
        Args:
            points: List of TrackPoint objects to persist
            
        Returns:
            Number of points successfully inserted/updated
        """
        pass
    
    @abstractmethod
    def exists(self, point_hash: str) -> bool:
        """
        Check if a track point with given hash already exists.
        
        Args:
            point_hash: Deterministic hash of the track point
            
        Returns:
            True if point exists, False otherwise
        """
        pass
    
    @abstractmethod
    def create_indexes(self) -> None:
        """Create necessary indexes for efficient querying."""
        pass
    
    @abstractmethod
    def get_by_device_and_time(
        self, 
        device_id: str, 
        start_time: Any, 
        end_time: Any
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
        pass
    
    @abstractmethod
    def count_by_ingest_id(self, ingest_id: str) -> int:
        """
        Count track points for a specific ingestion job.
        
        Args:
            ingest_id: Ingestion job identifier
            
        Returns:
            Number of points for this ingestion job
        """
        pass


class JobRegistry(ABC):
    """Abstract registry for tracking ingestion jobs."""
    
    @abstractmethod
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
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
    def get_job(self, ingest_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve job information.
        
        Args:
            ingest_id: Job identifier
            
        Returns:
            Job document or None if not found
        """
        pass
