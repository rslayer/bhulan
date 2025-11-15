"""
Kafka consumer for GPS data streams.

Consumes GPS data from Kafka topics and ingests into the system.
"""

import json
import uuid
from typing import Optional, Dict, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from bhulan.config.settings import settings
from bhulan.ingestion.normalize import normalize_batch, MappingPlan
from bhulan.storage.mongo_repo import MongoTrackPointRepository, MongoJobRegistry
from bhulan.models.vendor.generic import create_generic_mapping
import logging

logger = logging.getLogger(__name__)


class KafkaGPSConsumer:
    """Kafka consumer for GPS data ingestion."""
    
    def __init__(
        self,
        topic: str = None,
        group_id: str = None,
        mapping: Optional[MappingPlan] = None,
        vendor: str = 'generic'
    ):
        """
        Initialize Kafka consumer.
        
        Args:
            topic: Kafka topic to consume from
            group_id: Consumer group ID
            mapping: Mapping plan for data normalization
            vendor: Vendor identifier
        """
        self.topic = topic or settings.KAFKA_TOPIC
        self.group_id = group_id or settings.KAFKA_GROUP_ID
        self.mapping = mapping or create_generic_mapping()
        self.vendor = vendor
        
        self.track_repo = MongoTrackPointRepository()
        self.job_registry = MongoJobRegistry()
        
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=settings.KAFKA_BROKERS.split(','),
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_commit=False,  # Manual commit after successful processing
            enable_auto_commit=False
        )
        
        logger.info(f"Kafka consumer initialized for topic: {self.topic}")
    
    def consume_batch(self, batch_size: int = None) -> None:
        """
        Consume and process a batch of messages.
        
        Args:
            batch_size: Number of messages to consume in batch
        """
        batch_size = batch_size or settings.MAX_BATCH_SIZE
        
        messages = []
        records = []
        
        for message in self.consumer:
            messages.append(message)
            records.append(message.value)
            
            if len(messages) >= batch_size:
                break
        
        if not records:
            return
        
        ingest_id = str(uuid.uuid4())
        
        self.job_registry.create_job(
            ingest_id=ingest_id,
            source='kafka',
            params={
                'topic': self.topic,
                'vendor': self.vendor,
                'batch_size': len(records)
            }
        )
        
        try:
            result, points = normalize_batch(records, self.mapping, ingest_id)
            
            if points:
                self.track_repo.upsert_batch(points)
            
            self.job_registry.update_job_status(
                ingest_id=ingest_id,
                status='succeeded' if result.rejected == 0 else 'partial',
                stats={
                    'read': len(records),
                    'accepted': result.accepted,
                    'rejected': result.rejected
                },
                error_sample=dict(list(result.errors.items())[:10])
            )
            
            self.consumer.commit()
            
            logger.info(f"Processed Kafka batch: {result.accepted} accepted, {result.rejected} rejected")
            
        except Exception as e:
            logger.error(f"Error processing Kafka batch: {str(e)}")
            
            self.job_registry.update_job_status(
                ingest_id=ingest_id,
                status='failed',
                error_sample={0: str(e)}
            )
            
    
    def run(self):
        """Run consumer loop continuously."""
        logger.info("Starting Kafka consumer loop")
        
        try:
            while True:
                self.consume_batch()
        except KeyboardInterrupt:
            logger.info("Kafka consumer stopped by user")
        except Exception as e:
            logger.error(f"Kafka consumer error: {str(e)}")
            raise
        finally:
            self.consumer.close()
            logger.info("Kafka consumer closed")


def start_kafka_consumer(
    topic: str = None,
    vendor: str = 'generic',
    mapping: Optional[MappingPlan] = None
):
    """
    Start Kafka consumer for GPS data ingestion.
    
    Args:
        topic: Kafka topic to consume from
        vendor: Vendor identifier
        mapping: Optional custom mapping plan
    """
    if not settings.ENABLE_KAFKA:
        logger.warning("Kafka ingestion is disabled in settings")
        return
    
    consumer = KafkaGPSConsumer(
        topic=topic,
        mapping=mapping,
        vendor=vendor
    )
    
    consumer.run()
