"""
MQTT consumer for GPS data streams.

Subscribes to MQTT topics and ingests GPS data into the system.
"""

import json
import uuid
from typing import Optional, Dict, Any, List
from collections import deque
import paho.mqtt.client as mqtt
from bhulan.config.settings import settings
from bhulan.ingestion.normalize import normalize_batch, MappingPlan
from bhulan.storage.mongo_repo import MongoTrackPointRepository, MongoJobRegistry
from bhulan.models.vendor.generic import create_generic_mapping
import logging

logger = logging.getLogger(__name__)


class MQTTGPSConsumer:
    """MQTT consumer for GPS data ingestion."""
    
    def __init__(
        self,
        topic: str = None,
        mapping: Optional[MappingPlan] = None,
        vendor: str = 'generic',
        batch_size: int = None
    ):
        """
        Initialize MQTT consumer.
        
        Args:
            topic: MQTT topic pattern to subscribe to
            mapping: Mapping plan for data normalization
            vendor: Vendor identifier
            batch_size: Number of messages to batch before processing
        """
        self.topic = topic or settings.MQTT_TOPIC
        self.mapping = mapping or create_generic_mapping()
        self.vendor = vendor
        self.batch_size = batch_size or settings.MAX_BATCH_SIZE
        
        self.track_repo = MongoTrackPointRepository()
        self.job_registry = MongoJobRegistry()
        
        self.message_buffer: deque = deque(maxlen=self.batch_size * 2)
        
        self.dedup_buffer: deque = deque(maxlen=1000)
        
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        
        logger.info(f"MQTT consumer initialized for topic: {self.topic}")
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker."""
        if rc == 0:
            logger.info(f"Connected to MQTT broker, subscribing to {self.topic}")
            client.subscribe(self.topic)
        else:
            logger.error(f"Failed to connect to MQTT broker, return code: {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """Callback when disconnected from MQTT broker."""
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnect, return code: {rc}")
    
    def _on_message(self, client, userdata, msg):
        """Callback when message received."""
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            
            msg_hash = hash(json.dumps(payload, sort_keys=True))
            if msg_hash in self.dedup_buffer:
                logger.debug(f"Duplicate message detected, skipping")
                return
            
            self.dedup_buffer.append(msg_hash)
            
            self.message_buffer.append(payload)
            
            if len(self.message_buffer) >= self.batch_size:
                self._process_batch()
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse MQTT message: {str(e)}")
        except Exception as e:
            logger.error(f"Error processing MQTT message: {str(e)}")
    
    def _process_batch(self):
        """Process accumulated messages as a batch."""
        if not self.message_buffer:
            return
        
        records = list(self.message_buffer)
        self.message_buffer.clear()
        
        ingest_id = str(uuid.uuid4())
        
        self.job_registry.create_job(
            ingest_id=ingest_id,
            source='mqtt',
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
            
            logger.info(f"Processed MQTT batch: {result.accepted} accepted, {result.rejected} rejected")
            
        except Exception as e:
            logger.error(f"Error processing MQTT batch: {str(e)}")
            
            self.job_registry.update_job_status(
                ingest_id=ingest_id,
                status='failed',
                error_sample={0: str(e)}
            )
    
    def connect(self):
        """Connect to MQTT broker."""
        self.client.connect(
            settings.MQTT_BROKER,
            settings.MQTT_PORT,
            keepalive=60
        )
    
    def run(self):
        """Run consumer loop continuously."""
        logger.info("Starting MQTT consumer loop")
        
        try:
            self.connect()
            self.client.loop_forever()
        except KeyboardInterrupt:
            logger.info("MQTT consumer stopped by user")
            self._process_batch()
        except Exception as e:
            logger.error(f"MQTT consumer error: {str(e)}")
            raise
        finally:
            self.client.disconnect()
            logger.info("MQTT consumer closed")


def start_mqtt_consumer(
    topic: str = None,
    vendor: str = 'generic',
    mapping: Optional[MappingPlan] = None
):
    """
    Start MQTT consumer for GPS data ingestion.
    
    Args:
        topic: MQTT topic pattern to subscribe to
        vendor: Vendor identifier
        mapping: Optional custom mapping plan
    """
    if not settings.ENABLE_MQTT:
        logger.warning("MQTT ingestion is disabled in settings")
        return
    
    consumer = MQTTGPSConsumer(
        topic=topic,
        mapping=mapping,
        vendor=vendor
    )
    
    consumer.run()
