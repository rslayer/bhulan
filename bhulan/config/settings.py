"""
Configuration settings for bhulan ingestion subsystem.

Uses pydantic BaseSettings for environment variable support.
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DB_NAME: str = "bhulan"
    
    MAX_BATCH_SIZE: int = 1000
    MAX_INFLIGHT_JOBS: int = 10
    
    INGEST_S3_BUCKET: Optional[str] = None
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_REGION: str = "us-east-1"
    
    ENABLE_KAFKA: bool = False
    KAFKA_BROKERS: str = "localhost:9092"
    KAFKA_TOPIC: str = "gps.raw"
    KAFKA_GROUP_ID: str = "bhulan-ingest"
    
    ENABLE_MQTT: bool = False
    MQTT_BROKER: str = "localhost"
    MQTT_PORT: int = 1883
    MQTT_TOPIC: str = "devices/+/gps"
    
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8080
    API_KEY: Optional[str] = None
    # Comma-separated CORS allowlist. Use "*" for dev/open; lock down for prod.
    ALLOWED_ORIGINS: str = "*"

    # Per-IP rate limits for the public /v1 surface. Expressed in slowapi syntax
    # (e.g. "30/minute", "600/hour"). Empty disables the limit.
    RATE_LIMIT_INSIGHTS: str = "30/minute"
    RATE_LIMIT_PLOT: str = "60/minute"
    # Where the built SPA lives, relative to the container workdir. If the
    # directory is missing, the API still works — only the mounted UI is skipped.
    WEB_DIST_DIR: str = "web/dist"

    ENABLE_PROMETHEUS: bool = True
    LOG_LEVEL: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
