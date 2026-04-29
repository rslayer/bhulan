"""
Configuration settings for bhulan ingestion subsystem.

Uses pydantic BaseSettings for environment variable support.
"""

from typing import Optional

from pydantic_settings import BaseSettings


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
    RATE_LIMIT_AUTH: str = "10/minute"
    # Where the built SPA lives, relative to the container workdir. If the
    # directory is missing, the API still works — only the mounted UI is skipped.
    WEB_DIST_DIR: str = "web/dist"

    # --- User profiles + history (opt-in) ---
    # When empty, the /v1/auth + /v1/history endpoints still mount but every
    # call is a no-op / 401. Set BHULAN_AUTH_ENABLED=true and BHULAN_DB_PATH
    # to a writable location (e.g. /data/bhulan.db on Fly with a mounted volume)
    # to enable persistence.
    BHULAN_AUTH_ENABLED: bool = False
    BHULAN_DB_PATH: str = "/data/bhulan.db"
    # Signing secret for session tokens. MUST be set to a long random value in
    # production; the dev default is only for local sqlite runs.
    BHULAN_AUTH_SECRET: str = "dev-secret-change-me"
    # Magic-link + session lifetimes.
    BHULAN_MAGIC_LINK_TTL_MIN: int = 15
    BHULAN_SESSION_TTL_DAYS: int = 30
    # Public URL the magic link should point at. The backend composes
    # "{FRONTEND_URL}/#token=<magic>" and emails / logs that.
    FRONTEND_URL: str = "http://localhost:5173"
    # When true, the magic-link token is logged to stdout AND returned in
    # the /v1/auth/request response so a local developer can copy it
    # without running a mail server. MUST stay false in production — if
    # it's true, an attacker can request a link for any email and read
    # the token straight out of the HTTP response. Opt in explicitly by
    # setting AUTH_DEV_MODE=true in your local .env.
    AUTH_DEV_MODE: bool = False
    # SMTP. All optional — if SMTP_HOST is unset, mail sending is skipped.
    SMTP_HOST: Optional[str] = None
    SMTP_PORT: int = 587
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    SMTP_FROM: str = "bhulan@example.com"
    SMTP_USE_TLS: bool = True

    ENABLE_PROMETHEUS: bool = True
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
