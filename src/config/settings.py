"""
Application settings loaded from environment variables.

Uses pydantic-settings to validate and type-check all configuration.
Settings are loaded once and cached via lru_cache for performance.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pipeline configuration loaded from .env file or environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Database ──────────────────────────────────────────────────────────
    POSTGRES_USER: str = "weather_user"
    POSTGRES_PASSWORD: str = "weather_pass"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "weather_db"

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # ── API ───────────────────────────────────────────────────────────────
    API_BASE_URL: str = "https://api.open-meteo.com/v1/forecast"
    BATCH_SIZE: int = 50  # cities per API request
    API_TIMEOUT_SECONDS: int = 30
    API_MAX_RETRIES: int = 3
    API_RETRY_DELAY_SECONDS: float = 1.0
    RATE_LIMIT_DELAY_SECONDS: float = 1.0  # delay between batch requests

    # ── Pipeline ──────────────────────────────────────────────────────────
    FETCH_INTERVAL_MINUTES: int = 5
    ANOMALY_LOOKBACK_HOURS: int = 24
    ANOMALY_Z_THRESHOLD: float = 2.0
    MOVING_AVERAGE_WINDOW: int = 24  # number of readings

    # ── Logging ───────────────────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/pipeline.log"
    LOG_ROTATION: str = "10 MB"
    LOG_RETENTION: str = "7 days"

    # ── Monitoring ────────────────────────────────────────────────────────
    ALERT_ON_FAILURE: bool = True
    HEALTH_CHECK_INTERVAL_MINUTES: int = 10
    MAX_FAILURE_RATE_PERCENT: float = 20.0


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance. Call once at startup."""
    return Settings()
