"""
SQLAlchemy models and database engine management.

Defines the full data warehouse schema:
  - Raw layer:    raw_weather_data
  - Staging:      stg_weather_readings
  - Dimension:    dim_cities
  - Fact:         fact_weather
  - Aggregations: agg_daily / agg_weekly / agg_monthly
  - Analytics:    weather_anomalies
  - Lineage:      data_lineage
  - Monitoring:   pipeline_runs
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Generator

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from src.config.settings import get_settings

Base = declarative_base()


# ══════════════════════════════════════════════════════════════════════════════
# RAW LAYER
# ══════════════════════════════════════════════════════════════════════════════


class RawWeatherData(Base):
    """Stores raw API responses exactly as received."""

    __tablename__ = "raw_weather_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city_name = Column(String(100), nullable=False)
    state = Column(String(100), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    raw_json = Column(Text, nullable=False)
    api_response_time_ms = Column(Float, nullable=True)
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    pipeline_run_id = Column(String(36), nullable=True)

    __table_args__ = (
        Index("ix_raw_city_ingested", "city_name", "ingested_at"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# STAGING LAYER
# ══════════════════════════════════════════════════════════════════════════════


class StagingWeatherReading(Base):
    """Cleaned and validated weather readings."""

    __tablename__ = "stg_weather_readings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city_name = Column(String(100), nullable=False)
    state = Column(String(100), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    temperature_celsius = Column(Float, nullable=False)
    windspeed_kmh = Column(Float, nullable=False)
    winddirection_degrees = Column(Float, nullable=False)
    weathercode = Column(Integer, nullable=False)
    is_day = Column(Boolean, nullable=False)
    observation_time = Column(DateTime, nullable=False)
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    is_valid = Column(Boolean, nullable=False, default=True)
    validation_errors = Column(Text, nullable=True)
    pipeline_run_id = Column(String(36), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "city_name", "observation_time", name="uq_stg_city_obstime"
        ),
        Index("ix_stg_city_obstime", "city_name", "observation_time"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# DIMENSION LAYER
# ══════════════════════════════════════════════════════════════════════════════


class DimCity(Base):
    """City dimension table — master data for cities."""

    __tablename__ = "dim_cities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city_name = Column(String(100), nullable=False, unique=True)
    state = Column(String(100), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    population_rank = Column(Integer, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


# ══════════════════════════════════════════════════════════════════════════════
# FACT LAYER
# ══════════════════════════════════════════════════════════════════════════════


class FactWeather(Base):
    """Core fact table for weather readings."""

    __tablename__ = "fact_weather"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city_name = Column(String(100), nullable=False)
    state = Column(String(100), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    temperature_celsius = Column(Float, nullable=False)
    windspeed_kmh = Column(Float, nullable=False)
    winddirection_degrees = Column(Float, nullable=False)
    weathercode = Column(Integer, nullable=False)
    weather_description = Column(String(200), nullable=True)
    is_day = Column(Boolean, nullable=False)
    observation_time = Column(DateTime, nullable=False)
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    pipeline_run_id = Column(String(36), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "city_name", "observation_time", name="uq_fact_city_obstime"
        ),
        Index("ix_fact_city_obstime", "city_name", "observation_time"),
        Index("ix_fact_obstime", "observation_time"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# AGGREGATION LAYER
# ══════════════════════════════════════════════════════════════════════════════


class AggDailyWeather(Base):
    """Daily weather aggregations per city."""

    __tablename__ = "agg_daily_weather"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city_name = Column(String(100), nullable=False)
    state = Column(String(100), nullable=False)
    date = Column(DateTime, nullable=False)
    avg_temperature = Column(Float, nullable=True)
    min_temperature = Column(Float, nullable=True)
    max_temperature = Column(Float, nullable=True)
    avg_windspeed = Column(Float, nullable=True)
    max_windspeed = Column(Float, nullable=True)
    dominant_wind_direction = Column(Float, nullable=True)
    most_common_weathercode = Column(Integer, nullable=True)
    reading_count = Column(Integer, nullable=False, default=0)
    computed_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("city_name", "date", name="uq_daily_city_date"),
        Index("ix_daily_city_date", "city_name", "date"),
    )


class AggWeeklyWeather(Base):
    """Weekly weather aggregations per city."""

    __tablename__ = "agg_weekly_weather"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city_name = Column(String(100), nullable=False)
    state = Column(String(100), nullable=False)
    week_start = Column(DateTime, nullable=False)
    avg_temperature = Column(Float, nullable=True)
    min_temperature = Column(Float, nullable=True)
    max_temperature = Column(Float, nullable=True)
    avg_windspeed = Column(Float, nullable=True)
    max_windspeed = Column(Float, nullable=True)
    reading_count = Column(Integer, nullable=False, default=0)
    computed_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("city_name", "week_start", name="uq_weekly_city_week"),
    )


class AggMonthlyWeather(Base):
    """Monthly weather aggregations per city."""

    __tablename__ = "agg_monthly_weather"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city_name = Column(String(100), nullable=False)
    state = Column(String(100), nullable=False)
    month_start = Column(DateTime, nullable=False)
    avg_temperature = Column(Float, nullable=True)
    min_temperature = Column(Float, nullable=True)
    max_temperature = Column(Float, nullable=True)
    avg_windspeed = Column(Float, nullable=True)
    max_windspeed = Column(Float, nullable=True)
    reading_count = Column(Integer, nullable=False, default=0)
    computed_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("city_name", "month_start", name="uq_monthly_city_month"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS LAYER
# ══════════════════════════════════════════════════════════════════════════════


class WeatherAnomaly(Base):
    """Flagged anomalous weather readings."""

    __tablename__ = "weather_anomalies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city_name = Column(String(100), nullable=False)
    state = Column(String(100), nullable=False)
    observation_time = Column(DateTime, nullable=False)
    anomaly_type = Column(String(50), nullable=False)  # temperature, windspeed
    metric_value = Column(Float, nullable=False)
    mean_value = Column(Float, nullable=False)
    std_value = Column(Float, nullable=False)
    z_score = Column(Float, nullable=False)
    severity = Column(String(20), nullable=False)  # low, medium, high, critical
    detected_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    pipeline_run_id = Column(String(36), nullable=True)

    __table_args__ = (
        Index("ix_anomaly_city_time", "city_name", "observation_time"),
        Index("ix_anomaly_severity", "severity"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# DATA LINEAGE
# ══════════════════════════════════════════════════════════════════════════════


class DataLineage(Base):
    """Tracks data flow from source to target tables."""

    __tablename__ = "data_lineage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(String(36), nullable=False)
    source_table = Column(String(100), nullable=False)
    target_table = Column(String(100), nullable=False)
    operation = Column(String(50), nullable=False)  # insert, upsert, aggregate
    record_count = Column(Integer, nullable=False, default=0)
    status = Column(String(20), nullable=False, default="success")  # success, failed, partial
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)

    __table_args__ = (
        Index("ix_lineage_run", "pipeline_run_id"),
        Index("ix_lineage_source_target", "source_table", "target_table"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE MONITORING
# ══════════════════════════════════════════════════════════════════════════════


class PipelineRun(Base):
    """Tracks each pipeline execution for monitoring."""

    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(36), nullable=False, unique=True)
    status = Column(String(20), nullable=False, default="running")  # running, success, failed
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    records_extracted = Column(Integer, nullable=True, default=0)
    records_validated = Column(Integer, nullable=True, default=0)
    records_loaded = Column(Integer, nullable=True, default=0)
    records_failed = Column(Integer, nullable=True, default=0)
    error_message = Column(Text, nullable=True)

    __table_args__ = (
        Index("ix_pipeline_run_status", "status"),
        Index("ix_pipeline_run_started", "started_at"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# ENGINE & SESSION MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

_engine = None
_SessionFactory = None


def get_engine():
    """Get or create the SQLAlchemy engine with connection pooling."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(
            settings.DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
        )
    return _engine


def get_session_factory():
    """Get or create the session factory."""
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine())
    return _SessionFactory


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Provide a transactional database session via context manager.

    Usage:
        with get_session() as session:
            session.query(...)
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_all_tables() -> None:
    """Create all database tables. Idempotent — safe to call multiple times."""
    engine = get_engine()
    Base.metadata.create_all(engine)


def drop_all_tables() -> None:
    """Drop all database tables. USE WITH CAUTION."""
    engine = get_engine()
    Base.metadata.drop_all(engine)
