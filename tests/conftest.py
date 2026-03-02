"""
Shared test fixtures for the weather pipeline test suite.

Provides in-memory SQLite sessions, sample API responses,
and city data for unit testing.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.config.cities import City
from src.models.database import Base


@pytest.fixture
def db_engine():
    """Create an in-memory SQLite engine for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def db_session(db_engine):
    """Provide a transactional database session for tests."""
    SessionLocal = sessionmaker(bind=db_engine)
    session = SessionLocal()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def sample_city():
    """A single sample city for testing."""
    return City(
        name="TestCity",
        state="TestState",
        latitude=28.6,
        longitude=77.2,
        population_rank=1,
    )


@pytest.fixture
def sample_cities():
    """A small batch of sample cities for testing."""
    return [
        City("Delhi", "Delhi", 28.7041, 77.1025, 1),
        City("Mumbai", "Maharashtra", 19.0760, 72.8777, 2),
        City("Bangalore", "Karnataka", 12.9716, 77.5946, 3),
    ]


@pytest.fixture
def sample_api_response():
    """A sample Open-Meteo API response for a single city."""
    return {
        "latitude": 28.625,
        "longitude": 77.25,
        "generationtime_ms": 0.05,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": 224.0,
        "current_weather_units": {
            "time": "iso8601",
            "interval": "seconds",
            "temperature": "°C",
            "windspeed": "km/h",
            "winddirection": "°",
            "is_day": "",
            "weathercode": "wmo code",
        },
        "current_weather": {
            "time": "2026-03-02T09:15",
            "interval": 900,
            "temperature": 29.8,
            "windspeed": 13.0,
            "winddirection": 304,
            "is_day": 1,
            "weathercode": 1,
        },
    }


@pytest.fixture
def sample_batch_api_response():
    """A sample batch API response (list of 3 cities)."""
    return [
        {
            "latitude": 28.7,
            "longitude": 77.1,
            "current_weather": {
                "time": "2026-03-02T09:15",
                "interval": 900,
                "temperature": 30.0,
                "windspeed": 10.0,
                "winddirection": 180,
                "is_day": 1,
                "weathercode": 0,
            },
        },
        {
            "latitude": 19.08,
            "longitude": 72.88,
            "current_weather": {
                "time": "2026-03-02T09:15",
                "interval": 900,
                "temperature": 32.5,
                "windspeed": 15.0,
                "winddirection": 270,
                "is_day": 1,
                "weathercode": 2,
            },
        },
        {
            "latitude": 12.97,
            "longitude": 77.59,
            "current_weather": {
                "time": "2026-03-02T09:15",
                "interval": 900,
                "temperature": 27.0,
                "windspeed": 8.0,
                "winddirection": 90,
                "is_day": 1,
                "weathercode": 1,
            },
        },
    ]


@pytest.fixture
def sample_raw_records(sample_cities, sample_batch_api_response):
    """Sample raw records as returned by WeatherAPIClient.fetch_all_cities()."""
    return [
        {
            "city": city,
            "raw_response": response,
            "api_response_time_ms": 50.0,
            "fetched_at": datetime.utcnow().isoformat(),
        }
        for city, response in zip(sample_cities, sample_batch_api_response)
    ]


@pytest.fixture
def sample_validated_records():
    """Sample validated records ready for loading."""
    return [
        {
            "city_name": "Delhi",
            "state": "Delhi",
            "latitude": 28.7041,
            "longitude": 77.1025,
            "temperature_celsius": 30.0,
            "windspeed_kmh": 10.0,
            "winddirection_degrees": 180.0,
            "weathercode": 0,
            "is_day": True,
            "observation_time": datetime(2026, 3, 2, 9, 15),
        },
        {
            "city_name": "Mumbai",
            "state": "Maharashtra",
            "latitude": 19.0760,
            "longitude": 72.8777,
            "temperature_celsius": 32.5,
            "windspeed_kmh": 15.0,
            "winddirection_degrees": 270.0,
            "weathercode": 2,
            "is_day": True,
            "observation_time": datetime(2026, 3, 2, 9, 15),
        },
    ]
