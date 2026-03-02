"""
Unit tests for the data loading layer.

Tests bulk inserts, deduplication, and dimension loading
using an in-memory SQLite database.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from src.models.database import (
    DimCity,
    FactWeather,
    RawWeatherData,
    StagingWeatherReading,
)


class TestRawDataLoading:
    """Tests for raw data insertion."""

    def test_insert_raw_records(self, db_session):
        """Test that raw records can be inserted."""
        raw = RawWeatherData(
            city_name="Delhi",
            state="Delhi",
            latitude=28.7,
            longitude=77.1,
            raw_json='{"current_weather": {"temperature": 30.0}}',
            api_response_time_ms=50.0,
            ingested_at=datetime.utcnow(),
            pipeline_run_id="test-run-001",
        )
        db_session.add(raw)
        db_session.flush()

        count = db_session.query(RawWeatherData).count()
        assert count == 1

    def test_multiple_ingestions(self, db_session):
        """Test that multiple ingestions create separate raw records."""
        for i in range(3):
            raw = RawWeatherData(
                city_name="Delhi",
                state="Delhi",
                latitude=28.7,
                longitude=77.1,
                raw_json=f'{{"temperature": {25 + i}}}',
                ingested_at=datetime.utcnow(),
                pipeline_run_id=f"run-{i}",
            )
            db_session.add(raw)
        db_session.flush()

        count = db_session.query(RawWeatherData).count()
        assert count == 3


class TestDimensionLoading:
    """Tests for dimension table loading."""

    def test_city_dimension_insert(self, db_session):
        """Test that city dimensions can be inserted."""
        city = DimCity(
            city_name="Delhi",
            state="Delhi",
            latitude=28.7041,
            longitude=77.1025,
            population_rank=2,
        )
        db_session.add(city)
        db_session.flush()

        result = db_session.query(DimCity).filter_by(city_name="Delhi").first()
        assert result is not None
        assert result.state == "Delhi"
        assert result.population_rank == 2

    def test_city_uniqueness(self, db_session):
        """Test that duplicate city names are rejected."""
        city1 = DimCity(city_name="Delhi", state="Delhi", latitude=28.7, longitude=77.1)
        db_session.add(city1)
        db_session.flush()

        city2 = DimCity(city_name="Delhi", state="Delhi2", latitude=28.7, longitude=77.1)
        db_session.add(city2)

        with pytest.raises(Exception):  # IntegrityError
            db_session.flush()


class TestStagingLoading:
    """Tests for staging data operations."""

    def test_staging_insert(self, db_session):
        """Test staging record insertion."""
        stg = StagingWeatherReading(
            city_name="Mumbai",
            state="Maharashtra",
            latitude=19.076,
            longitude=72.878,
            temperature_celsius=32.5,
            windspeed_kmh=15.0,
            winddirection_degrees=270.0,
            weathercode=2,
            is_day=True,
            observation_time=datetime(2026, 3, 2, 9, 15),
            is_valid=True,
            pipeline_run_id="test-run-001",
        )
        db_session.add(stg)
        db_session.flush()

        count = db_session.query(StagingWeatherReading).count()
        assert count == 1


class TestFactLoading:
    """Tests for fact table operations."""

    def test_fact_insert(self, db_session):
        """Test fact record insertion."""
        fact = FactWeather(
            city_name="Bangalore",
            state="Karnataka",
            latitude=12.972,
            longitude=77.595,
            temperature_celsius=27.0,
            windspeed_kmh=8.0,
            winddirection_degrees=90.0,
            weathercode=1,
            weather_description="Mainly clear",
            is_day=True,
            observation_time=datetime(2026, 3, 2, 9, 15),
            pipeline_run_id="test-run-001",
        )
        db_session.add(fact)
        db_session.flush()

        result = db_session.query(FactWeather).first()
        assert result.city_name == "Bangalore"
        assert result.weather_description == "Mainly clear"

    def test_fact_deduplication(self, db_session):
        """Test that duplicate (city, observation_time) is rejected."""
        obs_time = datetime(2026, 3, 2, 9, 15)

        fact1 = FactWeather(
            city_name="Delhi",
            state="Delhi",
            latitude=28.7,
            longitude=77.1,
            temperature_celsius=30.0,
            windspeed_kmh=10.0,
            winddirection_degrees=180.0,
            weathercode=0,
            is_day=True,
            observation_time=obs_time,
        )
        db_session.add(fact1)
        db_session.flush()

        fact2 = FactWeather(
            city_name="Delhi",
            state="Delhi",
            latitude=28.7,
            longitude=77.1,
            temperature_celsius=31.0,
            windspeed_kmh=12.0,
            winddirection_degrees=190.0,
            weathercode=1,
            is_day=True,
            observation_time=obs_time,  # same time
        )
        db_session.add(fact2)

        with pytest.raises(Exception):  # IntegrityError due to unique constraint
            db_session.flush()
