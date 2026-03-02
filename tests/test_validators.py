"""
Unit tests for the data validation layer.

Tests Pydantic validators with valid, invalid, and edge-case data.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from src.transform.validators import WeatherReading, parse_raw_to_reading, validate_batch


class TestWeatherReading:
    """Tests for the WeatherReading Pydantic model."""

    def test_valid_reading(self):
        """Test a valid weather reading passes validation."""
        reading = WeatherReading(
            city_name="Delhi",
            state="Delhi",
            latitude=28.7,
            longitude=77.1,
            temperature_celsius=30.0,
            windspeed_kmh=10.0,
            winddirection_degrees=180.0,
            weathercode=0,
            is_day=True,
            observation_time=datetime(2026, 3, 2, 9, 0),
        )
        assert reading.temperature_celsius == 30.0
        assert reading.city_name == "Delhi"

    def test_temperature_too_high(self):
        """Temperature above 60°C should fail validation."""
        with pytest.raises(Exception):
            WeatherReading(
                city_name="Delhi",
                state="Delhi",
                latitude=28.7,
                longitude=77.1,
                temperature_celsius=65.0,  # > 60
                windspeed_kmh=10.0,
                winddirection_degrees=180.0,
                weathercode=0,
                is_day=True,
                observation_time=datetime(2026, 3, 2, 9, 0),
            )

    def test_temperature_too_low(self):
        """Temperature below -60°C should fail validation."""
        with pytest.raises(Exception):
            WeatherReading(
                city_name="Delhi",
                state="Delhi",
                latitude=28.7,
                longitude=77.1,
                temperature_celsius=-65.0,  # < -60
                windspeed_kmh=10.0,
                winddirection_degrees=180.0,
                weathercode=0,
                is_day=True,
                observation_time=datetime(2026, 3, 2, 9, 0),
            )

    def test_negative_windspeed(self):
        """Negative windspeed should fail validation."""
        with pytest.raises(Exception):
            WeatherReading(
                city_name="Delhi",
                state="Delhi",
                latitude=28.7,
                longitude=77.1,
                temperature_celsius=30.0,
                windspeed_kmh=-5.0,  # negative
                winddirection_degrees=180.0,
                weathercode=0,
                is_day=True,
                observation_time=datetime(2026, 3, 2, 9, 0),
            )

    def test_invalid_weathercode(self):
        """Invalid WMO weather code should fail validation."""
        with pytest.raises(Exception):
            WeatherReading(
                city_name="Delhi",
                state="Delhi",
                latitude=28.7,
                longitude=77.1,
                temperature_celsius=30.0,
                windspeed_kmh=10.0,
                winddirection_degrees=180.0,
                weathercode=999,  # invalid
                is_day=True,
                observation_time=datetime(2026, 3, 2, 9, 0),
            )

    def test_boundary_values(self):
        """Test at boundary values (should pass)."""
        reading = WeatherReading(
            city_name="X",
            state="Y",
            latitude=0.0,
            longitude=0.0,
            temperature_celsius=-60.0,  # exactly at min
            windspeed_kmh=0.0,  # exactly at min
            winddirection_degrees=0.0,  # exactly at min
            weathercode=0,
            is_day=False,
            observation_time=datetime(2026, 1, 1, 0, 0),
        )
        assert reading.temperature_celsius == -60.0

    def test_wind_direction_360(self):
        """Wind direction at exactly 360° should pass (due North)."""
        reading = WeatherReading(
            city_name="Delhi",
            state="Delhi",
            latitude=28.7,
            longitude=77.1,
            temperature_celsius=30.0,
            windspeed_kmh=10.0,
            winddirection_degrees=360.0,
            weathercode=0,
            is_day=True,
            observation_time=datetime(2026, 3, 2, 9, 0),
        )
        assert reading.winddirection_degrees == 360.0


class TestParseRawToReading:
    """Tests for raw API response parsing."""

    def test_parse_success(self, sample_city, sample_api_response):
        raw_record = {
            "city": sample_city,
            "raw_response": sample_api_response,
        }
        parsed = parse_raw_to_reading(raw_record)

        assert parsed["city_name"] == "TestCity"
        assert parsed["temperature_celsius"] == 29.8
        assert parsed["windspeed_kmh"] == 13.0
        assert parsed["winddirection_degrees"] == 304
        assert parsed["weathercode"] == 1
        assert parsed["is_day"] is True

    def test_parse_missing_fields(self, sample_city):
        """Parse should use defaults for missing fields."""
        raw_record = {
            "city": sample_city,
            "raw_response": {"current_weather": {}},
        }
        parsed = parse_raw_to_reading(raw_record)
        assert parsed["temperature_celsius"] == 0.0
        assert parsed["windspeed_kmh"] == 0.0


class TestValidateBatch:
    """Tests for batch validation."""

    def test_all_valid(self, sample_raw_records):
        valid, invalid = validate_batch(sample_raw_records)
        assert len(valid) == 3
        assert len(invalid) == 0

    def test_failed_extraction_marked_invalid(self, sample_city):
        """Records with no API response are marked invalid."""
        records = [
            {
                "city": sample_city,
                "raw_response": None,
                "error": "Connection failed",
            }
        ]
        valid, invalid = validate_batch(records)
        assert len(valid) == 0
        assert len(invalid) == 1
        assert invalid[0]["is_valid"] is False
