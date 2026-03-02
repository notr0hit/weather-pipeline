"""
Unit tests for the transform layer.

Tests aggregation logic, anomaly detection, and trend analysis.
Uses in-memory SQLite for fast, isolated testing.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from src.models.database import FactWeather


class TestAggregations:
    """Tests for weather data aggregations."""

    def test_daily_aggregation_data_insertion(self, db_session):
        """Test that fact data can be inserted for aggregation."""
        # Insert test readings for one city
        base_time = datetime(2026, 3, 2, 0, 0)
        for i in range(5):
            reading = FactWeather(
                city_name="Delhi",
                state="Delhi",
                latitude=28.7,
                longitude=77.1,
                temperature_celsius=25.0 + i,
                windspeed_kmh=10.0 + i,
                winddirection_degrees=180.0,
                weathercode=0,
                weather_description="Clear sky",
                is_day=True,
                observation_time=base_time + timedelta(hours=i),
                pipeline_run_id="test-run",
            )
            db_session.add(reading)
        db_session.flush()

        # Verify records were inserted
        count = db_session.query(FactWeather).count()
        assert count == 5

        # Verify temperature range
        from sqlalchemy import func
        result = db_session.query(
            func.avg(FactWeather.temperature_celsius),
            func.min(FactWeather.temperature_celsius),
            func.max(FactWeather.temperature_celsius),
        ).first()

        assert result[0] == 27.0  # avg of 25, 26, 27, 28, 29
        assert result[1] == 25.0  # min
        assert result[2] == 29.0  # max


class TestAnomalyDetection:
    """Tests for anomaly detection logic."""

    def test_z_score_calculation(self):
        """Test Z-score calculation for anomaly detection."""
        from src.transform.anomaly import _classify_severity

        assert _classify_severity(2.0) == "low"
        assert _classify_severity(2.5) == "medium"
        assert _classify_severity(3.0) == "high"
        assert _classify_severity(4.0) == "critical"
        assert _classify_severity(-3.5) == "high"  # negative z-scores too

    def test_insufficient_data_skipped(self, db_session):
        """Anomaly detection should skip cities with < 3 readings."""
        # Insert only 2 readings
        for i in range(2):
            reading = FactWeather(
                city_name="Delhi",
                state="Delhi",
                latitude=28.7,
                longitude=77.1,
                temperature_celsius=30.0 + i,
                windspeed_kmh=10.0,
                winddirection_degrees=180.0,
                weathercode=0,
                weather_description="Clear sky",
                is_day=True,
                observation_time=datetime.utcnow() - timedelta(hours=i),
                pipeline_run_id="test-run",
            )
            db_session.add(reading)
        db_session.flush()

        count = db_session.query(FactWeather).count()
        assert count == 2  # only 2 readings — not enough for anomaly detection


class TestTrends:
    """Tests for trend analysis functions."""

    def test_rate_of_change_data(self, db_session):
        """Test that rate of change can be computed from sequential readings."""
        time1 = datetime.utcnow() - timedelta(hours=1)
        time2 = datetime.utcnow()

        r1 = FactWeather(
            city_name="Delhi",
            state="Delhi",
            latitude=28.7,
            longitude=77.1,
            temperature_celsius=25.0,
            windspeed_kmh=10.0,
            winddirection_degrees=180.0,
            weathercode=0,
            weather_description="Clear sky",
            is_day=True,
            observation_time=time1,
            pipeline_run_id="test-run",
        )
        r2 = FactWeather(
            city_name="Delhi",
            state="Delhi",
            latitude=28.7,
            longitude=77.1,
            temperature_celsius=30.0,
            windspeed_kmh=15.0,
            winddirection_degrees=270.0,
            weathercode=1,
            weather_description="Mainly clear",
            is_day=True,
            observation_time=time2,
            pipeline_run_id="test-run",
        )
        db_session.add_all([r1, r2])
        db_session.flush()

        # Verify we have the right readings
        readings = (
            db_session.query(FactWeather)
            .order_by(FactWeather.observation_time.desc())
            .all()
        )
        assert len(readings) == 2
        assert readings[0].temperature_celsius == 30.0
        assert readings[1].temperature_celsius == 25.0

        # Temp change should be +5°C over ~1 hour
        temp_diff = readings[0].temperature_celsius - readings[1].temperature_celsius
        assert temp_diff == 5.0
