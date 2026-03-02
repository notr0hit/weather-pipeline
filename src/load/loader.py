"""
Data loading operations for weather pipeline.

Handles bulk inserts, upserts, and deduplication for all database layers.
"""

from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.config.cities import INDIAN_CITIES
from src.models.database import (
    DimCity,
    FactWeather,
    RawWeatherData,
    StagingWeatherReading,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

# WMO Weather interpretation codes → human-readable descriptions
WMO_CODES = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Foggy",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Light freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snowfall",
    73: "Moderate snowfall",
    75: "Heavy snowfall",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}


def load_city_dimensions(session: Session) -> int:
    """Populate or update the dim_cities table from the static city list.

    Uses PostgreSQL upsert (INSERT ... ON CONFLICT DO UPDATE).

    Returns:
        Number of cities loaded/updated.
    """
    logger.info("Loading city dimensions", city_count=len(INDIAN_CITIES))

    for city in INDIAN_CITIES:
        stmt = pg_insert(DimCity).values(
            city_name=city.name,
            state=city.state,
            latitude=city.latitude,
            longitude=city.longitude,
            population_rank=city.population_rank,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["city_name"],
            set_={
                "state": stmt.excluded.state,
                "latitude": stmt.excluded.latitude,
                "longitude": stmt.excluded.longitude,
                "population_rank": stmt.excluded.population_rank,
                "updated_at": datetime.utcnow(),
            },
        )
        session.execute(stmt)

    session.flush()
    logger.info("City dimensions loaded", city_count=len(INDIAN_CITIES))
    return len(INDIAN_CITIES)


def insert_raw_data(
    session: Session, raw_records: list[dict], pipeline_run_id: str
) -> int:
    """Bulk insert raw API responses into raw_weather_data.

    Args:
        session: Database session.
        raw_records: List of dicts from WeatherAPIClient.fetch_all_cities().
        pipeline_run_id: UUID of the current pipeline run.

    Returns:
        Number of records inserted.
    """
    rows = []
    for record in raw_records:
        if record.get("raw_response") is None:
            continue  # skip failed extractions

        city = record["city"]
        rows.append({
            "city_name": city.name,
            "state": city.state,
            "latitude": city.latitude,
            "longitude": city.longitude,
            "raw_json": json.dumps(record["raw_response"]),
            "api_response_time_ms": record.get("api_response_time_ms"),
            "ingested_at": datetime.utcnow(),
            "pipeline_run_id": pipeline_run_id,
        })

    if rows:
        session.execute(RawWeatherData.__table__.insert(), rows)
        session.flush()

    logger.info(f"Inserted {len(rows)} raw records", record_count=len(rows))
    return len(rows)


def upsert_staging(
    session: Session, validated_records: list[dict], pipeline_run_id: str
) -> int:
    """Upsert validated records into staging table.

    Uses ON CONFLICT to handle duplicate (city_name, observation_time).

    Args:
        session: Database session.
        validated_records: List of validated weather reading dicts.
        pipeline_run_id: UUID of the current pipeline run.

    Returns:
        Number of records upserted.
    """
    count = 0
    for record in validated_records:
        stmt = pg_insert(StagingWeatherReading).values(
            city_name=record["city_name"],
            state=record["state"],
            latitude=record["latitude"],
            longitude=record["longitude"],
            temperature_celsius=record["temperature_celsius"],
            windspeed_kmh=record["windspeed_kmh"],
            winddirection_degrees=record["winddirection_degrees"],
            weathercode=record["weathercode"],
            is_day=record["is_day"],
            observation_time=record["observation_time"],
            ingested_at=datetime.utcnow(),
            is_valid=record.get("is_valid", True),
            validation_errors=record.get("validation_errors"),
            pipeline_run_id=pipeline_run_id,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_stg_city_obstime",
            set_={
                "temperature_celsius": stmt.excluded.temperature_celsius,
                "windspeed_kmh": stmt.excluded.windspeed_kmh,
                "winddirection_degrees": stmt.excluded.winddirection_degrees,
                "weathercode": stmt.excluded.weathercode,
                "is_day": stmt.excluded.is_day,
                "ingested_at": datetime.utcnow(),
                "pipeline_run_id": pipeline_run_id,
            },
        )
        session.execute(stmt)
        count += 1

    session.flush()
    logger.info(f"Upserted {count} staging records", record_count=count)
    return count


def load_facts(
    session: Session, validated_records: list[dict], pipeline_run_id: str
) -> int:
    """Load validated records into the fact table.

    Uses ON CONFLICT to handle duplicate (city_name, observation_time).

    Args:
        session: Database session.
        validated_records: List of validated weather reading dicts.
        pipeline_run_id: UUID of the current pipeline run.

    Returns:
        Number of records loaded.
    """
    count = 0
    for record in validated_records:
        weathercode = record["weathercode"]
        weather_desc = WMO_CODES.get(weathercode, f"Unknown ({weathercode})")

        stmt = pg_insert(FactWeather).values(
            city_name=record["city_name"],
            state=record["state"],
            latitude=record["latitude"],
            longitude=record["longitude"],
            temperature_celsius=record["temperature_celsius"],
            windspeed_kmh=record["windspeed_kmh"],
            winddirection_degrees=record["winddirection_degrees"],
            weathercode=weathercode,
            weather_description=weather_desc,
            is_day=record["is_day"],
            observation_time=record["observation_time"],
            ingested_at=datetime.utcnow(),
            pipeline_run_id=pipeline_run_id,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_fact_city_obstime",
            set_={
                "temperature_celsius": stmt.excluded.temperature_celsius,
                "windspeed_kmh": stmt.excluded.windspeed_kmh,
                "winddirection_degrees": stmt.excluded.winddirection_degrees,
                "weathercode": stmt.excluded.weathercode,
                "weather_description": weather_desc,
                "is_day": stmt.excluded.is_day,
                "ingested_at": datetime.utcnow(),
                "pipeline_run_id": pipeline_run_id,
            },
        )
        session.execute(stmt)
        count += 1

    session.flush()
    logger.info(f"Loaded {count} fact records", record_count=count)
    return count
