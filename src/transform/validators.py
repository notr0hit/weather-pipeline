"""
Pydantic validators for weather data quality.

Validates API responses before they enter the staging layer.
Invalid records are flagged with specific error reasons.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Valid WMO weather interpretation codes
VALID_WMO_CODES = {
    0, 1, 2, 3, 45, 48,
    51, 53, 55, 56, 57,
    61, 63, 65, 66, 67,
    71, 73, 75, 77,
    80, 81, 82, 85, 86,
    95, 96, 99,
}


class WeatherReading(BaseModel):
    """Validated weather reading with field-level constraints."""

    city_name: str = Field(..., min_length=1, max_length=100)
    state: str = Field(..., min_length=1, max_length=100)
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    temperature_celsius: float = Field(..., ge=-60.0, le=60.0)
    windspeed_kmh: float = Field(..., ge=0.0, le=500.0)
    winddirection_degrees: float = Field(..., ge=0.0, le=360.0)
    weathercode: int
    is_day: bool
    observation_time: datetime

    @field_validator("weathercode")
    @classmethod
    def validate_weathercode(cls, v: int) -> int:
        if v not in VALID_WMO_CODES:
            raise ValueError(f"Invalid WMO weather code: {v}")
        return v

    @field_validator("observation_time")
    @classmethod
    def validate_observation_time(cls, v: datetime) -> datetime:
        # Observation time should not be more than 24 hours in the future
        now = datetime.utcnow()
        from datetime import timedelta
        if v > now + timedelta(hours=24):
            raise ValueError(f"Observation time {v} is too far in the future")
        return v


def parse_raw_to_reading(raw_record: dict) -> dict[str, Any]:
    """Parse a raw API response record into a flat dict for validation.

    Args:
        raw_record: Dict from WeatherAPIClient with 'city' and 'raw_response'.

    Returns:
        Flat dict suitable for WeatherReading validation.
    """
    city = raw_record["city"]
    response = raw_record["raw_response"]
    current = response.get("current_weather", {})

    # Parse observation time from API response
    time_str = current.get("time", "")
    try:
        obs_time = datetime.fromisoformat(time_str)
    except (ValueError, TypeError):
        obs_time = datetime.utcnow()

    return {
        "city_name": city.name,
        "state": city.state,
        "latitude": city.latitude,
        "longitude": city.longitude,
        "temperature_celsius": current.get("temperature", 0.0),
        "windspeed_kmh": current.get("windspeed", 0.0),
        "winddirection_degrees": current.get("winddirection", 0.0),
        "weathercode": current.get("weathercode", 0),
        "is_day": bool(current.get("is_day", 1)),
        "observation_time": obs_time,
    }


def validate_batch(
    raw_records: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Validate a batch of raw records using Pydantic models.

    Args:
        raw_records: List of raw dicts from the API client.

    Returns:
        Tuple of (valid_records, invalid_records).
        Each valid record is a dict ready for database insertion.
        Each invalid record includes the original data plus error info.
    """
    valid = []
    invalid = []

    for record in raw_records:
        # Skip records that failed extraction
        if record.get("raw_response") is None:
            invalid.append({
                "city_name": record["city"].name,
                "error": record.get("error", "No API response"),
                "is_valid": False,
            })
            continue

        try:
            parsed = parse_raw_to_reading(record)
            reading = WeatherReading(**parsed)
            valid.append(reading.model_dump())
        except Exception as e:
            city = record["city"]
            invalid.append({
                "city_name": city.name,
                "error": str(e),
                "is_valid": False,
                "validation_errors": str(e),
            })
            logger.warning(
                f"Validation failed for {city.name}: {e}",
                city=city.name,
                error=str(e),
            )

    logger.info(
        f"Validation complete: {len(valid)} valid, {len(invalid)} invalid",
        valid_count=len(valid),
        invalid_count=len(invalid),
    )

    return valid, invalid
