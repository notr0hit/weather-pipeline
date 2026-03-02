"""
Trend analysis and city comparison utilities.

Provides moving averages, rate-of-change calculations,
and city-to-city comparison views.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.config.settings import get_settings
from src.models.database import FactWeather
from src.utils.logger import get_logger

logger = get_logger(__name__)


def compute_moving_averages(
    session: Session,
    city_name: str,
    window: int | None = None,
) -> dict | None:
    """Compute moving averages for a city over the last N readings.

    Args:
        session: Database session.
        city_name: Name of the city.
        window: Number of readings to include (default from settings).

    Returns:
        Dict with moving average metrics, or None if insufficient data.
    """
    settings = get_settings()
    window = window or settings.MOVING_AVERAGE_WINDOW

    readings = (
        session.query(FactWeather)
        .filter(FactWeather.city_name == city_name)
        .order_by(FactWeather.observation_time.desc())
        .limit(window)
        .all()
    )

    if len(readings) < 2:
        return None

    temps = [r.temperature_celsius for r in readings]
    winds = [r.windspeed_kmh for r in readings]

    return {
        "city_name": city_name,
        "window_size": len(readings),
        "time_range_start": readings[-1].observation_time.isoformat(),
        "time_range_end": readings[0].observation_time.isoformat(),
        "temperature": {
            "moving_avg": round(sum(temps) / len(temps), 2),
            "moving_min": round(min(temps), 2),
            "moving_max": round(max(temps), 2),
            "latest": temps[0],
        },
        "windspeed": {
            "moving_avg": round(sum(winds) / len(winds), 2),
            "moving_min": round(min(winds), 2),
            "moving_max": round(max(winds), 2),
            "latest": winds[0],
        },
    }


def compute_rate_of_change(
    session: Session,
    city_name: str,
) -> dict | None:
    """Compute temperature rate of change (°C/hour) for a city.

    Compares the two most recent readings.

    Args:
        session: Database session.
        city_name: Name of the city.

    Returns:
        Dict with rate of change data, or None if insufficient data.
    """
    readings = (
        session.query(FactWeather)
        .filter(FactWeather.city_name == city_name)
        .order_by(FactWeather.observation_time.desc())
        .limit(2)
        .all()
    )

    if len(readings) < 2:
        return None

    latest, previous = readings[0], readings[1]
    time_diff = (latest.observation_time - previous.observation_time).total_seconds()

    if time_diff == 0:
        return None

    hours = time_diff / 3600
    temp_change = latest.temperature_celsius - previous.temperature_celsius
    wind_change = latest.windspeed_kmh - previous.windspeed_kmh

    return {
        "city_name": city_name,
        "time_interval_hours": round(hours, 2),
        "temperature": {
            "previous": previous.temperature_celsius,
            "current": latest.temperature_celsius,
            "change": round(temp_change, 2),
            "rate_per_hour": round(temp_change / hours, 2),
        },
        "windspeed": {
            "previous": previous.windspeed_kmh,
            "current": latest.windspeed_kmh,
            "change": round(wind_change, 2),
            "rate_per_hour": round(wind_change / hours, 2),
        },
    }


def get_city_comparison(
    session: Session,
    city_names: list[str],
) -> list[dict]:
    """Compare latest weather readings across multiple cities.

    Args:
        session: Database session.
        city_names: List of city names to compare.

    Returns:
        List of dicts with latest readings per city, sorted by temperature.
    """
    comparisons = []

    for city_name in city_names:
        latest = (
            session.query(FactWeather)
            .filter(FactWeather.city_name == city_name)
            .order_by(FactWeather.observation_time.desc())
            .first()
        )

        if latest:
            comparisons.append({
                "city_name": city_name,
                "state": latest.state,
                "temperature_celsius": latest.temperature_celsius,
                "windspeed_kmh": latest.windspeed_kmh,
                "winddirection_degrees": latest.winddirection_degrees,
                "weathercode": latest.weathercode,
                "weather_description": latest.weather_description,
                "is_day": latest.is_day,
                "observation_time": latest.observation_time.isoformat(),
            })

    # Sort by temperature descending (hottest first)
    comparisons.sort(key=lambda x: x["temperature_celsius"], reverse=True)

    logger.info(
        f"City comparison for {len(comparisons)} cities",
        city_count=len(comparisons),
    )
    return comparisons


def get_state_summary(session: Session, state: str) -> dict | None:
    """Get aggregated weather summary for all cities in a state.

    Args:
        session: Database session.
        state: State name.

    Returns:
        Dict with state-level weather summary.
    """
    # Get latest reading per city in the state
    cutoff = datetime.utcnow() - timedelta(hours=1)

    result = (
        session.query(
            func.count(func.distinct(FactWeather.city_name)).label("city_count"),
            func.avg(FactWeather.temperature_celsius).label("avg_temp"),
            func.min(FactWeather.temperature_celsius).label("min_temp"),
            func.max(FactWeather.temperature_celsius).label("max_temp"),
            func.avg(FactWeather.windspeed_kmh).label("avg_wind"),
        )
        .filter(
            FactWeather.state == state,
            FactWeather.observation_time >= cutoff,
        )
        .first()
    )

    if not result or not result.city_count:
        return None

    return {
        "state": state,
        "city_count": result.city_count,
        "avg_temperature": round(float(result.avg_temp), 2) if result.avg_temp else None,
        "min_temperature": float(result.min_temp) if result.min_temp else None,
        "max_temperature": float(result.max_temp) if result.max_temp else None,
        "avg_windspeed": round(float(result.avg_wind), 2) if result.avg_wind else None,
    }
