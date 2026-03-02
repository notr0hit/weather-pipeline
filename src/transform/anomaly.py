"""
Anomaly detection for weather readings.

Uses Z-score analysis to identify unusual weather patterns
based on rolling statistics per city.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.config.settings import get_settings
from src.models.database import FactWeather, WeatherAnomaly
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _classify_severity(z_score: float) -> str:
    """Classify anomaly severity based on Z-score magnitude."""
    abs_z = abs(z_score)
    if abs_z >= 4.0:
        return "critical"
    elif abs_z >= 3.0:
        return "high"
    elif abs_z >= 2.5:
        return "medium"
    else:
        return "low"


def detect_anomalies(
    session: Session,
    pipeline_run_id: str,
    lookback_hours: int | None = None,
) -> int:
    """Detect anomalous weather readings using Z-score analysis.

    For each city, computes rolling mean and standard deviation over the
    lookback window. Readings with Z-score exceeding the threshold are
    flagged as anomalies.

    Checks both temperature and windspeed.

    Args:
        session: Database session.
        pipeline_run_id: UUID of the current pipeline run.
        lookback_hours: Number of hours to look back for statistics.

    Returns:
        Number of anomalies detected.
    """
    settings = get_settings()
    lookback = lookback_hours or settings.ANOMALY_LOOKBACK_HOURS
    z_threshold = settings.ANOMALY_Z_THRESHOLD

    cutoff_time = datetime.utcnow() - timedelta(hours=lookback)

    # Get distinct cities that have recent readings
    cities = (
        session.query(FactWeather.city_name, FactWeather.state)
        .filter(FactWeather.observation_time >= cutoff_time)
        .distinct()
        .all()
    )

    anomaly_count = 0

    for city_name, state in cities:
        # Compute rolling stats for this city
        stats = (
            session.query(
                func.avg(FactWeather.temperature_celsius).label("avg_temp"),
                func.stddev(FactWeather.temperature_celsius).label("std_temp"),
                func.avg(FactWeather.windspeed_kmh).label("avg_wind"),
                func.stddev(FactWeather.windspeed_kmh).label("std_wind"),
                func.count(FactWeather.id).label("count"),
            )
            .filter(
                FactWeather.city_name == city_name,
                FactWeather.observation_time >= cutoff_time,
            )
            .first()
        )

        if not stats or stats.count < 3:
            # Need at least 3 readings for meaningful statistics
            continue

        avg_temp = float(stats.avg_temp) if stats.avg_temp else 0
        std_temp = float(stats.std_temp) if stats.std_temp else 0
        avg_wind = float(stats.avg_wind) if stats.avg_wind else 0
        std_wind = float(stats.std_wind) if stats.std_wind else 0

        # Get the latest reading for this city
        latest = (
            session.query(FactWeather)
            .filter(
                FactWeather.city_name == city_name,
                FactWeather.observation_time >= cutoff_time,
            )
            .order_by(FactWeather.observation_time.desc())
            .first()
        )

        if not latest:
            continue

        # Check temperature anomaly
        if std_temp > 0:
            temp_z = (latest.temperature_celsius - avg_temp) / std_temp
            if abs(temp_z) >= z_threshold:
                anomaly = WeatherAnomaly(
                    city_name=city_name,
                    state=state,
                    observation_time=latest.observation_time,
                    anomaly_type="temperature",
                    metric_value=latest.temperature_celsius,
                    mean_value=round(avg_temp, 2),
                    std_value=round(std_temp, 2),
                    z_score=round(temp_z, 2),
                    severity=_classify_severity(temp_z),
                    detected_at=datetime.utcnow(),
                    pipeline_run_id=pipeline_run_id,
                )
                session.add(anomaly)
                anomaly_count += 1

        # Check windspeed anomaly
        if std_wind > 0:
            wind_z = (latest.windspeed_kmh - avg_wind) / std_wind
            if abs(wind_z) >= z_threshold:
                anomaly = WeatherAnomaly(
                    city_name=city_name,
                    state=state,
                    observation_time=latest.observation_time,
                    anomaly_type="windspeed",
                    metric_value=latest.windspeed_kmh,
                    mean_value=round(avg_wind, 2),
                    std_value=round(std_wind, 2),
                    z_score=round(wind_z, 2),
                    severity=_classify_severity(wind_z),
                    detected_at=datetime.utcnow(),
                    pipeline_run_id=pipeline_run_id,
                )
                session.add(anomaly)
                anomaly_count += 1

    session.flush()
    logger.info(
        f"Anomaly detection complete: {anomaly_count} anomalies found "
        f"across {len(cities)} cities",
        anomaly_count=anomaly_count,
        cities_checked=len(cities),
    )
    return anomaly_count
