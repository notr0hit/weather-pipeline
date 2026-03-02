"""
Weather data aggregation computations.

Computes daily, weekly, and monthly rollups of weather readings
with idempotent upserts (safe to re-run).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from collections import Counter

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.models.database import (
    AggDailyWeather,
    AggMonthlyWeather,
    AggWeeklyWeather,
    FactWeather,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


def compute_daily_aggregations(session: Session, target_date: datetime | None = None) -> int:
    """Compute daily weather aggregations for all cities.

    Args:
        session: Database session.
        target_date: Date to compute aggregations for. Defaults to today (UTC).

    Returns:
        Number of city-day aggregations computed.
    """
    if target_date is None:
        target_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        target_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)

    next_day = target_date + timedelta(days=1)

    # Query aggregated metrics per city for the target date
    results = (
        session.query(
            FactWeather.city_name,
            FactWeather.state,
            func.avg(FactWeather.temperature_celsius).label("avg_temp"),
            func.min(FactWeather.temperature_celsius).label("min_temp"),
            func.max(FactWeather.temperature_celsius).label("max_temp"),
            func.avg(FactWeather.windspeed_kmh).label("avg_wind"),
            func.max(FactWeather.windspeed_kmh).label("max_wind"),
            func.count(FactWeather.id).label("reading_count"),
        )
        .filter(
            FactWeather.observation_time >= target_date,
            FactWeather.observation_time < next_day,
        )
        .group_by(FactWeather.city_name, FactWeather.state)
        .all()
    )

    count = 0
    for row in results:
        # Compute dominant wind direction separately (mode)
        wind_dirs = (
            session.query(FactWeather.winddirection_degrees)
            .filter(
                FactWeather.city_name == row.city_name,
                FactWeather.observation_time >= target_date,
                FactWeather.observation_time < next_day,
            )
            .all()
        )
        # Bucket wind directions into 8 compass directions (45° each)
        bucketed = [round(d[0] / 45) * 45 % 360 for d in wind_dirs]
        dominant_dir = Counter(bucketed).most_common(1)[0][0] if bucketed else None

        # Compute most common weather code
        wcodes = (
            session.query(FactWeather.weathercode)
            .filter(
                FactWeather.city_name == row.city_name,
                FactWeather.observation_time >= target_date,
                FactWeather.observation_time < next_day,
            )
            .all()
        )
        common_wcode = Counter(w[0] for w in wcodes).most_common(1)[0][0] if wcodes else None

        stmt = pg_insert(AggDailyWeather).values(
            city_name=row.city_name,
            state=row.state,
            date=target_date,
            avg_temperature=round(float(row.avg_temp), 2) if row.avg_temp else None,
            min_temperature=float(row.min_temp) if row.min_temp else None,
            max_temperature=float(row.max_temp) if row.max_temp else None,
            avg_windspeed=round(float(row.avg_wind), 2) if row.avg_wind else None,
            max_windspeed=float(row.max_wind) if row.max_wind else None,
            dominant_wind_direction=dominant_dir,
            most_common_weathercode=common_wcode,
            reading_count=int(row.reading_count),
            computed_at=datetime.utcnow(),
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_daily_city_date",
            set_={
                "avg_temperature": stmt.excluded.avg_temperature,
                "min_temperature": stmt.excluded.min_temperature,
                "max_temperature": stmt.excluded.max_temperature,
                "avg_windspeed": stmt.excluded.avg_windspeed,
                "max_windspeed": stmt.excluded.max_windspeed,
                "dominant_wind_direction": stmt.excluded.dominant_wind_direction,
                "most_common_weathercode": stmt.excluded.most_common_weathercode,
                "reading_count": stmt.excluded.reading_count,
                "computed_at": datetime.utcnow(),
            },
        )
        session.execute(stmt)
        count += 1

    session.flush()
    logger.info(
        f"Daily aggregations computed for {count} cities on {target_date.date()}",
        city_count=count,
        date=str(target_date.date()),
    )
    return count


def compute_weekly_aggregations(session: Session, week_start: datetime | None = None) -> int:
    """Compute weekly weather aggregations for all cities.

    Args:
        session: Database session.
        week_start: Monday of the week to aggregate. Defaults to current week.

    Returns:
        Number of city-week aggregations computed.
    """
    if week_start is None:
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = today - timedelta(days=today.weekday())  # Monday

    week_end = week_start + timedelta(days=7)

    results = (
        session.query(
            FactWeather.city_name,
            FactWeather.state,
            func.avg(FactWeather.temperature_celsius).label("avg_temp"),
            func.min(FactWeather.temperature_celsius).label("min_temp"),
            func.max(FactWeather.temperature_celsius).label("max_temp"),
            func.avg(FactWeather.windspeed_kmh).label("avg_wind"),
            func.max(FactWeather.windspeed_kmh).label("max_wind"),
            func.count(FactWeather.id).label("reading_count"),
        )
        .filter(
            FactWeather.observation_time >= week_start,
            FactWeather.observation_time < week_end,
        )
        .group_by(FactWeather.city_name, FactWeather.state)
        .all()
    )

    count = 0
    for row in results:
        stmt = pg_insert(AggWeeklyWeather).values(
            city_name=row.city_name,
            state=row.state,
            week_start=week_start,
            avg_temperature=round(float(row.avg_temp), 2) if row.avg_temp else None,
            min_temperature=float(row.min_temp) if row.min_temp else None,
            max_temperature=float(row.max_temp) if row.max_temp else None,
            avg_windspeed=round(float(row.avg_wind), 2) if row.avg_wind else None,
            max_windspeed=float(row.max_wind) if row.max_wind else None,
            reading_count=int(row.reading_count),
            computed_at=datetime.utcnow(),
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_weekly_city_week",
            set_={
                "avg_temperature": stmt.excluded.avg_temperature,
                "min_temperature": stmt.excluded.min_temperature,
                "max_temperature": stmt.excluded.max_temperature,
                "avg_windspeed": stmt.excluded.avg_windspeed,
                "max_windspeed": stmt.excluded.max_windspeed,
                "reading_count": stmt.excluded.reading_count,
                "computed_at": datetime.utcnow(),
            },
        )
        session.execute(stmt)
        count += 1

    session.flush()
    logger.info(
        f"Weekly aggregations computed for {count} cities (week of {week_start.date()})",
        city_count=count,
    )
    return count


def compute_monthly_aggregations(session: Session, month_start: datetime | None = None) -> int:
    """Compute monthly weather aggregations for all cities.

    Args:
        session: Database session.
        month_start: First day of the month. Defaults to current month.

    Returns:
        Number of city-month aggregations computed.
    """
    if month_start is None:
        today = datetime.utcnow()
        month_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # Compute first day of next month
    if month_start.month == 12:
        month_end = month_start.replace(year=month_start.year + 1, month=1)
    else:
        month_end = month_start.replace(month=month_start.month + 1)

    results = (
        session.query(
            FactWeather.city_name,
            FactWeather.state,
            func.avg(FactWeather.temperature_celsius).label("avg_temp"),
            func.min(FactWeather.temperature_celsius).label("min_temp"),
            func.max(FactWeather.temperature_celsius).label("max_temp"),
            func.avg(FactWeather.windspeed_kmh).label("avg_wind"),
            func.max(FactWeather.windspeed_kmh).label("max_wind"),
            func.count(FactWeather.id).label("reading_count"),
        )
        .filter(
            FactWeather.observation_time >= month_start,
            FactWeather.observation_time < month_end,
        )
        .group_by(FactWeather.city_name, FactWeather.state)
        .all()
    )

    count = 0
    for row in results:
        stmt = pg_insert(AggMonthlyWeather).values(
            city_name=row.city_name,
            state=row.state,
            month_start=month_start,
            avg_temperature=round(float(row.avg_temp), 2) if row.avg_temp else None,
            min_temperature=float(row.min_temp) if row.min_temp else None,
            max_temperature=float(row.max_temp) if row.max_temp else None,
            avg_windspeed=round(float(row.avg_wind), 2) if row.avg_wind else None,
            max_windspeed=float(row.max_wind) if row.max_wind else None,
            reading_count=int(row.reading_count),
            computed_at=datetime.utcnow(),
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_monthly_city_month",
            set_={
                "avg_temperature": stmt.excluded.avg_temperature,
                "min_temperature": stmt.excluded.min_temperature,
                "max_temperature": stmt.excluded.max_temperature,
                "avg_windspeed": stmt.excluded.avg_windspeed,
                "max_windspeed": stmt.excluded.max_windspeed,
                "reading_count": stmt.excluded.reading_count,
                "computed_at": datetime.utcnow(),
            },
        )
        session.execute(stmt)
        count += 1

    session.flush()
    logger.info(
        f"Monthly aggregations computed for {count} cities ({month_start.strftime('%Y-%m')})",
        city_count=count,
    )
    return count
