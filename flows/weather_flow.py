"""
Prefect flow for the Weather ETL pipeline.

Orchestrates: Extract → Validate → Load (raw + staging + facts)
              → Aggregate → Detect Anomalies → Track Lineage

Runs on a 5-minute interval schedule.
"""

from __future__ import annotations

import uuid

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

from src.config.settings import get_settings
from src.extract.weather_api import WeatherAPIClient
from src.lineage.tracker import LineageTracker
from src.load.loader import (
    insert_raw_data,
    load_city_dimensions,
    load_facts,
    upsert_staging,
)
from src.models.database import create_all_tables, get_session
from src.monitoring.alerts import PipelineMonitor
from src.transform.aggregations import (
    compute_daily_aggregations,
    compute_monthly_aggregations,
    compute_weekly_aggregations,
)
from src.transform.anomaly import detect_anomalies
from src.transform.validators import validate_batch
from src.utils.logger import setup_logging


# ══════════════════════════════════════════════════════════════════════════════
# TASKS
# ══════════════════════════════════════════════════════════════════════════════


@task(name="extract_weather", retries=2, retry_delay_seconds=30)
def extract_weather() -> list[dict]:
    """Extract current weather data for all 500 cities."""
    log = get_run_logger()
    log.info("Starting weather data extraction")

    client = WeatherAPIClient()
    raw_data = client.fetch_all_cities()

    log.info(f"Extracted {len(raw_data)} records")
    return raw_data


@task(name="validate_data")
def validate_data(raw_records: list[dict]) -> tuple[list[dict], list[dict]]:
    """Validate raw records using Pydantic models."""
    log = get_run_logger()
    log.info(f"Validating {len(raw_records)} records")

    valid, invalid = validate_batch(raw_records)

    log.info(f"Validation complete: {len(valid)} valid, {len(invalid)} invalid")
    return valid, invalid


@task(name="load_raw_data")
def load_raw_data(raw_records: list[dict], pipeline_run_id: str) -> int:
    """Load raw API responses into the raw layer."""
    log = get_run_logger()

    with get_session() as session:
        tracker = LineageTracker(session, pipeline_run_id)
        with tracker.track("api", "raw_weather_data", "insert") as t:
            count = insert_raw_data(session, raw_records, pipeline_run_id)
            t.record_count = count

    log.info(f"Loaded {count} raw records")
    return count


@task(name="load_staging_data")
def load_staging_data(valid_records: list[dict], pipeline_run_id: str) -> int:
    """Load validated records into the staging layer."""
    log = get_run_logger()

    with get_session() as session:
        tracker = LineageTracker(session, pipeline_run_id)
        with tracker.track("raw_weather_data", "stg_weather_readings", "upsert") as t:
            count = upsert_staging(session, valid_records, pipeline_run_id)
            t.record_count = count

    log.info(f"Loaded {count} staging records")
    return count


@task(name="load_fact_data")
def load_fact_data(valid_records: list[dict], pipeline_run_id: str) -> int:
    """Load validated records into the fact table."""
    log = get_run_logger()

    with get_session() as session:
        tracker = LineageTracker(session, pipeline_run_id)
        with tracker.track("stg_weather_readings", "fact_weather", "upsert") as t:
            count = load_facts(session, valid_records, pipeline_run_id)
            t.record_count = count

    log.info(f"Loaded {count} fact records")
    return count


@task(name="run_aggregations")
def run_aggregations(pipeline_run_id: str) -> dict:
    """Run daily, weekly, and monthly aggregations."""
    log = get_run_logger()
    results = {}

    with get_session() as session:
        tracker = LineageTracker(session, pipeline_run_id)

        with tracker.track("fact_weather", "agg_daily_weather", "aggregate") as t:
            results["daily"] = compute_daily_aggregations(session)
            t.record_count = results["daily"]

        with tracker.track("fact_weather", "agg_weekly_weather", "aggregate") as t:
            results["weekly"] = compute_weekly_aggregations(session)
            t.record_count = results["weekly"]

        with tracker.track("fact_weather", "agg_monthly_weather", "aggregate") as t:
            results["monthly"] = compute_monthly_aggregations(session)
            t.record_count = results["monthly"]

    log.info(
        f"Aggregations complete: daily={results['daily']}, "
        f"weekly={results['weekly']}, monthly={results['monthly']}"
    )
    return results


@task(name="run_anomaly_detection")
def run_anomaly_detection(pipeline_run_id: str) -> int:
    """Detect weather anomalies using Z-score analysis."""
    log = get_run_logger()

    with get_session() as session:
        tracker = LineageTracker(session, pipeline_run_id)
        with tracker.track("fact_weather", "weather_anomalies", "transform") as t:
            count = detect_anomalies(session, pipeline_run_id)
            t.record_count = count

    log.info(f"Anomaly detection found {count} anomalies")
    return count


# ══════════════════════════════════════════════════════════════════════════════
# MAIN FLOW
# ══════════════════════════════════════════════════════════════════════════════


@flow(
    name="weather_etl_pipeline",
    description="ETL pipeline for weather data from Open-Meteo API",
    retries=1,
    retry_delay_seconds=60,
)
def weather_etl_flow() -> dict:
    """Main ETL flow that orchestrates the full weather pipeline.

    Pipeline steps:
    1. Extract weather data from Open-Meteo API
    2. Validate raw data with Pydantic models
    3. Load raw data into raw layer
    4. Load validated data into staging and fact layers
    5. Run aggregations (daily/weekly/monthly)
    6. Detect anomalies
    7. Track pipeline run metadata

    Returns:
        Dict with pipeline run summary.
    """
    pipeline_run_id = str(uuid.uuid4())
    log = get_run_logger()
    log.info(f"🚀 Starting weather ETL pipeline — run_id={pipeline_run_id}")

    # Initialize monitoring
    with get_session() as session:
        monitor = PipelineMonitor(session)
        monitor.start_run(pipeline_run_id)

    try:
        # Step 1: Extract
        raw_data = extract_weather()
        records_extracted = len(raw_data)

        # Step 2: Validate
        valid_records, invalid_records = validate_data(raw_data)
        records_validated = len(valid_records)
        records_failed = len(invalid_records)

        # Step 3: Load raw
        load_raw_data(raw_data, pipeline_run_id)

        # Step 4: Load staging + facts
        load_staging_data(valid_records, pipeline_run_id)
        records_loaded = load_fact_data(valid_records, pipeline_run_id)

        # Step 5: Aggregations
        agg_results = run_aggregations(pipeline_run_id)

        # Step 6: Anomaly detection
        anomaly_count = run_anomaly_detection(pipeline_run_id)

        # Step 7: Record success
        with get_session() as session:
            monitor = PipelineMonitor(session)
            monitor.complete_run(
                pipeline_run_id,
                records_extracted=records_extracted,
                records_validated=records_validated,
                records_loaded=records_loaded,
                records_failed=records_failed,
            )

        summary = {
            "run_id": pipeline_run_id,
            "status": "success",
            "records_extracted": records_extracted,
            "records_validated": records_validated,
            "records_loaded": records_loaded,
            "records_failed": records_failed,
            "aggregations": agg_results,
            "anomalies_detected": anomaly_count,
        }

        log.info(f"✅ Pipeline complete: {summary}")
        return summary

    except Exception as e:
        # Record failure
        with get_session() as session:
            monitor = PipelineMonitor(session)
            monitor.fail_run(pipeline_run_id, str(e))

        log.error(f"❌ Pipeline failed: {e}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# INITIALIZATION & ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════


def initialize_pipeline() -> None:
    """One-time initialization: create tables and load city dimensions."""
    setup_logging()
    create_all_tables()

    with get_session() as session:
        load_city_dimensions(session)


if __name__ == "__main__":
    # Initialize
    initialize_pipeline()

    # Run the flow on a 5-minute schedule using Prefect's serve()
    settings = get_settings()
    weather_etl_flow.serve(
        name="weather-etl-deployment",
        interval=settings.FETCH_INTERVAL_MINUTES * 60,  # seconds
    )
