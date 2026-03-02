"""
Data lineage tracker.

Records the flow of data between pipeline stages for auditability.
Tracks source → target table movements, record counts, and durations.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import datetime
from typing import Generator

from sqlalchemy.orm import Session

from src.models.database import DataLineage
from src.utils.logger import get_logger

logger = get_logger(__name__)


class LineageTracker:
    """Tracks data lineage through the pipeline stages."""

    def __init__(self, session: Session, pipeline_run_id: str) -> None:
        self.session = session
        self.pipeline_run_id = pipeline_run_id

    @contextmanager
    def track(
        self,
        source_table: str,
        target_table: str,
        operation: str = "insert",
    ) -> Generator[LineageRecord, None, None]:
        """Context manager to track a data movement operation.

        Usage:
            with tracker.track("raw_weather_data", "stg_weather_readings", "upsert") as t:
                count = do_something()
                t.record_count = count

        Args:
            source_table: Name of the source table.
            target_table: Name of the target table.
            operation: Type of operation (insert, upsert, aggregate, transform).
        """
        record = LineageRecord()
        start_time = time.monotonic()
        started_at = datetime.utcnow()

        try:
            yield record
            duration = time.monotonic() - start_time

            lineage = DataLineage(
                pipeline_run_id=self.pipeline_run_id,
                source_table=source_table,
                target_table=target_table,
                operation=operation,
                record_count=record.record_count,
                status="success",
                started_at=started_at,
                completed_at=datetime.utcnow(),
                duration_seconds=round(duration, 3),
            )
            self.session.add(lineage)
            self.session.flush()

            logger.info(
                f"Lineage tracked: {source_table} → {target_table} "
                f"({record.record_count} records, {duration:.2f}s)",
                source=source_table,
                target=target_table,
                operation=operation,
                record_count=record.record_count,
                duration_seconds=round(duration, 3),
            )

        except Exception as e:
            duration = time.monotonic() - start_time

            lineage = DataLineage(
                pipeline_run_id=self.pipeline_run_id,
                source_table=source_table,
                target_table=target_table,
                operation=operation,
                record_count=record.record_count,
                status="failed",
                error_message=str(e),
                started_at=started_at,
                completed_at=datetime.utcnow(),
                duration_seconds=round(duration, 3),
            )
            self.session.add(lineage)
            self.session.flush()

            logger.error(
                f"Lineage failure: {source_table} → {target_table}: {e}",
                source=source_table,
                target=target_table,
                error=str(e),
            )
            raise


class LineageRecord:
    """Mutable record for tracking counts inside a lineage context."""

    def __init__(self) -> None:
        self.record_count: int = 0
