"""
Pipeline monitoring and alerting.

Tracks pipeline run metadata and provides health checks
with log-based alerting on failures.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.config.settings import get_settings
from src.models.database import PipelineRun
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PipelineMonitor:
    """Monitors pipeline health and records run metadata."""

    def __init__(self, session: Session) -> None:
        self.session = session
        self.settings = get_settings()

    def start_run(self, run_id: str) -> PipelineRun:
        """Record the start of a pipeline run.

        Args:
            run_id: UUID for this run.

        Returns:
            PipelineRun record.
        """
        run = PipelineRun(
            run_id=run_id,
            status="running",
            started_at=datetime.utcnow(),
        )
        self.session.add(run)
        self.session.flush()
        logger.info(f"Pipeline run started: {run_id}", run_id=run_id)
        return run

    def complete_run(
        self,
        run_id: str,
        records_extracted: int = 0,
        records_validated: int = 0,
        records_loaded: int = 0,
        records_failed: int = 0,
    ) -> None:
        """Mark a pipeline run as successfully completed.

        Args:
            run_id: UUID of the run to complete.
            records_extracted: Total records extracted from API.
            records_validated: Records that passed validation.
            records_loaded: Records loaded into fact table.
            records_failed: Records that failed validation or loading.
        """
        run = (
            self.session.query(PipelineRun)
            .filter(PipelineRun.run_id == run_id)
            .first()
        )
        if not run:
            logger.error(f"Pipeline run not found: {run_id}", run_id=run_id)
            return

        now = datetime.utcnow()
        run.status = "success"
        run.completed_at = now
        run.duration_seconds = (now - run.started_at).total_seconds()
        run.records_extracted = records_extracted
        run.records_validated = records_validated
        run.records_loaded = records_loaded
        run.records_failed = records_failed
        self.session.flush()

        logger.info(
            f"Pipeline run completed: {run_id} "
            f"(extracted={records_extracted}, loaded={records_loaded}, "
            f"failed={records_failed}, duration={run.duration_seconds:.1f}s)",
            run_id=run_id,
            duration=run.duration_seconds,
        )

    def fail_run(self, run_id: str, error: str) -> None:
        """Mark a pipeline run as failed.

        Args:
            run_id: UUID of the run.
            error: Error message.
        """
        run = (
            self.session.query(PipelineRun)
            .filter(PipelineRun.run_id == run_id)
            .first()
        )
        if not run:
            logger.error(f"Pipeline run not found: {run_id}", run_id=run_id)
            return

        now = datetime.utcnow()
        run.status = "failed"
        run.completed_at = now
        run.duration_seconds = (now - run.started_at).total_seconds()
        run.error_message = error
        self.session.flush()

        # CRITICAL alert for pipeline failures
        if self.settings.ALERT_ON_FAILURE:
            logger.critical(
                f"🚨 PIPELINE FAILURE: Run {run_id} failed — {error}",
                run_id=run_id,
                error=error,
                alert_type="pipeline_failure",
            )

    def check_health(self) -> dict:
        """Check pipeline health based on recent run history.

        Returns:
            Dict with health status, last run info, and failure rate.
        """
        # Get last successful run
        last_success = (
            self.session.query(PipelineRun)
            .filter(PipelineRun.status == "success")
            .order_by(PipelineRun.started_at.desc())
            .first()
        )

        # Get failure rate over the last 24 hours
        cutoff = datetime.utcnow() - timedelta(hours=24)
        total_runs = (
            self.session.query(func.count(PipelineRun.id))
            .filter(PipelineRun.started_at >= cutoff)
            .scalar()
        )
        failed_runs = (
            self.session.query(func.count(PipelineRun.id))
            .filter(
                PipelineRun.started_at >= cutoff,
                PipelineRun.status == "failed",
            )
            .scalar()
        )

        failure_rate = (failed_runs / total_runs * 100) if total_runs > 0 else 0

        # Determine health status
        is_stale = False
        if last_success:
            minutes_since = (
                datetime.utcnow() - last_success.completed_at
            ).total_seconds() / 60
            is_stale = minutes_since > self.settings.HEALTH_CHECK_INTERVAL_MINUTES

        is_healthy = not is_stale and failure_rate < self.settings.MAX_FAILURE_RATE_PERCENT

        health = {
            "status": "healthy" if is_healthy else "unhealthy",
            "last_successful_run": last_success.run_id if last_success else None,
            "last_run_time": (
                last_success.completed_at.isoformat() if last_success else None
            ),
            "total_runs_24h": total_runs,
            "failed_runs_24h": failed_runs,
            "failure_rate_percent": round(failure_rate, 1),
            "is_stale": is_stale,
            "checked_at": datetime.utcnow().isoformat(),
        }

        if not is_healthy:
            logger.warning(
                f"⚠️ Pipeline health check: UNHEALTHY "
                f"(failure_rate={failure_rate:.1f}%, stale={is_stale})",
                health=health,
                alert_type="health_check",
            )
        else:
            logger.info("Pipeline health check: HEALTHY", health=health)

        return health

    def get_recent_runs(self, limit: int = 10) -> list[dict]:
        """Get recent pipeline run summaries.

        Args:
            limit: Max number of runs to return.

        Returns:
            List of run summary dicts.
        """
        runs = (
            self.session.query(PipelineRun)
            .order_by(PipelineRun.started_at.desc())
            .limit(limit)
            .all()
        )

        return [
            {
                "run_id": r.run_id,
                "status": r.status,
                "started_at": r.started_at.isoformat(),
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                "duration_seconds": r.duration_seconds,
                "records_extracted": r.records_extracted,
                "records_loaded": r.records_loaded,
                "records_failed": r.records_failed,
                "error": r.error_message,
            }
            for r in runs
        ]
