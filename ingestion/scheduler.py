"""Daily pipeline scheduler — ingestion + dbt transform on a cron schedule.

Usage:
    python -m ingestion.scheduler              # runs at 02:00 UTC daily
    SCHEDULE_HOUR=6 python -m ingestion.scheduler
    SCHEDULE_HOUR=6 SCHEDULE_MINUTE=30 python -m ingestion.scheduler
"""

import logging
import os
import subprocess
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

PROJECT_ROOT = Path(__file__).parent.parent

logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    logger.info("Scheduler: starting pipeline run")
    result = subprocess.run(
        ["bash", "scripts/entrypoint_pipeline.sh"],
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        logger.error("Scheduler: pipeline failed (exit code %d)", result.returncode)
    else:
        logger.info("Scheduler: pipeline completed successfully")


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    hour = int(os.getenv("SCHEDULE_HOUR", "2"))
    minute = int(os.getenv("SCHEDULE_MINUTE", "0"))

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(run_pipeline, CronTrigger(hour=hour, minute=minute))
    logger.info("Scheduler started — pipeline runs daily at %02d:%02d UTC", hour, minute)

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped")
