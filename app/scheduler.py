"""APScheduler integration for daily fetches."""
from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .services import fetch_and_store
from .settings import settings

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone=ZoneInfo(settings.app_timezone))


async def scheduled_job() -> None:
    logger.info("Running scheduled fetch job")
    await fetch_and_store()


def start_scheduler() -> None:
    """Configure and start the scheduler if enabled."""
    if not settings.scheduler_enabled:
        logger.info("Scheduler disabled via configuration")
        return
    trigger = CronTrigger(
        hour=settings.schedule_hour,
        minute=settings.schedule_minute,
        timezone=ZoneInfo(settings.app_timezone),
    )
    scheduler.add_job(scheduled_job, trigger=trigger, name="daily_fetch")
    scheduler.start()
    logger.info(
        "Scheduler started for %s:%s in %s",
        settings.schedule_hour,
        settings.schedule_minute,
        settings.app_timezone,
    )


def shutdown_scheduler() -> None:
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler shut down")
