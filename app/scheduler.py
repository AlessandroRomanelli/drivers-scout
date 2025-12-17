"""APScheduler integration for scheduled fetches."""
from __future__ import annotations

import asyncio
import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .services import fetch_and_store, sync_members_from_snapshots_async
from .settings import settings

logger = logging.getLogger(__name__)

SCHEDULE_HOURS = [23, 5, 11, 17]
SCHEDULE_HOURS_EXPRESSION = ",".join(str(h) for h in SCHEDULE_HOURS)
SCHEDULE_TIMEZONE = ZoneInfo("UTC")

scheduler = AsyncIOScheduler(timezone=SCHEDULE_TIMEZONE)


async def scheduled_job() -> None:
    logger.info(
        "Starting scheduled fetch run: sports_car followed by formula_car with delay"
    )
    await fetch_and_store("sports_car")
    logger.info("sports_car fetch completed; waiting before formula_car")
    await asyncio.sleep(60)
    await fetch_and_store("formula_car")
    logger.info("formula_car fetch completed; waiting before sync_members_from_snapshots_async")
    await asyncio.sleep(60)
    await sync_members_from_snapshots_async()
    logger.info(
        "Scheduled fetch run complete for sports_car and formula_car with member sync"
    )


def start_scheduler() -> None:
    """Configure and start the scheduler if enabled."""
    if not settings.scheduler_enabled:
        logger.info("Scheduler disabled via configuration")
        return
    trigger = CronTrigger(
        hour=SCHEDULE_HOURS_EXPRESSION,
        minute=55,
        timezone=SCHEDULE_TIMEZONE,
    )
    scheduler.add_job(
        scheduled_job,
        trigger=trigger,
        name="sports_formula_fetch_pair",
        misfire_grace_time=None,
    )
    scheduler.start()
    if scheduler.running:
        logger.info(
            "Scheduler started for %s UTC at minute 55",
            SCHEDULE_HOURS_EXPRESSION,
        )
    else:
        logger.warning("Scheduler failed to start")


def shutdown_scheduler() -> None:
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler shut down")
