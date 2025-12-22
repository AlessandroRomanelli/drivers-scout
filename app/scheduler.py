"""APScheduler integration for scheduled fetches."""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from zoneinfo import ZoneInfo

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select

from .db import get_session
from .models import License, Subscription
from .services import (
    fetch_and_store,
    get_top_growers,
    sync_members_from_snapshots_async,
)
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


async def deliver_discord_subscriptions() -> None:
    logger.info("Starting scheduled Discord subscription delivery")
    with get_session() as session:
        subscriptions = session.execute(
            select(Subscription, License).join(
                License, Subscription.license_key == License.key
            )
            .filter(License.active.is_(True))
            .filter(License.revoked_at.is_(None))
        ).all()

    if not subscriptions:
        logger.info("No subscriptions found to deliver")
        return

    async with httpx.AsyncClient(timeout=settings.http_timeout_seconds) as client:
        for subscription, license_record in subscriptions:
            try:
                data = await get_top_growers(
                    subscription.category,
                    days=7,
                    limit=10,
                    min_current_irating=subscription.min_irating,
                )
                results = data.get("results", [])
                start_date_used = data.get("start_date_used")
                end_date_used = data.get("end_date_used")
                snapshot_range = _format_snapshot_range(start_date_used, end_date_used)

                embed = {
                    "title": "Top iRating Growers",
                    "description": (
                        "Weekly iRating growth report for Discord subscriptions."
                    ),
                    "fields": [
                        {
                            "name": "Metadata",
                            "value": (
                                f"Category: {subscription.category}\n"
                                f"Snapshot range: {snapshot_range}\n"
                                f"Minimum iRating: "
                                f"{subscription.min_irating if subscription.min_irating is not None else 'None'}"
                            ),
                            "inline": False,
                        }
                    ],
                }

                for index, item in enumerate(results, start=1):
                    driver = item.get("driver") or "Unknown Driver"
                    embed["fields"].append(
                        {
                            "name": f"{index}. {driver}",
                            "value": (
                                f"cust_id: {item.get('cust_id')}\n"
                                f"driver: {driver}\n"
                                f"delta: {item.get('delta')}\n"
                                f"end_value: {item.get('end_value')}\n"
                                f"wins/starts: {item.get('wins')}/{item.get('starts')}"
                            ),
                            "inline": False,
                        }
                    )

                payload = {"embeds": [embed]}
                response = await client.post(subscription.webhook_url, json=payload)
                if response.status_code // 100 != 2:
                    logger.warning(
                        "Discord webhook failed for subscription %s: %s %s",
                        subscription.id,
                        response.status_code,
                        response.text,
                    )
            except Exception:
                logger.exception(
                    "Discord subscription delivery failed for subscription %s",
                    subscription.id,
                )

    logger.info("Discord subscription delivery run complete")


def _format_snapshot_range(start: object, end: object) -> str:
    def _as_date(value: object) -> date | None:
        return value if isinstance(value, date) else None

    start_date = _as_date(start)
    end_date = _as_date(end)
    if start_date and end_date:
        return f"{start_date.isoformat()} to {end_date.isoformat()}"
    if end_date:
        return f"ending {end_date.isoformat()}"
    return "unavailable"


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
    scheduler.add_job(
        deliver_discord_subscriptions,
        trigger=CronTrigger(
            day_of_week="mon",
            hour=23,
            minute=55,
            timezone=SCHEDULE_TIMEZONE,
        ),
        name="deliver_discord_subscriptions",
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
