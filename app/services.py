"""Business logic for fetching and computing statistics."""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

from .db import get_session
from .iracing_client import IRacingClient, normalize_rows
from .models import Base
from .repository import (
    ensure_members,
    fetch_all_cust_ids,
    fetch_latest_snapshot,
    fetch_snapshot_on_or_before,
    fetch_snapshots_range,
    upsert_snapshots,
)
from .settings import settings

logger = logging.getLogger(__name__)


def init_db() -> None:
    """Create database tables."""
    from .db import engine

    Base.metadata.create_all(bind=engine)


async def fetch_and_store(category: str | None = None) -> Dict[str, int]:
    """Fetch stats for configured categories and store snapshots.

    Returns a dict of counts keyed by category.
    """
    target_categories = [category] if category else settings.categories_normalized
    tz = ZoneInfo(settings.app_timezone)
    client = IRacingClient()
    counts: Dict[str, int] = {}

    async def process_category(cat: str) -> None:
        rows = client.fetch_category_csv(cat)
        normalized = normalize_rows(rows)
        snapshot_day = datetime.now(tz).date()
        fetched_at = datetime.now(timezone.utc)
        with get_session() as session:
            stored = 0

            def persist_chunk(chunk: Sequence[Dict[str, object]]) -> int:
                chunk_members = [
                    (
                        item["cust_id"],
                        item.get("display_name"),
                        item.get("location"),
                    )
                    for item in chunk
                    if item.get("cust_id")
                ]
                if chunk_members:
                    ensure_members(session, chunk_members)

                snapshots = [
                    {
                        "cust_id": item.get("cust_id"),
                        "category": cat,
                        "snapshot_date": snapshot_day,
                        "fetched_at": fetched_at,
                        "irating": item.get("irating"),
                        "starts": item.get("starts"),
                        "wins": item.get("wins"),
                    }
                    for item in chunk
                    if item.get("cust_id") is not None
                ]
                return upsert_snapshots(session, snapshots)

            buffer: List[Dict[str, object]] = []
            batch_size = 300
            async for item in normalized:
                buffer.append(item)
                if len(buffer) >= batch_size:
                    stored += persist_chunk(buffer)
                    buffer = []
            if buffer:
                stored += persist_chunk(buffer)

            counts[cat] = stored
        logger.info("Stored %s snapshots for category %s", stored, cat)

    try:
        await asyncio.gather(*(process_category(cat) for cat in target_categories))
    finally:
        await client.close()
    return counts


def _find_member_or_raise(cust_id: int) -> None:
    with get_session() as session:
        known_ids = fetch_all_cust_ids(session)
    if cust_id not in known_ids:
        raise ValueError(f"cust_id {cust_id} not tracked")


def get_latest_snapshot(cust_id: int, category: str):
    _find_member_or_raise(cust_id)
    with get_session() as session:
        return fetch_latest_snapshot(session, cust_id, category)


def get_history(cust_id: int, category: str, start: Optional[date], end: Optional[date]):
    _find_member_or_raise(cust_id)
    with get_session() as session:
        return fetch_snapshots_range(session, cust_id, category, start, end)


def get_irating_delta(
    cust_id: int,
    category: str,
    days: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
):
    """Compute iRating delta between two snapshots."""
    if days is not None:
        end_date = end_date or date.today()
        start_date = end_date - timedelta(days=days)
    if not end_date:
        end_date = date.today()
    _find_member_or_raise(cust_id)
    with get_session() as session:
        end_snapshot = fetch_snapshot_on_or_before(session, cust_id, category, end_date)
        start_snapshot = None
        if start_date:
            start_snapshot = fetch_snapshot_on_or_before(session, cust_id, category, start_date)
        if not end_snapshot or not start_snapshot:
            return None
        start_value = start_snapshot.irating
        end_value = end_snapshot.irating
        if start_value is None or end_value is None:
            return None
        delta = end_value - start_value
        percent_change = (delta / start_value * 100) if start_value else None
        return {
            "cust_id": cust_id,
            "category": category,
            "start_date_used": start_snapshot.snapshot_date,
            "end_date_used": end_snapshot.snapshot_date,
            "start_value": start_value,
            "end_value": end_value,
            "delta": delta,
            "percent_change": percent_change,
        }


def get_top_growers(category: str, days: int, limit: int) -> List[Dict[str, object]]:
    """Return top growers by iRating delta for tracked members."""
    results: List[Dict[str, object]] = []
    with get_session() as session:
        cust_ids = fetch_all_cust_ids(session)
    for cust_id in cust_ids:
        delta = get_irating_delta(cust_id, category, days=days)
        if delta:
            results.append(delta)
    results.sort(key=lambda d: d.get("delta", 0), reverse=True)
    return results[:limit]
