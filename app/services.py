"""Business logic for fetching and computing statistics using CSV snapshots."""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from fastapi.concurrency import run_in_threadpool

from .db import engine
from .iracing_client import IRacingClient
from .models import Base
from .snapshots import (
    find_closest_snapshot,
    get_oldest_snapshot_date,
    load_snapshot_map,
    load_snapshot_rows,
    snapshot_path,
    store_snapshot,
)
from .settings import settings

logger = logging.getLogger(__name__)


_top_growers_cache: dict[
    tuple[str, date, int, int | None],
    dict[str, object],
] = {}
_top_growers_cache_lock = asyncio.Lock()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _next_cache_expiry(now: datetime | None = None) -> datetime:
    now = now or _utcnow()
    expiry = now.replace(hour=23, minute=55, second=0, microsecond=0)
    if now >= expiry:
        expiry += timedelta(days=1)
    return expiry


def init_db() -> None:
    """Initialize database tables for ORM models."""

    logger.info("Creating database tables if they do not exist")
    Base.metadata.create_all(engine)


async def _download_snapshot(category: str, snapshot_date: date, client: IRacingClient) -> Path:
    content = await client.download_category_csv(category)
    return store_snapshot(category, snapshot_date, content)


async def _ensure_snapshot(
    category: str,
    target_date: date,
    client: IRacingClient,
    *,
    fetch_if_missing: bool,
) -> Tuple[Path | None, date | None]:
    """Return snapshot path for target_date or the closest available."""

    path = snapshot_path(category, target_date)
    if path.exists():
        return path, target_date

    if fetch_if_missing:
        try:
            downloaded = await _download_snapshot(category, target_date, client)
            return downloaded, target_date
        except Exception:
            logger.exception("Failed to fetch snapshot for %s on %s", category, target_date)

    return find_closest_snapshot(category, target_date)


async def fetch_and_store(category: str | None = None) -> Dict[str, int]:
    """Fetch stats for configured categories and store snapshots as CSV files."""

    target_categories = [category] if category else settings.categories_normalized
    tz = ZoneInfo(settings.app_timezone)
    snapshot_day = datetime.now(tz).date()
    counts: Dict[str, int] = {}

    client = IRacingClient()

    async def process_category(cat: str) -> None:
        logger.info("Starting fetch for category %s", cat)
        path = await _download_snapshot(cat, snapshot_day, client)
        counts[cat] = sum(1 for _ in load_snapshot_rows(path))
        logger.info(
            "Completed fetch for category %s with %s rows stored at %s",
            cat,
            counts[cat],
            path,
        )

    try:
        await asyncio.gather(*(process_category(cat) for cat in target_categories))
    finally:
        await client.close()
    return counts


async def _get_member_row(
    cust_id: int, category: str, target_date: date | None = None
) -> tuple[Dict[str, object] | None, date | None]:
    client = IRacingClient()
    try:
        target_date = target_date or date.today()
        path, resolved_date = await _ensure_snapshot(
            category, target_date, client, fetch_if_missing=True
        )
        if not path:
            return None, None
        for row in load_snapshot_rows(path):
            if row.get("cust_id") == cust_id:
                return row, resolved_date
        return None, resolved_date
    finally:
        await client.close()


async def get_latest_snapshot(cust_id: int, category: str):
    row, snapshot_date = await _get_member_row(cust_id, category)
    if not row or not snapshot_date:
        return None
    return {
        "cust_id": cust_id,
        "category": category,
        "snapshot_date": snapshot_date,
        "fetched_at": datetime.now(timezone.utc),
        "driver": row.get("display_name"),
        "location": row.get("location"),
        "irating": row.get("irating"),
        "starts": row.get("starts"),
        "wins": row.get("wins"),
    }


async def get_history(
    cust_id: int,
    category: str,
    start: Optional[date],
    end: Optional[date],
) -> List[Dict[str, object]]:
    start = start or date.min
    end = end or date.max
    client = IRacingClient()
    results: List[Dict[str, object]] = []
    try:
        path, resolved_end = await _ensure_snapshot(
            category, end, client, fetch_if_missing=(end == date.today())
        )
        if not path:
            return []
        directory = path.parent
        for file_path in sorted(directory.glob("*.csv")):
            snapshot_date = file_path.stem
            try:
                file_date = datetime.strptime(snapshot_date, "%Y-%m-%d").date()
            except ValueError:
                continue
            if not (start <= file_date <= end):
                continue
            for row in load_snapshot_rows(file_path):
                if row.get("cust_id") == cust_id:
                    results.append(
                        {
                            "snapshot_date": file_date,
                            "fetched_at": datetime.now(timezone.utc),
                            "driver": row.get("display_name"),
                            "location": row.get("location"),
                            "irating": row.get("irating"),
                            "starts": row.get("starts"),
                            "wins": row.get("wins"),
                        }
                    )
        return sorted(results, key=lambda item: item["snapshot_date"])
    finally:
        await client.close()


async def get_irating_delta(
    cust_id: int,
    category: str,
    days: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
):
    if days is not None:
        end_date = end_date or date.today()
        start_date = end_date - timedelta(days=days)
    if not end_date:
        end_date = date.today()
    start_date = start_date or (end_date - timedelta(days=1))

    client = IRacingClient()
    try:
        end_path, end_used = await _ensure_snapshot(
            category, end_date, client, fetch_if_missing=True
        )
        if not end_path or not end_used:
            return None
        start_path, start_used = await _ensure_snapshot(
            category, start_date, client, fetch_if_missing=False
        )
        if not start_path or not start_used:
            return None

        start_map = load_snapshot_map(start_path)
        end_map = load_snapshot_map(end_path)
        start_row = start_map.get(cust_id)
        end_row = end_map.get(cust_id)
        if not start_row or not end_row:
            return None

        start_value = start_row.get("irating")
        end_value = end_row.get("irating")
        if not isinstance(end_value, int) or end_value == -1:
            return None
        if not isinstance(start_value, int):
            return None
        start_value = 1500 if start_value == -1 else start_value
        delta = end_value - start_value
        percent_change = (delta / start_value * 100) if start_value else None

        return {
            "cust_id": cust_id,
            "category": category,
            "start_date_used": start_used,
            "end_date_used": end_used,
            "start_value": start_value,
            "end_value": end_value,
            "delta": delta,
            "percent_change": percent_change,
        }
    finally:
        await client.close()


async def get_top_growers(
    category: str, days: int, limit: int, min_current_irating: int | None = None
) -> Dict[str, object]:
    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    oldest_snapshot = get_oldest_snapshot_date(category)
    if oldest_snapshot and start_date < oldest_snapshot:
        start_date = oldest_snapshot

    logger.info(
        "Fetching top growers: category=%s days=%s limit=%s min_current_irating=%s",
        category,
        days,
        limit,
        min_current_irating,
    )

    cache_key = (category, start_date, limit, min_current_irating)
    now = _utcnow()
    async with _top_growers_cache_lock:
        cached = _top_growers_cache.get(cache_key)
        if cached:
            expires_at = cached.get("expires_at")
            if isinstance(expires_at, datetime) and expires_at > now:
                return cached["payload"]

    client = IRacingClient()
    try:
        end_path, end_used = await _ensure_snapshot(
            category, end_date, client, fetch_if_missing=True
        )
        if not end_path or not end_used:
            logger.warning("No snapshots available for %s", category)
            return {"results": [], "snapshot_age_days": None}

        start_path, start_used = await _ensure_snapshot(
            category, start_date, client, fetch_if_missing=False
        )
        if not start_path or not start_used:
            logger.warning("No starting snapshot found for %s", category)
            return {"results": [], "snapshot_age_days": None}

        normalized_start = start_used
        cache_key = (category, normalized_start, limit, min_current_irating)
        now = _utcnow()
        async with _top_growers_cache_lock:
            cached = _top_growers_cache.get(cache_key)
            if cached:
                expires_at = cached.get("expires_at")
                if isinstance(expires_at, datetime) and expires_at > now:
                    return cached["payload"]

        def _compute() -> List[Dict[str, object]]:
            start_map = load_snapshot_map(start_path)
            end_map = load_snapshot_map(end_path)
            results: List[Dict[str, object]] = []
            for cust_id, end_row in end_map.items():
                end_ir = end_row.get("irating")
                if not isinstance(end_ir, int) or end_ir == -1:
                    continue
                if min_current_irating is not None and end_ir < min_current_irating:
                    continue
                start_row = start_map.get(cust_id)
                if not start_row:
                    continue
                start_ir = start_row.get("irating")
                if not isinstance(start_ir, int):
                    continue
                normalized_start = 1500 if start_ir == -1 else start_ir
                delta = end_ir - normalized_start
                percent_change = (
                    delta * 100.0 / normalized_start if normalized_start else None
                )
                results.append(
                    {
                        "cust_id": cust_id,
                        "category": category,
                        "end_value": end_ir,
                        "delta": delta,
                        "percent_change": percent_change,
                        "driver": end_row.get("display_name"),
                        "location": end_row.get("location"),
                        "starts": end_row.get("starts"),
                        "wins": end_row.get("wins"),
                    }
                )
            results.sort(key=lambda item: item["delta"], reverse=True)
            return results[:limit]

        computed = await run_in_threadpool(_compute)
        logger.info(
            "Prepared %s top grower results for category=%s (requested limit=%s)",
            len(computed),
            category,
            limit,
        )
        snapshot_age_days = None
        if start_used and end_used:
            snapshot_age_days = (end_used - start_used).days
        payload = {
            "results": computed,
            "snapshot_age_days": snapshot_age_days,
            "start_date_used": start_used,
            "end_date_used": end_used,
        }

        async with _top_growers_cache_lock:
            _top_growers_cache[cache_key] = {
                "payload": payload,
                "expires_at": _next_cache_expiry(_utcnow()),
            }

        return payload
    finally:
        await client.close()
