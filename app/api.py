"""FastAPI routers and endpoints."""
from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from .services import (
    fetch_and_store,
    get_history,
    get_irating_delta,
    get_latest_snapshot,
    get_top_growers,
)
from .settings import settings

router = APIRouter()


@router.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@router.post("/admin/run-fetch")
async def run_fetch_now():
    counts = await fetch_and_store()
    return {"counts": counts}


@router.get("/members/{cust_id}/latest")
async def latest_member_snapshot(cust_id: int, category: str = Query("sports_car")):
    if category not in settings.categories_normalized:
        raise HTTPException(status_code=400, detail="Unsupported category")
    snapshot = await get_latest_snapshot(cust_id, category)
    if not snapshot:
        raise HTTPException(status_code=404, detail="No snapshot found")
    return snapshot


@router.get("/members/{cust_id}/history")
async def member_history(
    cust_id: int,
    category: str = Query("sports_car"),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
):
    if category not in settings.categories_normalized:
        raise HTTPException(status_code=400, detail="Unsupported category")
    snapshots = await get_history(cust_id, category, start, end)
    return snapshots


@router.get("/members/{cust_id}/delta")
async def member_delta(
    cust_id: int,
    category: str = Query("sports_car"),
    days: Optional[int] = Query(None, ge=1),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
):
    if category not in settings.categories_normalized:
        raise HTTPException(status_code=400, detail="Unsupported category")
    result = await get_irating_delta(
        cust_id, category, days=days, start_date=start, end_date=end
    )
    if not result:
        raise HTTPException(status_code=404, detail="Insufficient data")
    return result


@router.get("/leaders/growers")
async def leaders_growers(
    category: str = Query("sports_car"),
    days: int = Query(30, ge=1),
    limit: int = Query(20, ge=1, le=100),
    min_current_irating: int | None = Query(None, ge=0),
):
    if category not in settings.categories_normalized:
        raise HTTPException(status_code=400, detail="Unsupported category")
    results = await get_top_growers(category, days, limit, min_current_irating)
    return {
        "category": category,
        "days": days,
        "min_current_irating": min_current_irating,
        "results": results,
    }
