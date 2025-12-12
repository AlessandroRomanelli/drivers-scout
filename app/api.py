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
    try:
        snapshot = get_latest_snapshot(cust_id, category)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if not snapshot:
        raise HTTPException(status_code=404, detail="No snapshot found")
    return {
        "cust_id": cust_id,
        "category": category,
        "snapshot_date": snapshot.snapshot_date,
        "fetched_at": snapshot.fetched_at,
        "irating": snapshot.irating,
        "license_class": snapshot.license_class,
        "license_sr": snapshot.license_sr,
        "stats_json": snapshot.stats_json,
    }


@router.get("/members/{cust_id}/history")
async def member_history(
    cust_id: int,
    category: str = Query("sports_car"),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
):
    if category not in settings.categories_normalized:
        raise HTTPException(status_code=400, detail="Unsupported category")
    try:
        snapshots = get_history(cust_id, category, start, end)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [
        {
            "snapshot_date": s.snapshot_date,
            "fetched_at": s.fetched_at,
            "irating": s.irating,
            "license_class": s.license_class,
            "license_sr": s.license_sr,
            "stats_json": s.stats_json,
        }
        for s in snapshots
    ]


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
    try:
        result = get_irating_delta(cust_id, category, days=days, start_date=start, end_date=end)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if not result:
        raise HTTPException(status_code=404, detail="Insufficient data")
    return result


@router.get("/leaders/growers")
async def leaders_growers(
    category: str = Query("sports_car"),
    days: int = Query(30, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    if category not in settings.categories_normalized:
        raise HTTPException(status_code=400, detail="Unsupported category")
    results = get_top_growers(category, days, limit)
    return {"category": category, "days": days, "results": results}
