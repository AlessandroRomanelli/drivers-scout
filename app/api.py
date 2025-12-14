"""FastAPI routers and endpoints."""
from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, status
from sqlalchemy.orm import Session

from .db import get_session
from .license_repository import (
    activate_license,
    create_unique_license,
    license_to_dict,
    list_licenses,
    revoke_license,
)
from .services import (
    fetch_and_store,
    get_history,
    get_irating_delta,
    get_latest_snapshot,
    get_top_growers,
)
from .settings import settings

router = APIRouter()


def _require_admin(admin_secret: str | None = Header(None, alias="X-Admin-Secret")) -> None:
    configured = settings.license_admin_secret
    if configured and admin_secret != configured:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")


def _get_db_session() -> Session:
    with get_session() as session:
        yield session


@router.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@router.post("/admin/run-fetch")
async def run_fetch_now():
    counts = await fetch_and_store()
    return {"counts": counts}


@router.post("/admin/licenses", dependencies=[Depends(_require_admin)])
def issue_license(
    label: str | None = Body(None, embed=True), session: Session = Depends(_get_db_session)
):
    record = create_unique_license(
        session,
        length=settings.license_key_length,
        alphabet=settings.license_key_alphabet,
        label=label,
    )
    return license_to_dict(record)


@router.get("/admin/licenses", dependencies=[Depends(_require_admin)])
def list_license_records(
    include_inactive: bool = Query(False),
    session: Session = Depends(_get_db_session),
):
    records = list_licenses(session, include_inactive=include_inactive)
    return [license_to_dict(record) for record in records]


@router.post("/admin/licenses/{license_key}/revoke", dependencies=[Depends(_require_admin)])
def revoke_license_key(
    license_key: str, session: Session = Depends(_get_db_session)
):
    record = revoke_license(session, key=license_key)
    if not record:
        raise HTTPException(status_code=404, detail="License not found")
    return license_to_dict(record)


@router.post(
    "/admin/licenses/{license_key}/activate", dependencies=[Depends(_require_admin)]
)
def activate_license_key(
    license_key: str, session: Session = Depends(_get_db_session)
):
    record = activate_license(session, key=license_key)
    if not record:
        raise HTTPException(status_code=404, detail="License not found")
    return license_to_dict(record)


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
    data = await get_top_growers(category, days, limit, min_current_irating)
    return {
        "category": category,
        "min_current_irating": min_current_irating,
        "results": data.get("results", []),
        "start_date_used": data.get("start_date_used"),
        "end_date_used": data.get("end_date_used"),
        "snapshot_age_days": data.get("snapshot_age_days"),
    }
