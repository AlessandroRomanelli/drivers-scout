"""FastAPI routers and endpoints."""
from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, status
from sqlalchemy.orm import Session

from .auth import require_license
from .db import get_session
from .models import License, Member, Subscription
from .license_repository import (
    activate_license,
    create_unique_license,
    license_to_dict,
    list_licenses,
    revoke_license,
)
from .schemas import SubscriptionCreate, SubscriptionResponse
from .services import (
    fetch_and_store,
    get_irating_delta,
    get_latest_snapshot,
    get_latest_snapshots,
    get_top_growers,
    sync_members_from_snapshots_async,
)
from .settings import settings

public_router = APIRouter()
router = APIRouter(dependencies=[Depends(require_license)])


def _require_admin(admin_secret: str | None = Header(None, alias="X-Admin-Secret")) -> None:
    configured = settings.license_admin_secret
    if configured and admin_secret != configured:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")


def _extract_license_token(
    x_license_key: str | None, authorization: str | None
) -> str | None:
    if x_license_key:
        return x_license_key.strip()

    if authorization:
        token = authorization.strip()
        if token.lower().startswith("bearer "):
            return token[7:].strip()
        return token

    return None


def _get_license_token(
    x_license_key: str | None = Header(None, alias="X-License-Key"),
    authorization: str | None = Header(None, alias="Authorization"),
) -> str:
    token = _extract_license_token(x_license_key, authorization)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing license token",
        )
    return token


def _get_db_session() -> Session:
    with get_session() as session:
        yield session


def _parse_cust_ids(cust_ids: str) -> list[int]:
    raw_ids = [value.strip() for value in cust_ids.split(",")]
    filtered = [value for value in raw_ids if value]
    if not filtered:
        raise HTTPException(
            status_code=400,
            detail="cust_ids must be a non-empty comma-separated list of integers",
        )
    parsed: list[int] = []
    for value in filtered:
        try:
            parsed.append(int(value))
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid cust_id '{value}'. Expected comma-separated integers.",
            ) from exc
    return parsed


@public_router.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@public_router.get("/licenses/{license_key}/status")
def license_status(license_key: str, session: Session = Depends(_get_db_session)) -> dict:
    record = session.get(License, license_key)
    if not record:
        return {"key": license_key, "valid": False, "active": False, "label": None, "revoked_at": None}

    return {
        "key": record.key,
        "label": record.label,
        "active": record.active,
        "revoked_at": record.revoked_at,
        "valid": bool(record.active),
    }


@public_router.post("/admin/run-fetch", dependencies=[Depends(_require_admin)])
async def run_fetch_now(category: str | None = Query(None)):
    if category is not None and category not in settings.categories_normalized:
        raise HTTPException(status_code=400, detail="Unsupported category")

    counts = await fetch_and_store(category)
    return {"counts": counts}


@public_router.post("/admin/sync-members", dependencies=[Depends(_require_admin)])
async def sync_members():
    count = await sync_members_from_snapshots_async()
    return {"upserted": count}


@public_router.post("/admin/licenses", dependencies=[Depends(_require_admin)])
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


@public_router.get("/admin/licenses", dependencies=[Depends(_require_admin)])
def list_license_records(
    include_inactive: bool = Query(False),
    session: Session = Depends(_get_db_session),
):
    records = list_licenses(session, include_inactive=include_inactive)
    return [license_to_dict(record) for record in records]


@public_router.post("/admin/licenses/{license_key}/revoke", dependencies=[Depends(_require_admin)])
def revoke_license_key(
    license_key: str, session: Session = Depends(_get_db_session)
):
    record = revoke_license(session, key=license_key)
    if not record:
        raise HTTPException(status_code=404, detail="License not found")
    return license_to_dict(record)


@public_router.post(
    "/admin/licenses/{license_key}/activate", dependencies=[Depends(_require_admin)]
)
def activate_license_key(
    license_key: str, session: Session = Depends(_get_db_session)
):
    record = activate_license(session, key=license_key)
    if not record:
        raise HTTPException(status_code=404, detail="License not found")
    return license_to_dict(record)


def _subscription_to_response(subscription: Subscription) -> SubscriptionResponse:
    return SubscriptionResponse.model_validate(subscription)


@router.get("/members/search")
def search_members(
    q: str = Query(..., min_length=3, description="Partial member display name"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(_get_db_session),
):
    term = q.strip()
    if len(term) < 3:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Query must be at least 3 characters",
        )

    query = (
        session.query(Member)
        .filter(Member.display_name.isnot(None))
        .filter(Member.display_name.ilike(f"%{term}%"))
        .order_by(Member.display_name.asc())
        .offset(offset)
        .limit(limit)
    )

    results = [
        {
            "cust_id": member.cust_id,
            "display_name": member.display_name,
            "location": member.location,
        }
        for member in query.all()
    ]

    return {"query": term, "limit": limit, "offset": offset, "results": results}


@router.get("/members/{cust_id}/latest")
async def latest_member_snapshot(cust_id: int, category: str = Query("sports_car")):
    if category not in settings.categories_normalized:
        raise HTTPException(status_code=400, detail="Unsupported category")
    snapshot = await get_latest_snapshot(cust_id, category)
    if not snapshot:
        raise HTTPException(status_code=404, detail="No snapshot found")
    return snapshot


@router.get("/members/latest")
async def latest_members_snapshot(
    cust_ids: str = Query(..., description="Comma-separated list of member cust_ids"),
    category: str = Query("sports_car"),
):
    if category not in settings.categories_normalized:
        raise HTTPException(status_code=400, detail="Unsupported category")
    parsed_ids = _parse_cust_ids(cust_ids)
    snapshot = await get_latest_snapshots(parsed_ids, category)
    if not snapshot:
        raise HTTPException(status_code=404, detail="No snapshot found")
    return snapshot


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
        "snapshot_age_days": data.get("snapshot_age_days"),
        "start_date_used": data.get("start_date_used"),
        "end_date_used": data.get("end_date_used")
    }


@router.post("/subscriptions", response_model=SubscriptionResponse, status_code=201)
def create_subscription(
    payload: SubscriptionCreate,
    license_key: str = Depends(_get_license_token),
    session: Session = Depends(_get_db_session),
):
    record = Subscription(
        license_key=license_key,
        webhook_url=str(payload.webhook_url),
        category=payload.category,
        min_irating=payload.min_irating,
    )
    session.add(record)
    session.commit()
    session.refresh(record)
    return _subscription_to_response(record)


@router.delete("/subscriptions/{subscription_id}", response_model=SubscriptionResponse)
def delete_subscription(
    subscription_id: int,
    license_key: str = Depends(_get_license_token),
    session: Session = Depends(_get_db_session),
):
    record = (
        session.query(Subscription)
        .filter(Subscription.id == subscription_id)
        .filter(Subscription.license_key == license_key)
        .one_or_none()
    )
    if not record:
        raise HTTPException(status_code=404, detail="Subscription not found")
    response = _subscription_to_response(record)
    session.delete(record)
    session.commit()
    return response
