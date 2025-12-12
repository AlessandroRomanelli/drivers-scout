"""Repository functions for database interactions."""
from __future__ import annotations

from datetime import date
from typing import Iterable, Sequence, Tuple

from sqlalchemy import and_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .models import Member, MemberStatsSnapshot


def ensure_members(session: Session, members: Iterable[int | Tuple[int, str | None]]) -> None:
    """Ensure Member records exist for provided cust_ids with optional names."""

    member_records: list[Tuple[int, str | None]] = []
    for item in members:
        if isinstance(item, tuple):
            cust_id, display_name = item
        else:
            cust_id, display_name = int(item), None
        member_records.append((cust_id, display_name))

    if not member_records:
        return

    cust_ids = [cust_id for cust_id, _ in member_records]
    existing_ids = {
        row[0]
        for row in session.execute(select(Member.cust_id).where(Member.cust_id.in_(cust_ids)))
    }
    for cust_id, display_name in member_records:
        if cust_id not in existing_ids:
            session.add(Member(cust_id=cust_id, display_name=display_name))
        elif display_name:
            member = session.get(Member, cust_id)
            if member and member.display_name != display_name:
                member.display_name = display_name


def upsert_snapshot(
    session: Session,
    *,
    cust_id: int,
    category: str,
    snapshot_date: date,
    fetched_at,
    stats_json: dict | None,
    irating: int | None,
    license_class: str | None,
    license_sr: float | None,
    ttrating: int | None,
    starts: int | None,
    wins: int | None,
    avg_inc: float | None,
) -> None:
    """Insert or update a snapshot idempotently."""
    stmt = sqlite_insert(MemberStatsSnapshot).values(
        cust_id=cust_id,
        category=category,
        snapshot_date=snapshot_date,
        fetched_at=fetched_at,
        stats_json=stats_json,
        irating=irating,
        license_class=license_class,
        license_sr=license_sr,
        ttrating=ttrating,
        starts=starts,
        wins=wins,
        avg_inc=avg_inc,
    )
    update_fields = {
        "fetched_at": fetched_at,
        "stats_json": stats_json,
        "irating": irating,
        "license_class": license_class,
        "license_sr": license_sr,
        "ttrating": ttrating,
        "starts": starts,
        "wins": wins,
        "avg_inc": avg_inc,
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=[MemberStatsSnapshot.cust_id, MemberStatsSnapshot.category, MemberStatsSnapshot.snapshot_date],
        set_=update_fields,
    )
    session.execute(stmt)


def fetch_latest_snapshot(
    session: Session, cust_id: int, category: str
) -> MemberStatsSnapshot | None:
    """Return the most recent snapshot for a member/category."""
    return session.scalars(
        select(MemberStatsSnapshot)
        .where(
            and_(
                MemberStatsSnapshot.cust_id == cust_id,
                MemberStatsSnapshot.category == category,
            )
        )
        .order_by(MemberStatsSnapshot.snapshot_date.desc())
        .limit(1)
    ).first()


def fetch_snapshots_range(
    session: Session,
    cust_id: int,
    category: str,
    start_date: date | None,
    end_date: date | None,
) -> Sequence[MemberStatsSnapshot]:
    """Fetch snapshots within date range inclusive."""
    stmt = select(MemberStatsSnapshot).where(
        and_(
            MemberStatsSnapshot.cust_id == cust_id,
            MemberStatsSnapshot.category == category,
        )
    )
    if start_date:
        stmt = stmt.where(MemberStatsSnapshot.snapshot_date >= start_date)
    if end_date:
        stmt = stmt.where(MemberStatsSnapshot.snapshot_date <= end_date)
    stmt = stmt.order_by(MemberStatsSnapshot.snapshot_date.asc())
    return list(session.scalars(stmt).all())


def fetch_snapshot_on_or_before(
    session: Session, cust_id: int, category: str, target_date: date
) -> MemberStatsSnapshot | None:
    """Fetch latest snapshot on or before target_date."""
    stmt = (
        select(MemberStatsSnapshot)
        .where(
            and_(
                MemberStatsSnapshot.cust_id == cust_id,
                MemberStatsSnapshot.category == category,
                MemberStatsSnapshot.snapshot_date <= target_date,
            )
        )
        .order_by(MemberStatsSnapshot.snapshot_date.desc())
        .limit(1)
    )
    return session.scalars(stmt).first()


def fetch_all_cust_ids(session: Session) -> list[int]:
    """Return all tracked cust_ids."""
    return list(session.scalars(select(Member.cust_id)).all())
