"""Repository functions for database interactions."""
from __future__ import annotations

from datetime import date
from typing import Iterable, Sequence

from sqlalchemy import and_, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .models import Member, MemberStatsSnapshot


def ensure_members(
    session: Session, members: Iterable[int | tuple[int, str | None, str | None]]
) -> None:
    """Ensure Member records exist for provided cust_ids with optional metadata."""

    member_records: dict[int, tuple[str | None, str | None]] = {}
    for item in members:
        if isinstance(item, tuple):
            if len(item) == 3:
                cust_id, display_name, location = item
            else:
                cust_id, display_name = item  # type: ignore[misc]
                location = None
        else:
            cust_id, display_name, location = int(item), None, None

        # Favor the latest non-empty display name for each cust_id
        existing = member_records.get(cust_id)
        if not existing:
            member_records[cust_id] = (display_name, location)
        else:
            current_name, current_location = existing
            member_records[cust_id] = (
                display_name or current_name,
                location or current_location,
            )

    if not member_records:
        return

    def chunks(
        items: list[tuple[int, str | None, str | None]], size: int = 200
    ) -> Iterable[list[tuple[int, str | None, str | None]]]:
        for i in range(0, len(items), size):
            yield items[i : i + size]

    deduped_records = list(member_records.items())

    for chunk in chunks(deduped_records):
        stmt = sqlite_insert(Member).values(
            [
                {
                    "cust_id": cust_id,
                    "display_name": display_name,
                    "location": location,
                }
                for cust_id, (display_name, location) in chunk
            ]
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[Member.cust_id],
            set_={
                "display_name": func.coalesce(stmt.excluded.display_name, Member.display_name),
                "location": func.coalesce(stmt.excluded.location, Member.location),
            },
        )
        session.execute(stmt)


def upsert_snapshot(
    session: Session,
    *,
    cust_id: int,
    category: str,
    snapshot_date: date,
    fetched_at,
    irating: int | None,
    starts: int | None,
    wins: int | None,
) -> None:
    """Insert or update a snapshot idempotently."""
    stmt = sqlite_insert(MemberStatsSnapshot).values(
        cust_id=cust_id,
        category=category,
        snapshot_date=snapshot_date,
        fetched_at=fetched_at,
        irating=irating,
        starts=starts,
        wins=wins,
    )
    update_fields = {
        "fetched_at": fetched_at,
        "irating": irating,
        "starts": starts,
        "wins": wins,
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=[MemberStatsSnapshot.cust_id, MemberStatsSnapshot.category, MemberStatsSnapshot.snapshot_date],
        set_=update_fields,
    )
    session.execute(stmt)


def upsert_snapshots(
    session: Session, snapshots: Iterable[dict[str, object]]
) -> int:
    """Bulk insert or update member snapshots idempotently."""

    chunk = list(snapshots)
    if not chunk:
        return 0

    stmt = sqlite_insert(MemberStatsSnapshot).values(chunk)
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            MemberStatsSnapshot.cust_id,
            MemberStatsSnapshot.category,
            MemberStatsSnapshot.snapshot_date,
        ],
        set_={
            "fetched_at": stmt.excluded.fetched_at,
            "irating": stmt.excluded.irating,
            "starts": stmt.excluded.starts,
            "wins": stmt.excluded.wins,
        },
    )
    session.execute(stmt)
    return len(chunk)


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


def fetch_irating_deltas_for_category(
    session: Session,
    *,
    category: str,
    start_date: date,
    end_date: date,
    limit: int | None = None,
) -> Sequence[dict[str, object]]:
    """Return iRating deltas for members with snapshots on or before target dates."""

    start_snapshots = (
        select(
            MemberStatsSnapshot.cust_id.label("cust_id"),
            MemberStatsSnapshot.snapshot_date.label("snapshot_date"),
            MemberStatsSnapshot.irating.label("irating"),
            func.row_number()
            .over(
                partition_by=MemberStatsSnapshot.cust_id,
                order_by=MemberStatsSnapshot.snapshot_date.desc(),
            )
            .label("row_num"),
        )
        .where(
            and_(
                MemberStatsSnapshot.category == category,
                MemberStatsSnapshot.snapshot_date <= start_date,
            )
        )
        .cte("start_snapshots")
    )

    end_snapshots = (
        select(
            MemberStatsSnapshot.cust_id.label("cust_id"),
            MemberStatsSnapshot.snapshot_date.label("snapshot_date"),
            MemberStatsSnapshot.irating.label("irating"),
            func.row_number()
            .over(
                partition_by=MemberStatsSnapshot.cust_id,
                order_by=MemberStatsSnapshot.snapshot_date.desc(),
            )
            .label("row_num"),
        )
        .where(
            and_(
                MemberStatsSnapshot.category == category,
                MemberStatsSnapshot.snapshot_date <= end_date,
            )
        )
        .cte("end_snapshots")
    )

    start_latest = select(start_snapshots).where(start_snapshots.c.row_num == 1).subquery()
    end_latest = select(end_snapshots).where(end_snapshots.c.row_num == 1).subquery()

    delta_expression = end_latest.c.irating - start_latest.c.irating
    percent_change_expression = delta_expression * 100.0 / func.nullif(start_latest.c.irating, 0)

    stmt = (
        select(
            end_latest.c.cust_id,
            start_latest.c.snapshot_date.label("start_snapshot_date"),
            end_latest.c.snapshot_date.label("end_snapshot_date"),
            start_latest.c.irating.label("start_irating"),
            end_latest.c.irating.label("end_irating"),
            delta_expression.label("delta"),
            percent_change_expression.label("percent_change"),
        )
        .select_from(
            start_latest.join(
                end_latest,
                start_latest.c.cust_id == end_latest.c.cust_id,
            )
        )
        .where(
            and_(
                start_latest.c.irating.is_not(None),
                end_latest.c.irating.is_not(None),
            )
        )
        .order_by(delta_expression.desc())
    )

    if limit:
        stmt = stmt.limit(limit)

    return list(session.execute(stmt).mappings().all())


def fetch_all_cust_ids(session: Session) -> list[int]:
    """Return all tracked cust_ids."""
    return list(session.scalars(select(Member.cust_id)).all())
