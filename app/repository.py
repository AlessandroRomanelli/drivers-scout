"""Repository functions for database interactions."""
from __future__ import annotations

from typing import Iterable

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .models import Member


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


def fetch_all_cust_ids(session: Session) -> list[int]:
    """Return all tracked cust_ids."""
    return list(session.scalars(select(Member.cust_id)).all())
