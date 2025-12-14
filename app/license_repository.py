"""Repository helpers for license management."""
from __future__ import annotations

import secrets
from datetime import datetime, timezone

from sqlalchemy import select, true
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .models import License


def generate_license_key(length: int, alphabet: str) -> str:
    """Generate a secure random license key from the provided alphabet."""

    return "".join(secrets.choice(alphabet) for _ in range(length))


def ensure_license(
    session: Session, *, key: str, label: str | None = None
) -> License:
    """Insert or reactivate a license idempotently."""

    stmt = sqlite_insert(License).values(key=key, label=label, active=True, revoked_at=None)
    stmt = stmt.on_conflict_do_update(
        index_elements=[License.key],
        set_={"label": stmt.excluded.label, "active": true(), "revoked_at": None},
    )
    session.execute(stmt)
    return session.get(License, key)  # type: ignore[return-value]


def create_unique_license(
    session: Session, *, length: int, alphabet: str, label: str | None = None
) -> License:
    """Create a license with a unique key, retrying on collision."""

    while True:
        key = generate_license_key(length, alphabet)
        existing = session.get(License, key)
        if existing:
            continue
        license_record = ensure_license(session, key=key, label=label)
        return license_record


def list_licenses(session: Session, *, include_inactive: bool = False) -> list[License]:
    """Return licenses filtered by active flag."""

    stmt = select(License)
    if not include_inactive:
        stmt = stmt.where(License.active == true())
    return list(session.scalars(stmt).all())


def revoke_license(session: Session, *, key: str) -> License | None:
    """Mark a license as revoked."""

    record = session.get(License, key)
    if not record:
        return None
    if record.active:
        record.active = False
        record.revoked_at = datetime.now(timezone.utc)
        session.add(record)
    return record


def activate_license(session: Session, *, key: str) -> License | None:
    """Reactivate a revoked license."""

    record = session.get(License, key)
    if not record:
        return None
    if not record.active:
        record.active = True
        record.revoked_at = None
        session.add(record)
    return record


def license_to_dict(record: License) -> dict:
    """Serialize a License ORM object to a JSON-friendly dict."""

    return {
        "key": record.key,
        "label": record.label,
        "active": record.active,
        "created_at": record.created_at,
        "revoked_at": record.revoked_at,
    }


__all__ = [
    "activate_license",
    "create_unique_license",
    "ensure_license",
    "generate_license_key",
    "license_to_dict",
    "list_licenses",
    "revoke_license",
]
