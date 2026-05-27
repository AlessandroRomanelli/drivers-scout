"""SQLAlchemy models for member statistics."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for ORM models."""

    # server_default ensures raw INSERTs (e.g. the staging-table path in
    # sync_members_from_snapshots) populate the column without silently
    # dropping the row to the NOT NULL constraint.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        server_default=func.current_timestamp(),
    )


class Member(Base):
    """Member tracked by the service."""

    __tablename__ = "members"

    cust_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    display_name: Mapped[str | None] = mapped_column(String(255))
    location: Mapped[str | None] = mapped_column(String(255))
    display_name_folded: Mapped[str | None] = mapped_column(String(255), index=True)

    __table_args__ = (
        Index("ix_members_display_name", "display_name"),
        Index("ix_members_display_name_lower", text("lower(display_name)")),
    )


class License(Base):
    """License key issued to consumers of the service."""

    __tablename__ = "licenses"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    label: Mapped[str | None] = mapped_column(String(255))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Subscription(Base):
    """Webhook subscription for license-driven notifications."""

    __tablename__ = "subscriptions"
    __table_args__ = (
        UniqueConstraint("license_key", "category", name="uq_subscriptions_license_category"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    license_key: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("licenses.key", ondelete="CASCADE"),
        index=True,
    )
    webhook_url: Mapped[str] = mapped_column(String(500), index=True)
    category: Mapped[str] = mapped_column(String(64))
    min_irating: Mapped[int | None] = mapped_column(Integer)


__all__ = ["Base", "License", "Member", "Subscription"]
