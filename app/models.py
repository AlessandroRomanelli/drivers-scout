"""SQLAlchemy models for member statistics."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for ORM models."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class Member(Base):
    """Member tracked by the service."""

    __tablename__ = "members"

    cust_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    display_name: Mapped[str | None] = mapped_column(String(255))
    location: Mapped[str | None] = mapped_column(String(255))


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
