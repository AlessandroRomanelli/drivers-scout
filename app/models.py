"""SQLAlchemy models for member statistics."""
from __future__ import annotations

from datetime import datetime, date, timezone

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


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

    snapshots: Mapped[list["MemberStatsSnapshot"]] = relationship(
        back_populates="member", cascade="all, delete-orphan"
    )


class MemberStatsSnapshot(Base):
    """Historical snapshot of iRacing member statistics."""

    __tablename__ = "member_stats_snapshots"
    __table_args__ = (
        UniqueConstraint("cust_id", "category", "snapshot_date", name="uix_member_day_category"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cust_id: Mapped[int] = mapped_column(ForeignKey("members.cust_id"), index=True)
    category: Mapped[str] = mapped_column(String(64), index=True)
    snapshot_date: Mapped[date] = mapped_column(Date, index=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    irating: Mapped[int | None] = mapped_column(Integer)
    starts: Mapped[int | None] = mapped_column(Integer)
    wins: Mapped[int | None] = mapped_column(Integer)

    member: Mapped[Member] = relationship(back_populates="snapshots")


class License(Base):
    """License key issued to consumers of the service."""

    __tablename__ = "licenses"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    label: Mapped[str | None] = mapped_column(String(255))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


__all__ = ["Base", "License", "Member", "MemberStatsSnapshot"]
