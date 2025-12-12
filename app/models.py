"""SQLAlchemy models for member statistics."""
from __future__ import annotations

from datetime import datetime, date, timezone

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy import JSON as SAJSON
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

    stats_json: Mapped[dict | None] = mapped_column(SAJSON)
    irating: Mapped[int | None] = mapped_column(Integer)
    license_class: Mapped[str | None] = mapped_column(String(8))
    license_sr: Mapped[float | None] = mapped_column()
    ttrating: Mapped[int | None] = mapped_column(Integer)
    starts: Mapped[int | None] = mapped_column(Integer)
    wins: Mapped[int | None] = mapped_column(Integer)
    avg_inc: Mapped[float | None] = mapped_column()

    member: Mapped[Member] = relationship(back_populates="snapshots")


__all__ = ["Base", "Member", "MemberStatsSnapshot"]
