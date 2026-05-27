"""Pydantic schemas for request/response payloads."""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from .settings import settings


class SubscriptionCreate(BaseModel):
    webhook_url: HttpUrl
    category: str
    min_irating: int | None = Field(None, ge=0)

    @field_validator("category")
    @classmethod
    def validate_category(cls, value: str) -> str:
        normalized = value.strip()
        if normalized not in settings.categories_normalized:
            raise ValueError("Unsupported category")
        return normalized


class SubscriptionResponse(BaseModel):
    id: int
    license_key: str
    category: str
    min_irating: int | None
    webhook_url: HttpUrl
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class MemberLookupRequest(BaseModel):
    """Bulk member lookup by display name."""

    names: list[str] = Field(..., min_length=1, max_length=500)
    category: str | None = "sports_car"

    @field_validator("names")
    @classmethod
    def validate_names(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        for raw in value:
            if not isinstance(raw, str):
                raise ValueError("names must be strings")
            stripped = raw.strip()
            if not stripped:
                raise ValueError("names entries must be non-empty after strip")
            if len(stripped) > 200:
                raise ValueError("names entries must be 200 chars or fewer")
            cleaned.append(raw)
        return cleaned


class MemberLookupResolution(BaseModel):
    query: str
    match_type: Literal["exact", "folded"] | None = None
    cust_id: int | None = None
    display_name: str | None = None
    location: str | None = None


class MemberLookupResponse(BaseModel):
    resolutions: list[MemberLookupResolution]


__all__ = [
    "SubscriptionCreate",
    "SubscriptionResponse",
    "MemberLookupRequest",
    "MemberLookupResolution",
    "MemberLookupResponse",
]
