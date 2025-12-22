"""Pydantic schemas for request/response payloads."""
from __future__ import annotations

from datetime import datetime

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


__all__ = ["SubscriptionCreate", "SubscriptionResponse"]
