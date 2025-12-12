"""Application configuration using pydantic-settings."""
from __future__ import annotations

from pathlib import Path
from typing import List

from pydantic import Field, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = Field("drivers-scout", description="Service name for logging")
    log_level: str = Field("INFO", description="Log level")
    host: str = Field("0.0.0.0", description="Host for the HTTP server")
    port: int = Field(8000, description="Port for the HTTP server")

    app_timezone: str = Field("Europe/Zurich", description="Timezone for scheduling")
    schedule_hour: int = Field(0, ge=0, le=23, description="Hour for daily job")
    schedule_minute: int = Field(0, ge=0, le=59, description="Minute for daily job")
    scheduler_enabled: bool = Field(True, description="Toggle scheduler for local dev")

    database_url: str = Field(
        "sqlite:///./iracing_stats.db", description="Database URL; SQLite by default"
    )

    iracing_username: str = Field(..., description="iRacing account username")
    iracing_password: str = Field(..., description="Opaque iRacing password string")
    iracing_client_id: str = Field("ar-pwlimited", description="iRacing OAuth client id")
    iracing_client_secret: str = Field(..., description="iRacing OAuth client secret")
    iracing_scope: str = Field("iracing.auth", description="OAuth scope")
    iracing_rate_limit_rpm: int = Field(60, description="Rate limit RPM for iRacing API")

    cust_ids: List[int] = Field(
        default_factory=list,
        description="Comma separated cust_ids to track",
        json_schema_extra={"example": "419877,221850"},
    )
    categories: List[str] = Field(
        default_factory=lambda: ["sports_car"],
        description="Categories to fetch",
        json_schema_extra={"example": "sports_car"},
    )

    http_timeout_seconds: float = Field(15.0, description="HTTP client timeout")
    rate_limit_burst: int = Field(5, description="Burst size for rate limiting")

    model_config = SettingsConfigDict(
        env_file=(Path(__file__).parent.parent / ".env"),
        env_file_encoding="utf-8",
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("cust_ids", mode="before")
    @classmethod
    def parse_cust_ids(cls, value: str | List[int]) -> List[int]:
        if isinstance(value, str):
            return [int(v.strip()) for v in value.split(",") if v.strip()]
        return value

    @field_validator("categories", mode="before")
    @classmethod
    def parse_categories(cls, value: str | List[str]) -> List[str]:
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        return value

    @computed_field
    @property
    def categories_normalized(self) -> List[str]:
        return [c.strip() for c in self.categories if c.strip()]

    @computed_field
    @property
    def cust_ids_normalized(self) -> List[int]:
        return [int(c) for c in self.cust_ids if str(c).strip()]


settings = Settings()
