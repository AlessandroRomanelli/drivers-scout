"""Application configuration using pydantic-settings."""
from __future__ import annotations

from pathlib import Path
from typing import List

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = Field("drivers-scout", description="Service name for logging")
    log_level: str = Field("INFO", description="Log level")
    log_file: Path = Field(Path("drivers-scout.log"), description="File path for log output")
    host: str = Field("0.0.0.0", description="Host for the HTTP server")
    port: int = Field(8000, description="Port for the HTTP server")

    app_timezone: str = Field("UTC", description="Timezone for scheduling")
    schedule_hour: int = Field(23, ge=0, le=23, description="Hour for daily job")
    schedule_minute: int = Field(55, ge=0, le=59, description="Minute for daily job")
    scheduler_enabled: bool = Field(True, description="Toggle scheduler for local dev")

    snapshots_dir: Path = Field(
        Path("snapshots"), description="Directory for storing downloaded CSV snapshots"
    )

    iracing_username: str = Field(..., description="iRacing account username")
    iracing_password: str = Field(..., description="Opaque iRacing password string")
    iracing_client_id: str = Field("ar-pwlimited", description="iRacing OAuth client id")
    iracing_client_secret: str = Field(..., description="iRacing OAuth client secret")
    iracing_scope: str = Field("iracing.auth", description="OAuth scope")
    iracing_rate_limit_rpm: int = Field(60, description="Rate limit RPM for iRacing API")

    categories: str = Field(
        "sports_car",
        description="Categories to fetch as comma-separated values",
        json_schema_extra={"example": "sports_car"},
    )

    http_timeout_seconds: float = Field(15.0, description="HTTP client timeout")
    rate_limit_burst: int = Field(5, description="Burst size for rate limiting")

    database_url: str = Field(
        "sqlite:///./iracing_stats.db", description="SQLAlchemy database URL"
    )
    license_key_length: int = Field(
        24, ge=8, description="Length of generated license keys"
    )
    license_key_alphabet: str = Field(
        "ABCDEFGHJKLMNPQRSTUVWXYZ23456789", description="Characters used for license keys"
    )
    license_admin_secret: str | None = Field(
        None, description="Shared secret required for admin license endpoints"
    )

    model_config = SettingsConfigDict(
        env_file=(Path(__file__).parent.parent / ".env"),
        env_file_encoding="utf-8",
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )

    @computed_field
    @property
    def categories_normalized(self) -> List[str]:
        if isinstance(self.categories, str):
            return [v.strip() for v in self.categories.split(",") if v.strip()]
        return [c.strip() for c in self.categories if c.strip()]


settings = Settings()
