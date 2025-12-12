"""Client for interacting with the iRacing OAuth and data endpoints."""
from __future__ import annotations

import asyncio
import csv
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List

import httpx

from .settings import settings

logger = logging.getLogger(__name__)

TOKEN_URL = "https://oauth.iracing.com/oauth2/token"
DATA_URL_TEMPLATE = "https://members-ng.iracing.com/data/driver_stats_by_category/{category}"


@dataclass
class TokenInfo:
    """Holds token metadata for reuse."""

    access_token: str
    refresh_token: str | None
    expires_at: datetime

    def is_expiring(self, threshold_seconds: int = 60) -> bool:
        return datetime.now(timezone.utc) + timedelta(seconds=threshold_seconds) >= self.expires_at


class IRacingClient:
    """Lightweight client for iRacing OAuth and CSV retrieval."""

    def __init__(self) -> None:
        self._token: TokenInfo | None = None
        self._client = httpx.AsyncClient(timeout=settings.http_timeout_seconds)
        self._rate_limit_lock = asyncio.Semaphore(settings.rate_limit_burst)
        self._rate_reset: datetime | None = None

    async def _throttle(self) -> None:
        now = datetime.now(timezone.utc)
        window = timedelta(seconds=60)
        if self._rate_reset and now >= self._rate_reset:
            self._rate_reset = None
        if self._rate_reset is None:
            self._rate_reset = now + window
            self._rate_limit_lock = asyncio.Semaphore(settings.iracing_rate_limit_rpm)
        await self._rate_limit_lock.acquire()

    async def _post_token(self, data: Dict[str, str]) -> dict:
        for attempt in range(3):
            try:
                response = await self._client.post(
                    TOKEN_URL, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}
                )
                response.raise_for_status()
                return response.json()
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
        return {}

    async def login(self) -> TokenInfo:
        """Perform OAuth password_limited login."""
        payload = {
            "grant_type": "password_limited",
            "client_id": settings.iracing_client_id,
            "client_secret": settings.iracing_client_secret,
            "username": settings.iracing_username,
            "password": settings.iracing_password,
            "scope": settings.iracing_scope,
        }
        token_data = await self._post_token(payload)
        token = self._build_token(token_data)
        logger.info("Obtained new access token")
        self._token = token
        return token

    async def refresh(self) -> TokenInfo:
        if not self._token or not self._token.refresh_token:
            return await self.login()
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self._token.refresh_token,
            "client_id": settings.iracing_client_id,
            "client_secret": settings.iracing_client_secret,
            "scope": settings.iracing_scope,
        }
        token_data = await self._post_token(payload)
        token = self._build_token(token_data)
        logger.info("Refreshed access token")
        self._token = token
        return token

    def _build_token(self, token_data: Dict[str, Any]) -> TokenInfo:
        access_token = token_data["access_token"]
        expires_in = int(token_data.get("expires_in", 600))
        refresh_token = token_data.get("refresh_token")
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        return TokenInfo(access_token=access_token, refresh_token=refresh_token, expires_at=expires_at)

    async def _ensure_token(self) -> TokenInfo:
        if not self._token:
            return await self.login()
        if self._token.is_expiring():
            return await self.refresh()
        return self._token

    async def _authorized_get(self, url: str) -> httpx.Response:
        token = await self._ensure_token()
        headers = {"Authorization": f"Bearer {token.access_token}"}
        for attempt in range(3):
            try:
                await self._throttle()
                resp = await self._client.get(url, headers=headers)
                if resp.status_code == 401:
                    if attempt == 0:
                        await self.refresh()
                        token = self._token  # type: ignore[assignment]
                        headers = {"Authorization": f"Bearer {token.access_token}"}
                        continue
                    if attempt == 1:
                        await self.login()
                        token = self._token  # type: ignore[assignment]
                        headers = {"Authorization": f"Bearer {token.access_token}"}
                        continue
                resp.raise_for_status()
                return resp
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError("Failed to fetch after retries")

    async def _unauthorized_get(self, url: str) -> httpx.Response:
        for attempt in range(3):
            try:
                await self._throttle()
                resp = await self._client.get(url)
                resp.raise_for_status()
                return resp
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError("Failed to fetch after retries")

    async def fetch_category_csv(self, category: str) -> List[Dict[str, Any]]:
        """Fetch and parse category CSV, returning list of dict rows."""
        data_url = DATA_URL_TEMPLATE.format(category=category)
        category_resp = await self._authorized_get(data_url)
        link = category_resp.json().get("link")
        if not link:
            raise RuntimeError("Missing CSV link in response")
        csv_resp = await self._unauthorized_get(link)
        rows: List[Dict[str, Any]] = []
        decoded = csv_resp.text
        reader = csv.DictReader(decoded.splitlines())
        for row in reader:
            rows.append(row)
        return rows

    async def close(self) -> None:
        await self._client.aclose()


def parse_license(raw: str | None) -> tuple[str | None, float | None]:
    """Parse license string into class and safety rating."""
    if not raw:
        return None, None
    parts = raw.split()
    if len(parts) >= 2:
        license_class = parts[0]
        try:
            license_sr = float(parts[1])
        except ValueError:
            return license_class, None
        return license_class, license_sr
    return raw, None


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Extract typed fields from a CSV row with parsed values."""

    license_class, license_sr = parse_license(row.get("CLASS"))

    def parse_int(key: str) -> int | None:
        try:
            return int(row.get(key, ""))
        except (TypeError, ValueError):
            return None

    def parse_float(key: str) -> float | None:
        try:
            return float(row.get(key, ""))
        except (TypeError, ValueError):
            return None

    return {
        "cust_id": parse_int("CUSTID"),
        "display_name": row.get("DRIVER"),
        "irating": parse_int("IRATING"),
        "ttrating": parse_int("TTRATING"),
        "license_class": license_class,
        "license_sr": license_sr,
        "starts": parse_int("STARTS"),
        "wins": parse_int("WINS"),
        "avg_inc": parse_float("AVG_INC"),
        "raw": row,
    }


def normalize_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize a collection of CSV rows without filtering by cust_id."""

    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(normalize_row(row))
    return normalized
