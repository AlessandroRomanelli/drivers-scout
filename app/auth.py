"""Authentication helpers for license verification."""
from __future__ import annotations

import logging
from typing import Iterable

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.orm import Session

from .db import get_session
from .models import License
from .settings import settings

logger = logging.getLogger(__name__)

EXEMPT_PATHS: set[str] = {"/health"}


def _get_db_session() -> Iterable[Session]:
    with get_session() as session:
        yield session


def _extract_license_token(
    x_license_key: str | None, authorization: str | None
) -> str | None:
    if x_license_key:
        return x_license_key.strip()

    if authorization:
        token = authorization.strip()
        if token.lower().startswith("bearer "):
            return token[7:].strip()
        return token

    return None


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={"error": "unauthorized", "message": detail},
    )


def require_license(
    request: Request,
    session: Session = Depends(_get_db_session),
    x_license_key: str | None = Header(None, alias="X-License-Key"),
    authorization: str | None = Header(None, alias="Authorization"),
) -> None:
    """Validate the incoming request includes an active license key."""

    if request.url.path in EXEMPT_PATHS:
        return

    if not settings.license_admin_secret:
        return

    if request.url.path.startswith("/admin") or request.url.path.startswith("/licenses/"):
        return

    token = _extract_license_token(x_license_key, authorization)
    if not token:
        logger.warning("License validation failed: missing token", extra={"path": request.url.path})
        raise _unauthorized("Missing license token")

    record = session.get(License, token)
    if not record or not record.active:
        logger.warning(
            "License validation failed: inactive or unknown token",
            extra={"path": request.url.path, "license_key": token},
        )
        raise _unauthorized("Invalid or inactive license")

    logger.debug(
        "License validation succeeded",
        extra={"path": request.url.path, "license_key": token, "label": record.label},
    )


__all__ = ["require_license"]
