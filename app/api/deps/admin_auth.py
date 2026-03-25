from __future__ import annotations

from fastapi import Cookie, Header, HTTPException, status

from app.core.config import get_settings


def require_admin_access(
    x_dwellio_admin_token: str | None = Header(default=None),
    dwellio_admin_token: str | None = Cookie(default=None),
) -> None:
    provided_token = x_dwellio_admin_token or dwellio_admin_token
    expected_token = get_settings().admin_api_token

    if not provided_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin token required.",
        )
    if provided_token != expected_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid admin token.",
        )
