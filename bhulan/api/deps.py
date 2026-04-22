"""FastAPI dependencies shared across routes.

Kept tiny on purpose — every hook in here is used by both ``routes/auth``
and ``routes/history``, plus a best-effort optional hook on
``routes/insights`` so anonymous requests continue to work.
"""

from typing import Optional

from fastapi import Header, HTTPException, status

from bhulan.auth import service as auth_service
from bhulan.auth.service import User
from bhulan.config.settings import settings


def _extract_bearer(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    parts = authorization.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None


def _auth_enabled() -> bool:
    return bool(settings.BHULAN_AUTH_ENABLED)


def current_user_required(
    authorization: Optional[str] = Header(default=None),
) -> User:
    """Dependency that 401s if there's no valid session."""
    if not _auth_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication is not enabled on this server",
        )
    token = _extract_bearer(authorization)
    if token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    user = auth_service.user_for_session(settings.BHULAN_DB_PATH, token)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def current_user_optional(
    authorization: Optional[str] = Header(default=None),
) -> Optional[User]:
    """Dependency that resolves a user when possible but never raises.

    Used on ``/v1/insights`` so anonymous callers still get their report
    but authenticated callers also get the side effect of having the run
    saved to their history.
    """
    if not _auth_enabled():
        return None
    token = _extract_bearer(authorization)
    if token is None:
        return None
    return auth_service.user_for_session(settings.BHULAN_DB_PATH, token)
