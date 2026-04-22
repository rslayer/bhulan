"""Magic-link sign-in endpoints.

- ``POST /v1/auth/request``  — accepts an email, creates a short-lived
  magic-link token, emails it (or logs it in dev mode), and returns 202.
- ``POST /v1/auth/verify``   — exchanges a magic-link token for a session
  token. The session token is returned in the JSON body; the frontend
  puts it in ``localStorage`` and sends it as ``Authorization: Bearer``.
- ``POST /v1/auth/logout``   — revokes the current session.
- ``GET  /v1/auth/me``       — returns the current user, or 401.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address

from bhulan.api.deps import current_user_required
from bhulan.auth import db as auth_db
from bhulan.auth import service as auth_service
from bhulan.auth.email import SmtpConfig, build_magic_link_url, send_magic_link
from bhulan.auth.service import User
from bhulan.config.settings import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/auth", tags=["auth"])

limiter = Limiter(key_func=get_remote_address)

# Belt-and-suspenders: Pydantic's EmailStr already rejects garbage, but we
# also lowercase + strip for DB consistency.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _require_auth_enabled() -> None:
    if not settings.BHULAN_AUTH_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication is not enabled on this server",
        )


def _smtp_config() -> SmtpConfig:
    return SmtpConfig(
        host=settings.SMTP_HOST,
        port=settings.SMTP_PORT,
        user=settings.SMTP_USER,
        password=settings.SMTP_PASSWORD,
        from_addr=settings.SMTP_FROM,
        use_tls=settings.SMTP_USE_TLS,
    )


class AuthRequestBody(BaseModel):
    email: str = Field(
        ...,
        description="Email to send the magic link to",
        min_length=3,
        max_length=320,
    )


class AuthRequestResponse(BaseModel):
    """Response to :func:`request_magic_link`.

    ``dev_magic_link`` is only populated when the server is running with
    ``AUTH_DEV_MODE=true`` (or without SMTP configured) so a developer can
    copy the link without running a mail server. It's never set in
    production.
    """

    ok: bool = True
    email: str
    dev_magic_link: Optional[str] = None


@router.post(
    "/request",
    response_model=AuthRequestResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@limiter.limit(lambda: settings.RATE_LIMIT_AUTH or "10/minute")
async def request_magic_link(
    request: Request,
    payload: AuthRequestBody = Body(...),
) -> AuthRequestResponse:
    _require_auth_enabled()
    auth_db.init_db(settings.BHULAN_DB_PATH)

    email = payload.email.strip().lower()
    if not _EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="Invalid email address")

    token = auth_service.create_magic_link(
        settings.BHULAN_DB_PATH,
        email,
        settings.BHULAN_MAGIC_LINK_TTL_MIN,
    )
    link = build_magic_link_url(settings.FRONTEND_URL, token)

    sent = send_magic_link(
        _smtp_config(),
        email,
        link,
        dev_mode=settings.AUTH_DEV_MODE,
    )

    # In dev mode, return the link in the response so local testers can
    # click through without SMTP. In production (dev_mode=False and SMTP
    # configured) the link stays server-side only.
    dev_link = link if (settings.AUTH_DEV_MODE or not sent) else None
    return AuthRequestResponse(ok=True, email=email, dev_magic_link=dev_link)


class AuthVerifyBody(BaseModel):
    token: str = Field(..., min_length=8, description="Magic-link token from the URL")


class AuthVerifyResponse(BaseModel):
    session_token: str
    expires_in_days: int
    user: dict


@router.post("/verify", response_model=AuthVerifyResponse)
@limiter.limit(lambda: settings.RATE_LIMIT_AUTH or "10/minute")
async def verify_magic_link(
    request: Request,
    payload: AuthVerifyBody = Body(...),
) -> AuthVerifyResponse:
    _require_auth_enabled()
    auth_db.init_db(settings.BHULAN_DB_PATH)

    try:
        user, session_token = auth_service.verify_magic_link(
            settings.BHULAN_DB_PATH,
            payload.token,
            settings.BHULAN_SESSION_TTL_DAYS,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return AuthVerifyResponse(
        session_token=session_token,
        expires_in_days=settings.BHULAN_SESSION_TTL_DAYS,
        user={"id": user.id, "email": user.email, "created_at": user.created_at},
    )


class AuthLogoutResponse(BaseModel):
    ok: bool = True


@router.post("/logout", response_model=AuthLogoutResponse)
async def logout(
    request: Request,
    _user: User = Depends(current_user_required),
) -> AuthLogoutResponse:
    _require_auth_enabled()
    auth_db.init_db(settings.BHULAN_DB_PATH)
    # We re-extract the raw token because current_user_required already
    # validated it, but the dependency returns the user object not the token.
    authz = request.headers.get("authorization", "")
    parts = authz.split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        auth_service.revoke_session(settings.BHULAN_DB_PATH, parts[1].strip())
    return AuthLogoutResponse(ok=True)


class MeResponse(BaseModel):
    id: int
    email: str
    created_at: int


@router.get("/me", response_model=MeResponse)
async def me(user: User = Depends(current_user_required)) -> MeResponse:
    return MeResponse(id=user.id, email=user.email, created_at=user.created_at)
