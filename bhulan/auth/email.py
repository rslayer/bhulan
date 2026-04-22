"""Magic-link email delivery.

In production this sends an email via SMTP. For local dev and CI the
``AUTH_DEV_MODE`` flag short-circuits to logging the link so a developer
doesn't need a mail server to try the flow end-to-end.
"""

from __future__ import annotations

import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SmtpConfig:
    host: Optional[str]
    port: int
    user: Optional[str]
    password: Optional[str]
    from_addr: str
    use_tls: bool


def build_magic_link_url(frontend_url: str, token: str) -> str:
    """Build the URL the user clicks. We use a fragment so the token
    never leaves the browser (fragments aren't sent to the server on a
    plain GET)."""
    base = frontend_url.rstrip("/")
    return f"{base}/#magic={token}"


def send_magic_link(
    smtp: SmtpConfig,
    to_email: str,
    magic_link: str,
    *,
    dev_mode: bool,
) -> bool:
    """Send the magic link. Returns True if an email was actually sent.

    When ``dev_mode`` is True or when no SMTP host is configured, the
    function logs the link and returns False. Callers can then decide to
    echo the link in the API response for local use.
    """
    if dev_mode or not smtp.host:
        logger.info("[bhulan.auth] magic link for %s: %s", to_email, magic_link)
        return False

    msg = EmailMessage()
    msg["Subject"] = "Your Bhulan sign-in link"
    msg["From"] = smtp.from_addr
    msg["To"] = to_email
    msg.set_content(
        "Click the link below to sign in to Bhulan. It expires in 15 minutes.\n\n"
        f"{magic_link}\n\n"
        "If you didn't request this, you can ignore this email."
    )

    try:
        with smtplib.SMTP(smtp.host, smtp.port, timeout=10) as s:
            if smtp.use_tls:
                s.starttls()
            if smtp.user and smtp.password:
                s.login(smtp.user, smtp.password)
            s.send_message(msg)
        return True
    except Exception as exc:  # pragma: no cover - network-dependent
        logger.warning(
            "[bhulan.auth] SMTP send failed for %s: %s", to_email, exc
        )
        return False
