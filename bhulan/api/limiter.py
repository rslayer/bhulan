"""
Shared slowapi :class:`Limiter` instance and key function.

Route decorators (``@limiter.limit(...)``) and slowapi's exception handler
(which reads ``request.app.state.limiter._inject_headers``) must operate on
the *same* Limiter object — otherwise limits are tracked on one instance
while header injection reads from another that has no knowledge of the
window state. Keeping the constructor in its own module avoids a circular
import: ``app.py`` imports the router from ``routes/insights.py``, and both
sides need the limiter at module load time.
"""

from typing import List

from fastapi import Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from bhulan.config.settings import settings


def _parse_trusted_proxies(raw: str) -> List[str]:
    """Parse a comma-separated allowlist of trusted proxy IPs / wildcard."""
    raw = (raw or "").strip()
    if not raw:
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


_TRUSTED_PROXIES = _parse_trusted_proxies(settings.TRUSTED_PROXIES)


def rate_limit_key(request: Request) -> str:
    """
    Return the client identifier used for per-IP rate limits.

    When the service is deployed behind a reverse proxy / CDN (the expected
    shape for the public web app), ``request.client.host`` is the proxy's
    IP — every user would share one rate-limit bucket. If
    ``TRUSTED_PROXIES`` is set and the peer matches, honor the leftmost
    ``X-Forwarded-For`` entry instead. Falls back to the direct peer IP for
    local development or when no proxy is configured.
    """
    peer = get_remote_address(request)
    if not _TRUSTED_PROXIES:
        return peer
    if "*" not in _TRUSTED_PROXIES and peer not in _TRUSTED_PROXIES:
        return peer
    fwd = request.headers.get("x-forwarded-for")
    if not fwd:
        return peer
    # X-Forwarded-For: client, proxy1, proxy2 — leftmost is the original client.
    client = fwd.split(",", 1)[0].strip()
    return client or peer


#: Single process-wide rate limiter. Individual per-route limits are attached
#: via ``@limiter.limit(...)`` on the handlers in ``bhulan/api/routes``.
limiter = Limiter(key_func=rate_limit_key, default_limits=[])
