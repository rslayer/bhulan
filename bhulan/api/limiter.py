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
    ``TRUSTED_PROXIES`` is set and the peer matches, honor the **rightmost**
    ``X-Forwarded-For`` entry instead (this is the IP the trusted proxy
    itself appended — i.e. the peer it actually saw). Falls back to the
    direct peer IP for local development or when no proxy is configured.

    Why rightmost and not leftmost: the leftmost XFF entry is whatever the
    client sent, so an attacker can spoof it to rotate keys on every
    request and bypass the rate limit entirely. Standard proxies
    (nginx's ``proxy_add_x_forwarded_for``, Apache's ``mod_remoteip``,
    AWS ALB, Cloudflare) *append* the observed peer IP, so the trusted
    value is at the right. This assumes a single trusted proxy hop;
    multi-hop deployments need to strip N known-trusted entries from
    the right.
    """
    peer = get_remote_address(request)
    if not _TRUSTED_PROXIES:
        return peer
    if "*" not in _TRUSTED_PROXIES and peer not in _TRUSTED_PROXIES:
        return peer
    fwd = request.headers.get("x-forwarded-for")
    if not fwd:
        return peer
    # X-Forwarded-For: client, proxy1, proxy2 — the *rightmost* entry is the
    # one the trusted proxy itself appended, i.e. the peer it actually saw.
    # Taking the leftmost entry would let a client spoof any key they want.
    client = fwd.rsplit(",", 1)[-1].strip()
    return client or peer


#: Single process-wide rate limiter. Individual per-route limits are attached
#: via ``@limiter.limit(...)`` on the handlers in ``bhulan/api/routes``.
#:
#: We deliberately do NOT set ``headers_enabled=True``: slowapi implements it by
#: injecting headers into *successful* responses too, mutating the returned
#: object — and our handlers return pydantic models, so it fails and turns a 200
#: into a 500 (verified). It is also the only path by which slowapi would emit a
#: ``Retry-After`` header, so instead a small custom 429 handler in
#: ``bhulan/api/app.py`` adds ``Retry-After`` (the client contract that matters)
#: without touching the success path. NB: storage is in-memory and per-process,
#: so with multiple workers each keeps its own counters (see DEPLOY.md).
limiter = Limiter(key_func=rate_limit_key, default_limits=[])
