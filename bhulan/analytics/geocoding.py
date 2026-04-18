"""
Reverse-geocode stop centroids into human-readable place names.

Backed by `Nominatim <https://nominatim.org/>`_, the OSM reverse-geocoding
service. Nominatim's `usage policy
<https://operations.osmfoundation.org/policies/nominatim/>`_ requires:

* A descriptive, contactable ``User-Agent``.
* At most one request per second to ``nominatim.openstreetmap.org``.
* Caching of results on the caller side — we must not re-query the same
  coordinate.

The caller interface is a single async function,
:func:`reverse_geocode_stops`, that takes a list of ``(lat, lon)`` pairs
and returns the place name string for each (``None`` if the lookup fails
or times out). The client itself is intentionally tiny; if the
public-hosting app grows enough to need a persistent cache or a rate-
limit bypass (self-hosted Nominatim, Mapbox API), swap the backend here
rather than plumbing through the analytics code.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import httpx

# Round coordinates to ~11m resolution (5 decimal places ≈ 1.1m, 4 ≈ 11m)
# before cache lookup. Two stops resolved separately but within 11m almost
# certainly share a building/address, and this cuts the cache miss rate
# dramatically for a user who re-runs insights on a similar track.
_CACHE_PRECISION = 4

_NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
_USER_AGENT = "bhulan/0.1 (+https://github.com/rslayer/bhulan)"
_REQUEST_TIMEOUT_S = 5.0
# Nominatim policy: at most one request per second per IP.
_MIN_REQUEST_INTERVAL_S = 1.05
_CACHE_TTL_S = 24 * 3600  # 24h; place names don't change often.
_CACHE_MAX_ENTRIES = 10_000


@dataclass
class _CacheEntry:
    value: Optional[str]
    expires_at: float


class _InMemoryGeocodeCache:
    """Tiny TTL cache for ``(lat, lon) → place_name``.

    Not thread-safe; a FastAPI process handles one request at a time per
    worker, which is enough for the public-hosting scope. Swap for Redis
    if you deploy multiple replicas.
    """

    def __init__(self, ttl_s: float = _CACHE_TTL_S, max_entries: int = _CACHE_MAX_ENTRIES):
        self._ttl_s = ttl_s
        self._max_entries = max_entries
        self._data: Dict[Tuple[float, float], _CacheEntry] = {}

    def _key(self, lat: float, lon: float) -> Tuple[float, float]:
        return (round(lat, _CACHE_PRECISION), round(lon, _CACHE_PRECISION))

    def get(self, lat: float, lon: float) -> Tuple[bool, Optional[str]]:
        """Return ``(hit, value)``. ``hit=False`` means the caller should fetch."""
        entry = self._data.get(self._key(lat, lon))
        if entry is None:
            return False, None
        if entry.expires_at < time.time():
            self._data.pop(self._key(lat, lon), None)
            return False, None
        return True, entry.value

    def set(self, lat: float, lon: float, value: Optional[str]) -> None:
        if len(self._data) >= self._max_entries:
            # Evict the single oldest entry; good enough at these sizes.
            oldest = min(self._data.items(), key=lambda kv: kv[1].expires_at)[0]
            self._data.pop(oldest, None)
        self._data[self._key(lat, lon)] = _CacheEntry(
            value=value, expires_at=time.time() + self._ttl_s
        )


# Module-level singleton. Reset via :func:`reset_cache` in tests.
_CACHE = _InMemoryGeocodeCache()


def reset_cache() -> None:
    """Drop all cached entries. For tests only."""
    global _CACHE
    _CACHE = _InMemoryGeocodeCache()


class NominatimClient:
    """Thin async wrapper around Nominatim's ``/reverse`` endpoint.

    Injectable for tests — production code calls
    :func:`reverse_geocode_stops` with the module default.
    """

    def __init__(
        self,
        url: str = _NOMINATIM_URL,
        user_agent: str = _USER_AGENT,
        timeout_s: float = _REQUEST_TIMEOUT_S,
    ) -> None:
        self._url = url
        self._user_agent = user_agent
        self._timeout_s = timeout_s

    async def reverse(
        self, client: httpx.AsyncClient, lat: float, lon: float
    ) -> Optional[str]:
        """Return ``display_name`` for the point, or ``None`` on any failure."""
        try:
            resp = await client.get(
                self._url,
                params={
                    "lat": f"{lat:.6f}",
                    "lon": f"{lon:.6f}",
                    "format": "jsonv2",
                    "zoom": 14,  # roughly "neighborhood"
                    "addressdetails": 0,
                },
                headers={"User-Agent": self._user_agent},
                timeout=self._timeout_s,
            )
        except (httpx.TimeoutException, httpx.TransportError):
            return None

        if resp.status_code != 200:
            return None

        try:
            payload = resp.json()
        except ValueError:
            return None

        name = payload.get("display_name") if isinstance(payload, dict) else None
        return name if isinstance(name, str) and name else None


async def reverse_geocode_stops(
    coords: Sequence[Tuple[float, float]],
    client: Optional[NominatimClient] = None,
    http: Optional[httpx.AsyncClient] = None,
) -> List[Optional[str]]:
    """Reverse-geocode a sequence of ``(lat, lon)`` stop centroids.

    Returns a list of place names the same length as ``coords`` — ``None``
    wherever the lookup failed or returned nothing. Results are cached in
    memory; cache hits do not count against Nominatim's per-second limit.

    The ``client`` and ``http`` parameters are injection points for tests;
    leave them defaulted for production use.
    """
    if not coords:
        return []

    geocoder = client or NominatimClient()

    # Unique-ify the pending lookups so we pay 1 req per unique coord, then
    # fan the results back out to the original order. Preserves request
    # efficiency for tracks that stop at the same landmark twice.
    pending: Dict[Tuple[float, float], None] = {}
    results: List[Optional[str]] = [None] * len(coords)
    cached: Dict[Tuple[float, float], Optional[str]] = {}

    for lat, lon in coords:
        hit, value = _CACHE.get(lat, lon)
        key = (round(lat, _CACHE_PRECISION), round(lon, _CACHE_PRECISION))
        if hit:
            cached[key] = value
        else:
            pending[key] = None

    if pending:
        owned_http = http is None
        http_client = http or httpx.AsyncClient()
        try:
            last_request_at = 0.0
            for key in pending:
                # Respect Nominatim's 1 req/sec rate limit.
                gap = time.monotonic() - last_request_at
                if gap < _MIN_REQUEST_INTERVAL_S:
                    await asyncio.sleep(_MIN_REQUEST_INTERVAL_S - gap)
                lat, lon = key
                name = await geocoder.reverse(http_client, lat, lon)
                last_request_at = time.monotonic()
                _CACHE.set(lat, lon, name)
                cached[key] = name
        finally:
            if owned_http:
                await http_client.aclose()

    for i, (lat, lon) in enumerate(coords):
        key = (round(lat, _CACHE_PRECISION), round(lon, _CACHE_PRECISION))
        results[i] = cached.get(key)

    return results
