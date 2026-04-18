"""
Tests for the Nominatim reverse-geocoding wrapper.

All tests stub the HTTP call via a :class:`FakeNominatim` so the suite
stays hermetic (no outbound requests). The rate-limit sleep is patched
out globally to keep the suite fast.
"""

from __future__ import annotations

import pytest

import bhulan.analytics.geocoding as geocoding


@pytest.fixture(autouse=True)
def _reset_cache_and_disable_sleep(monkeypatch):
    geocoding.reset_cache()

    async def _no_sleep(_):
        return None

    monkeypatch.setattr(geocoding.asyncio, "sleep", _no_sleep)
    yield
    geocoding.reset_cache()


class FakeNominatim(geocoding.NominatimClient):
    """Deterministic stand-in for :class:`NominatimClient` used in tests."""

    def __init__(self, responses):
        super().__init__()
        self._responses = responses
        self.calls: list = []

    async def reverse(self, client, lat, lon):
        self.calls.append((round(lat, 4), round(lon, 4)))
        return self._responses.get((round(lat, 4), round(lon, 4)))


@pytest.mark.asyncio
async def test_reverse_geocode_returns_names_in_input_order():
    fake = FakeNominatim(
        {
            (12.97, 77.59): "Cubbon Park, Bangalore",
            (40.7128, -74.0060): "New York City",
        }
    )
    out = await geocoding.reverse_geocode_stops(
        [(12.97, 77.59), (40.7128, -74.0060)],
        client=fake,
        http=object(),  # Never used by FakeNominatim.
    )
    assert out == ["Cubbon Park, Bangalore", "New York City"]


@pytest.mark.asyncio
async def test_cache_hit_prevents_second_lookup():
    fake = FakeNominatim({(12.97, 77.59): "Cubbon Park"})
    await geocoding.reverse_geocode_stops([(12.97, 77.59)], client=fake, http=object())
    # Ask again — should come from cache, no new HTTP call.
    out = await geocoding.reverse_geocode_stops([(12.97, 77.59)], client=fake, http=object())
    assert out == ["Cubbon Park"]
    assert len(fake.calls) == 1, "cache should have suppressed the second call"


@pytest.mark.asyncio
async def test_duplicate_coords_within_request_are_deduplicated():
    fake = FakeNominatim({(12.97, 77.59): "Cubbon Park"})
    out = await geocoding.reverse_geocode_stops(
        [(12.97, 77.59), (12.97, 77.59), (12.97, 77.59)],
        client=fake,
        http=object(),
    )
    assert out == ["Cubbon Park"] * 3
    assert len(fake.calls) == 1


@pytest.mark.asyncio
async def test_missing_result_returns_none():
    fake = FakeNominatim({})  # Every coord returns None.
    out = await geocoding.reverse_geocode_stops(
        [(0.0, 0.0)], client=fake, http=object()
    )
    assert out == [None]


@pytest.mark.asyncio
async def test_empty_input_does_nothing():
    fake = FakeNominatim({})
    out = await geocoding.reverse_geocode_stops([], client=fake, http=object())
    assert out == []
    assert fake.calls == []
