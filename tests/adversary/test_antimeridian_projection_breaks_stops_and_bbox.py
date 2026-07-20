"""
Defect (silently-wrong-answer): a track that GPS-jitters across the
antimeridian (longitude +/-180) -- e.g. a device parked a few meters from
the 180th meridian, like American Samoa, Fiji, or the Aleutian Islands --
is reported as never stopping (``stops: []``) and its bounding box is
reported as spanning almost the entire globe's longitude, even though every
sample sits within a few meters of a single spot.

Root cause: ``bhulan/analytics/geodesy.py::latlon_to_xy_m`` projects
lat/lon onto a local tangent plane using a *linear* longitude difference
(``x = (lons - lon0) * meters_per_deg_lon``) with no wraparound handling.
For two points on opposite sides of the antimeridian (lon=179.9999 and
lon=-179.9999 -- ~21.9m apart in reality, per
:func:`bhulan.analytics.geodesy.haversine_m`), the *linear* longitude
difference is computed as ~359.9998 degrees instead of the true ~0.0002
degrees, so the projected x-coordinates land roughly 40,000 km apart (close
to the Earth's circumference) instead of ~22m apart.

``bhulan/analytics/stops.py::detect_stops`` and
``bhulan/analytics/hotspots.py::detect_hotspots`` both cluster on this
broken projection (``latlon_to_xy_m``), so a real, tight dwell at the
antimeridian never clusters into a stop, and a shared/pooled hotspot in
``/v1/compare`` would suffer the same fate. ``bhulan/analytics/geodesy.py::
bounding_box`` has the identical wraparound blindness (naive min/max over
raw longitude values), so ``InsightsSummary.bbox`` for the same track
reports a span of the *entire* longitude range (-179.9999 to 179.9999, i.e.
~360 degrees) instead of the true sub-meter-wide sliver around +/-180 -- any
client that zooms a map to fit this bbox would zoom out to the whole world
instead of a single point.

Notably ``bhulan/analytics/geodesy.py::haversine_m`` /
``haversine_vec_m`` do NOT have this bug (the half-angle trig formula
naturally wraps at +/-180 degrees), so ``summary.total_distance_km`` for
this exact track is correctly small -- the same request produces a
correct total-distance figure alongside a stop count and bbox that are
both dramatically wrong, all from the same 30-sample, single-location
input.

Impact: any real-world track recorded near the antimeridian (a genuine,
non-contrived location -- Fiji, Tonga, Kiribati, the Aleutians, Chukotka --
silently loses all stop detection and gets a nonsensical "spans the whole
world" bounding box, with zero error/warning surfaced to the caller.
Reachable via ordinary, unauthenticated ``points`` input to ``/v1/insights``
-- no crafted edge case beyond "GPS jitter happens to straddle the
180th meridian".
"""

from __future__ import annotations

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from bhulan.analytics.geodesy import haversine_m

_T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LAT = 10.0
_LON_A = 179.9999
_LON_B = -179.9999


def _jitter_track(n: int = 30, step_s: int = 10) -> list[dict]:
    """``n`` samples alternating between two points ~22m apart, straddling
    the antimeridian -- a realistic GPS-jitter dwell at a single spot.
    """
    points = []
    t = _T0
    for i in range(n):
        lon = _LON_A if i % 2 == 0 else _LON_B
        points.append({"lat": _LAT, "lon": lon, "ts_utc": t.isoformat()})
        t += timedelta(seconds=step_s)
    return points


def test_antimeridian_dwell_is_detected_as_a_stop(client: TestClient):
    """A device jittering ~22m from the 180th meridian for 290 seconds is a
    textbook stop (well within the default 50m ``stop_radius_m`` and 5-minute
    ``min_stop_minutes`` easily satisfied by lowering the threshold below).
    ``detect_stops``'s broken antimeridian projection reports zero stops
    instead.
    """
    true_separation_m = haversine_m(_LAT, _LON_A, _LAT, _LON_B)
    assert true_separation_m < 50.0  # sanity: genuinely within any sane stop radius

    payload = {
        "points": _jitter_track(),
        "options": {"stop_radius_m": 50.0, "min_stop_minutes": 1.0},
    }
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    report = r.json()

    assert len(report["stops"]) == 1, (
        f"a 30-sample dwell with true spread {true_separation_m:.1f}m "
        f"(well within stop_radius_m=50) was reported as {len(report['stops'])} "
        f"stops instead of 1 -- latlon_to_xy_m's local-tangent-plane "
        f"projection has no antimeridian wraparound handling, so samples on "
        f"opposite sides of longitude +/-180 project to points ~40,000 km "
        f"apart instead of the true ~{true_separation_m:.0f}m, breaking "
        f"detect_stops's spatial clustering for any real-world dwell near "
        f"the 180th meridian (Fiji, Tonga, the Aleutians, ...)."
    )


def test_antimeridian_bbox_reports_almost_the_whole_globe(client: TestClient):
    """The same tight, single-location dwell must produce a bbox that
    reflects its true (sub-100m) extent, not one spanning virtually the
    entire longitude range of the planet.
    """
    payload = {"points": _jitter_track()}
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    bbox = r.json()["summary"]["bbox"]
    assert bbox is not None

    lon_span_deg = bbox["max_lon"] - bbox["min_lon"]
    assert lon_span_deg < 1.0, (
        f"a track whose samples are all within ~22m of the 180th meridian "
        f"reports a bbox longitude span of {lon_span_deg:.4f} degrees "
        f"(min_lon={bbox['min_lon']}, max_lon={bbox['max_lon']}) -- "
        f"bounding_box() takes a naive min/max over raw longitude values "
        f"with no antimeridian wraparound, so a sub-100m-wide real-world "
        f"extent is reported as spanning almost the entire globe "
        f"(~360 degrees); a client zooming a map to 'fit' this bbox would "
        f"zoom out to the whole world instead of a single point."
    )


def test_antimeridian_total_distance_is_correct_unlike_stops_and_bbox(client: TestClient):
    """Control case: total_distance_km uses haversine_vec_m (trig-based,
    naturally wraps at +/-180), so it gets this exact track right -- proving
    the stop-count and bbox failures above are a projection bug, not bad
    input or a blanket "antimeridian is unsupported" limitation.
    """
    payload = {"points": _jitter_track()}
    r = client.post("/v1/insights", json=payload)
    assert r.status_code == 200
    summary = r.json()["summary"]

    true_separation_m = haversine_m(_LAT, _LON_A, _LAT, _LON_B)
    expected_km = (true_separation_m * 29) / 1000.0  # 30 points -> 29 hops
    assert summary["total_distance_km"] == pytest.approx(expected_km, rel=0.05)
