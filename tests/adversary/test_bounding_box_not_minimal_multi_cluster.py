"""
Defect (silently-wrong-answer): ``bhulan/analytics/geodesy.py::bounding_box``
documents itself as returning "the minimal-longitude-span box", and cycle
4's antimeridian fix added exactly one extra candidate framing (longitudes
shifted into ``[0, 360)``, i.e. a cut at 0 degrees) alongside the naive
framing (a cut at +/-180 degrees), picking whichever of those two is
tighter. That correctly handles the two-cluster case this cycle targeted
(a cluster split across +/-180), but it is not actually the minimal box in
general: the true minimal longitude-span box is found by cutting at the
single *largest* gap between consecutive points around the longitude
circle, which can sit anywhere -- not just at 0 or +/-180.

For three or more points/clusters spread around the circle such that the
largest gap falls somewhere other than near 0 or +/-180, both of
``bounding_box``'s candidate cuts miss the true largest gap, and it
returns a box far wider than necessary -- while still (incorrectly)
labelling it as "the minimal box" and, in this example, as an
antimeridian-crossing box, even though the true minimal box crosses the
antimeridian at different, much tighter, coordinates.

Concretely: six points at longitudes -170, -100, -30, 0, 60, 170 (all at
the same latitude, so only longitude framing is in play). The true minimal
box (found by cutting at the largest circular gap, between 60 and 170 --
a gap of 110 degrees) has longitude span 250 degrees, running from 170
(the antimeridian side) to 60. ``bounding_box`` instead reports a box
running from 0 to -30 the "long way" (through +/-180), spanning 330
degrees -- 80 degrees (about 8,900 km at the equator) wider than the true
minimal box, and mislabels the crossing location as 0 -> -30 rather than
the true 170 -> 60.

This directly hits the spec's own stated trap for this cycle: "A track
that legitimately spans a *wide* longitude range ... vs. a jitter across
+/-180 -- the fix must not mistake one for the other." Reachable via
ordinary ``points`` input to ``/v1/insights`` (no crafted single-point
antimeridian jitter needed -- any track visiting several genuinely distant
longitudes triggers it).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import bhulan.storage.mongo_repo as mongo_repo  # noqa: F401  (fixture parity, see conftest)

_LAT = 10.0
_LONS = [-170.0, -100.0, -30.0, 0.0, 60.0, 170.0]
_TRUE_MINIMAL_SPAN_DEG = 250.0  # cut at the true largest gap: 60 -> 170 (110 deg)


def _bbox_span_deg(min_lon: float, max_lon: float) -> float:
    """Longitude span implied by ``bounding_box``'s own convention: if
    ``min_lon > max_lon`` the box crosses the antimeridian and runs the
    "long way" from ``min_lon`` east to ``max_lon``.
    """
    if min_lon <= max_lon:
        return max_lon - min_lon
    return (max_lon + 360.0) - min_lon


def test_bounding_box_is_not_actually_minimal_for_three_plus_clusters():
    """Direct unit-level check against :func:`bounding_box` itself."""
    from bhulan.analytics.geodesy import bounding_box

    lats = [_LAT] * len(_LONS)
    min_lat, min_lon, max_lat, max_lon = bounding_box(lats, _LONS)

    reported_span = _bbox_span_deg(min_lon, max_lon)
    assert reported_span <= _TRUE_MINIMAL_SPAN_DEG + 1e-6, (
        f"bounding_box() reports a longitude span of {reported_span:.1f} "
        f"degrees (min_lon={min_lon}, max_lon={max_lon}) for points at "
        f"{_LONS}, but the true minimal-span box (cutting at the largest "
        f"circular gap, between 60 and 170 degrees) spans only "
        f"{_TRUE_MINIMAL_SPAN_DEG:.0f} degrees -- bounding_box() only "
        f"checks two candidate cuts (raw framing at +/-180, shifted "
        f"framing at 0) instead of the true largest gap, so for 3+ "
        f"spread-out points it can report a box up to ~80 degrees "
        f"(~8,900 km) wider than the minimal box its own docstring "
        f"promises."
    )


def test_insights_bbox_endpoint_is_not_minimal_for_three_plus_clusters(client: TestClient):
    """Same defect, exercised end-to-end through ``POST /v1/insights``."""
    t = datetime(2024, 1, 1, tzinfo=timezone.utc)
    points = []
    for lon in _LONS:
        points.append({"lat": _LAT, "lon": lon, "ts_utc": t.isoformat()})
        t += timedelta(seconds=10)

    r = client.post("/v1/insights", json={"points": points})
    assert r.status_code == 200
    bbox = r.json()["summary"]["bbox"]
    assert bbox is not None

    reported_span = _bbox_span_deg(bbox["min_lon"], bbox["max_lon"])
    assert reported_span <= _TRUE_MINIMAL_SPAN_DEG + 1e-6, (
        f"/v1/insights summary.bbox reports min_lon={bbox['min_lon']}, "
        f"max_lon={bbox['max_lon']} -- a longitude span of "
        f"{reported_span:.1f} degrees -- for a track whose true minimal "
        f"bounding box spans only {_TRUE_MINIMAL_SPAN_DEG:.0f} degrees. "
        f"A client zooming a map to 'fit' this bbox zooms out ~80 degrees "
        f"further than necessary."
    )
