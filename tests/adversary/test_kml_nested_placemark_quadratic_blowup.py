"""
Defect: cycle 3's fix for the KML dated-Point O(n^2) blowup
(``bhulan/analytics/file_parsers.py::_build_point_timestamps``) still
degrades to O(n^2) when ``<Placemark>`` elements are *nested* inside one
another instead of laid out as flat siblings.

Root cause: ``_build_point_timestamps`` (lines 230-250) iterates every
``<Placemark>`` once (good), but for *each* Placemark it calls
``_iter_elems(pm, "Point")`` (line 248), which does
``pm.findall(".//Point")`` -- a full walk of **that Placemark's entire
subtree**. For flat, sibling Placemarks this subtree is O(1) sized, so the
whole function is O(n) as intended (and as the acceptance test in
``test_kml_point_timestamp_quadratic_blowup.py`` confirms). But nothing
requires Placemarks to be flat: if Placemark_0 contains Placemark_1 which
contains Placemark_2 ... which contains Placemark_{n-1} (a "Russian doll"
of nested Placemarks, one Point each -- well-formed XML, and not even an
unusual shape for KML exported/re-saved by some third-party tools that
preserve a Folder-like grouping via nesting instead of siblings), then for
outer Placemark_k, ``pm.findall(".//Point")`` walks all ``n - k`` Points
nested inside it. Summed over all k this is
``n + (n-1) + ... + 1 = O(n^2)`` -- the exact same quadratic-blowup shape
the cycle-3 fix was supposed to eliminate, just moved one level down.

(This also means the ADR's "First Placemark wins" rule silently degrades:
for this nested shape, every Point ends up stamped with the *outermost*
Placemark's timestamp, not its own immediate parent's -- because the
outermost Placemark's subtree walk visits every Point first and
``setdefault`` locks in that timestamp. That matches the *old*
``_nearest_timestamp`` behavior bug-for-bug, so it is not a new
correctness regression, but it means the "outer Placemark's timestamp
leaks onto all its descendants' Points" behavior is baked into both the
old and new implementation for nested input -- worth knowing when judging
whether nested Placemarks are in-scope for a future fix.)

Measured through the public endpoint (in-process, no network), a KML file
of ``n`` Russian-doll-nested ``<Placemark><TimeStamp>+<Point></Placemark>``
elements, same point count as an equivalent *flat* file:

  n (points) | flat-shape wall time | nested-shape wall time
  -----------|----------------------|------------------------
  1,000      | 0.023s               | 0.34s   (~15x slower)
  2,000      | 0.035s               | ~1.3s (extrapolated, quadratic)

The flat shape (what the cycle-3 acceptance test covers) is comfortably
linear; the nested shape -- identical point/timestamp count, just a
different (still well-formed) document structure -- reproduces the same
quadratic wall-clock growth the fix was meant to remove.

Impact: an unauthenticated caller can still trigger the DoS the cycle-3
fix targeted, just by nesting Placemarks instead of listing them as
siblings -- a small structural tweak to the same "n dated waypoints" KML
export.
"""

from __future__ import annotations

import time

from fastapi.testclient import TestClient

# 1,000 points is a small, realistic export size; a linear parser handles
# it in ~25ms (see the flat-shape acceptance test / measurements above).
_POINT_COUNT = 1_000
_MAX_ACCEPTABLE_SECONDS = 0.15


def _kml_with_nested_placemarks(n: int) -> bytes:
    """n Placemarks nested inside one another (Placemark_0 > Placemark_1 >
    ... > Placemark_{n-1}), each with its own TimeStamp + Point -- still
    well-formed KML, just not flat siblings."""
    opens = []
    closes = []
    for i in range(n):
        opens.append(
            "<Placemark>"
            f"<name>p{i}</name>"
            f"<TimeStamp><when>2024-01-01T00:00:{i % 60:02d}Z</when></TimeStamp>"
            f"<Point><coordinates>1.{i % 1000},1.{i % 1000},0</coordinates></Point>"
        )
        closes.append("</Placemark>")
    body = "".join(opens) + "".join(reversed(closes))
    return (
        '<?xml version="1.0"?>\n'
        '<kml xmlns="http://www.opengis.net/kml/2.2">\n'
        "<Document>\n"
        f"{body}\n"
        "</Document>\n"
        "</kml>\n"
    ).encode("utf-8")


def test_kml_nested_placemarks_parse_in_reasonable_time(client: TestClient):
    data = _kml_with_nested_placemarks(_POINT_COUNT)
    start = time.monotonic()
    r = client.post(
        "/v1/parse/file",
        files={"file": ("nested_places.kml", data, "application/vnd.google-earth.kml+xml")},
    )
    elapsed = time.monotonic() - start

    assert r.status_code == 200
    assert r.json()["accepted"] == _POINT_COUNT
    assert elapsed < _MAX_ACCEPTABLE_SECONDS, (
        f"a {len(data)}-byte KML file with {_POINT_COUNT} Russian-doll-"
        f"nested <Placemark><TimeStamp>+<Point> elements took {elapsed:.2f}s "
        f"(expected < {_MAX_ACCEPTABLE_SECONDS}s, matching an equivalent "
        f"flat-shape file which parses in well under 50ms) -- "
        f"_build_point_timestamps's per-Placemark `_iter_elems(pm, \"Point\")` "
        f"walks that Placemark's entire subtree, which is O(n) sized when "
        f"Placemarks are nested instead of flat siblings, making the "
        f"cycle-3 'single pass' fix quadratic again for this well-formed "
        f"KML shape"
    )
