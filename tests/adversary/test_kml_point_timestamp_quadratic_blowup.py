"""
Defect: parsing a KML file whose points are encoded as
``<Placemark><TimeStamp>...</TimeStamp><Point>...</Point></Placemark>``
(the standard, widest-compatibility way Google My Maps / Earth and many
GPS-export tools represent a *dated* point, as opposed to a single
``<gx:Track>``) is O(n^2) in the number of ``<Point>`` elements, not O(n).

Root cause: ``bhulan/analytics/file_parsers.py::parse_kml_bytes`` (lines
134-142), for every ``<Point>`` element found anywhere in the document,
calls ``_nearest_timestamp(root, elem)`` to look up its enclosing
Placemark's timestamp. Because the code parses with stdlib
``xml.etree.ElementTree`` (which has no parent pointers), the
``parent = elem.getparent() if hasattr(elem, "getparent") else None``
check (line 140) is *always* ``None`` for stdlib elements (only ``lxml``
elements expose ``getparent``) -- so every single ``<Point>`` triggers a
*full walk from the document root*: ``_nearest_timestamp`` (lines
224-244) iterates every ``<Placemark>`` in the whole document via
``_iter_elems(root, "Placemark")``, and for each one calls ``_contains``
(lines 247-251), which itself does a full ``ancestor.iter()`` walk of
that Placemark's subtree checking identity against the target. For a
document with ``n`` Placemarks (one Point each), this is
``O(n)`` Placemarks-to-check times ``O(1)`` average subtree size times
``n`` Points needing a lookup = ``O(n^2)`` overall, even though nothing
about the actual KML shape is unusual -- "n dated waypoints, one
Placemark each" is exactly how Google My Maps exports a saved-places
list or a "starred locations" KML, not a contrived shape.

Measured through the public endpoint (in-process, no network), a KML
file of tightly-packed ``<Placemark><TimeStamp>+<Point>`` blocks:

  n (points) | file size | wall time
  -----------|-----------|----------
  500        | 70 KB     | 0.12s
  1,000      | 142 KB    | 0.39s
  1,500      | 213 KB    | 0.87s
  2,000      | 284 KB    | 1.51s

Time roughly quadruples with each doubling of ``n`` (the signature of
O(n^2)), and the growth is in *file size* terms shockingly cheap for an
attacker: extrapolating the measured quadratic coefficient, a single KML
file well under 5 MB (a small fraction of the service's own
``MAX_UPLOAD_BYTES`` = 25 MB) would tie up a worker process for tens of
minutes. Unlike the already-fixed stop-detection blowup, this endpoint
(``POST /v1/parse/file``) requires no authentication and has no
points-count cap applied *before* parsing (``_enforce_point_cap`` only
runs *after* ``parse_file_bytes`` returns) -- so the full O(n^2) cost is
paid before any size guard has a chance to reject the request.

Impact: an unauthenticated caller can submit an ordinary-looking,
sub-megabyte KML export (the kind produced by "export my saved places"
in Google My Maps) and tie up a worker for a very long time, a
straightforward denial-of-service vector against a stateless,
unauthenticated file-upload endpoint.
"""

from __future__ import annotations

import time

from fastapi.testclient import TestClient

# 1,500 points / ~213 KB is a small, realistic "saved places" export size,
# yet already reproduces multi-hundred-millisecond quadratic blowup. A
# linear parser would comfortably finish this in well under 100ms.
_POINT_COUNT = 1_500
_MAX_ACCEPTABLE_SECONDS = 0.3


def _kml_with_dated_points(n: int) -> bytes:
    placemarks = []
    for i in range(n):
        placemarks.append(
            "<Placemark>"
            f"<TimeStamp><when>2024-01-01T00:00:{i % 60:02d}Z</when></TimeStamp>"
            f"<Point><coordinates>1.{i % 1000},1.{i % 1000},0</coordinates></Point>"
            "</Placemark>"
        )
    body = "\n".join(placemarks)
    return (
        '<?xml version="1.0"?>\n'
        '<kml xmlns="http://www.opengis.net/kml/2.2">\n'
        "<Document>\n"
        f"{body}\n"
        "</Document>\n"
        "</kml>\n"
    ).encode("utf-8")


def test_kml_dated_points_parse_in_reasonable_time(client: TestClient):
    data = _kml_with_dated_points(_POINT_COUNT)
    start = time.monotonic()
    r = client.post(
        "/v1/parse/file",
        files={"file": ("saved_places.kml", data, "application/vnd.google-earth.kml+xml")},
    )
    elapsed = time.monotonic() - start

    assert r.status_code == 200
    assert r.json()["accepted"] == _POINT_COUNT
    assert elapsed < _MAX_ACCEPTABLE_SECONDS, (
        f"a {len(data)}-byte KML file with {_POINT_COUNT} dated "
        f"<Placemark><TimeStamp>+<Point> entries took {elapsed:.2f}s "
        f"(expected < {_MAX_ACCEPTABLE_SECONDS}s) -- "
        f"parse_kml_bytes's per-Point _nearest_timestamp lookup rescans "
        f"the whole document from the root every time, making KML parsing "
        f"O(n^2) in the number of dated points; this is a DoS vector on an "
        f"unauthenticated file-upload endpoint that requires no auth and no "
        f"unusual input shape"
    )
