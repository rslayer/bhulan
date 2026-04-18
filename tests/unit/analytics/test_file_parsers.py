"""
Tests for the GPX / KML / FIT parsers.

Each test uses a tiny, hand-written sample of the format rather than a
real-world export — the goal is coverage of the parser's *shape handling*
(namespaces, coord tuples, rejection of malformed rows), not format
conformance. Real-world file tests would live under a ``sampledata/``
fixtures tree; skipped here to keep the test suite hermetic.
"""

from __future__ import annotations

import io

import pytest

from bhulan.analytics.file_parsers import (
    parse_file_bytes,
    parse_gpx_bytes,
    parse_kml_bytes,
)
from bhulan.analytics.parsers import ParseError

GPX_SAMPLE = b"""<?xml version="1.0" encoding="UTF-8"?>
<gpx version="1.1" creator="bhulan-tests" xmlns="http://www.topografix.com/GPX/1/1">
  <trk><name>t</name>
    <trkseg>
      <trkpt lat="12.97" lon="77.59"><time>2025-01-01T09:00:00Z</time></trkpt>
      <trkpt lat="12.98" lon="77.60"><time>2025-01-01T09:05:00Z</time></trkpt>
    </trkseg>
  </trk>
  <wpt lat="12.99" lon="77.61"><name>wp</name></wpt>
</gpx>
"""


def test_parse_gpx_extracts_track_and_waypoints():
    points = parse_gpx_bytes(GPX_SAMPLE)
    assert len(points) == 3
    assert points[0].lat == pytest.approx(12.97)
    assert points[0].lon == pytest.approx(77.59)
    assert points[0].ts_utc is not None
    # Waypoint comes last and has no timestamp.
    assert points[2].lat == pytest.approx(12.99)


def test_parse_gpx_rejects_invalid_payload():
    with pytest.raises(ParseError):
        parse_gpx_bytes(b"not a gpx file, just text")


KML_LINESTRING = b"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document><Placemark>
    <LineString><coordinates>
      77.59,12.97,0
      77.60,12.98,0
      77.61,12.99,0
    </coordinates></LineString>
  </Placemark></Document>
</kml>
"""


def test_parse_kml_linestring_extracts_lon_lat_order():
    points = parse_kml_bytes(KML_LINESTRING)
    assert len(points) == 3
    # KML is lon,lat,alt — parser must flip back to (lat, lon).
    assert points[0].lat == pytest.approx(12.97)
    assert points[0].lon == pytest.approx(77.59)
    assert points[2].lat == pytest.approx(12.99)


KML_GX_TRACK = b"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"
     xmlns:gx="http://www.google.com/kml/ext/2.2">
  <Document><Placemark>
    <gx:Track>
      <when>2025-01-01T09:00:00Z</when>
      <when>2025-01-01T09:05:00Z</when>
      <gx:coord>77.59 12.97 0</gx:coord>
      <gx:coord>77.60 12.98 0</gx:coord>
    </gx:Track>
  </Placemark></Document>
</kml>
"""


def test_parse_kml_gx_track_pairs_when_with_coord():
    points = parse_kml_bytes(KML_GX_TRACK)
    assert len(points) == 2
    assert points[0].ts_utc is not None
    assert points[1].ts_utc is not None
    # gx:coord uses space separator and same lon,lat,alt order.
    assert points[0].lat == pytest.approx(12.97)
    assert points[0].lon == pytest.approx(77.59)


def test_parse_kml_drops_out_of_range_coords():
    bad = b"""<?xml version="1.0"?>
<kml xmlns="http://www.opengis.net/kml/2.2"><Placemark>
  <LineString><coordinates>
    91.0,0.0,0
    77.59,12.97,0
  </coordinates></LineString>
</Placemark></kml>
"""
    points = parse_kml_bytes(bad)
    # The 91.0 lat (sent as lon in the tuple) is actually lon=91.0, lat=0.0.
    # Both are in-range for lat/lon but lon=91 is valid. Construct a worse
    # sample to exercise the reject path — lat > 90.
    really_bad = b"""<?xml version="1.0"?>
<kml xmlns="http://www.opengis.net/kml/2.2"><Placemark>
  <LineString><coordinates>
    0.0,91.0,0
    77.59,12.97,0
  </coordinates></LineString>
</Placemark></kml>
"""
    rb_points = parse_kml_bytes(really_bad)
    # One valid point survives; invalid one is silently dropped.
    assert len(rb_points) == 1
    assert rb_points[0].lat == pytest.approx(12.97)

    # Original "bad" sample just has two valid-range tuples.
    assert len(points) == 2


def test_parse_kml_rejects_non_xml():
    with pytest.raises(ParseError):
        parse_kml_bytes(b"not xml")


def test_parse_file_bytes_dispatches_on_extension():
    gpx_points = parse_file_bytes("track.gpx", GPX_SAMPLE)
    kml_points = parse_file_bytes("map.kml", KML_LINESTRING)
    csv_points = parse_file_bytes("pasted.csv", b"lat,lon\n12.97,77.59\n12.98,77.60\n")
    assert len(gpx_points) == 3
    assert len(kml_points) == 3
    assert len(csv_points) == 2


def test_parse_file_bytes_unknown_extension_falls_back_to_text():
    # "unknown.xyz" with CSV content — parser tries text parsers.
    points = parse_file_bytes("unknown.xyz", b"12.97,77.59\n12.98,77.60\n")
    assert len(points) == 2


def test_parse_fit_bytes_rejects_garbage():
    from bhulan.analytics.file_parsers import parse_fit_bytes

    # Random bytes that don't form a valid FIT header.
    with pytest.raises(ParseError):
        parse_fit_bytes(b"\x00" * 32)


def test_parse_fit_bytes_minimal_valid_header_with_record(monkeypatch):
    """End-to-end FIT parse using the fitdecode test fixture.

    Rather than hand-build a FIT binary (which is fragile) we monkeypatch
    fitdecode.FitReader to yield a fake record so we can exercise the
    happy path of the lat/lon conversion code without a dependency on
    a real FIT file.
    """
    import fitdecode

    from bhulan.analytics.file_parsers import parse_fit_bytes

    class FakeRecord(fitdecode.FitDataMessage):
        def __init__(self, lat_sc, lon_sc):
            self._lat_sc = lat_sc
            self._lon_sc = lon_sc

        @property
        def name(self):
            return "record"

        def get_value(self, key):
            return {
                "position_lat": self._lat_sc,
                "position_long": self._lon_sc,
                "timestamp": None,
                "speed": None,
            }[key]

    class FakeReader:
        def __init__(self, stream):
            self._stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def __iter__(self):
            # 77.59° = 77.59 / (180 / 2**31) = 925_420_834 semicircles
            lat_sc = int(12.97 / (180.0 / (2 ** 31)))
            lon_sc = int(77.59 / (180.0 / (2 ** 31)))
            yield FakeRecord(lat_sc, lon_sc)

    monkeypatch.setattr("bhulan.analytics.file_parsers.io", io)
    monkeypatch.setattr(fitdecode, "FitReader", FakeReader)

    pts = parse_fit_bytes(b"dummy")
    assert len(pts) == 1
    assert pts[0].lat == pytest.approx(12.97, rel=1e-6)
    assert pts[0].lon == pytest.approx(77.59, rel=1e-6)
