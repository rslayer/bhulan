"""
Parsers for real-world GPS file formats uploaded by the web UI.

The in-sprint formats are:

* **GPX** — GPS Exchange Format (``.gpx``). XML, widely exported by phones,
  watches, and Strava/OSM. Uses ``gpxpy`` for robustness against the many
  dialects in the wild (track/route/waypoint, multiple segments, extensions).
* **KML** — Keyhole Markup Language (``.kml``). XML, used by Google Earth /
  My Maps. We parse with the stdlib rather than a heavyweight dep — the
  bulk of user exports are ``LineString/coordinates`` and ``gx:Track`` with
  optional ``when`` timestamps, both of which are easy to handle directly.
* **FIT** — Flexible and Interoperable Data Transfer (``.fit``). Binary,
  Garmin/Wahoo/Polar native. Uses ``fitdecode`` to iterate ``record`` frames
  and pull ``position_lat/long`` (semicircles) + ``timestamp`` + ``speed``.

Each parser returns a plain ``list[PointIn]`` so the caller can feed it
straight into :func:`bhulan.analytics.insights.compute_insights` or the
``/v1/plot/validate`` endpoint. Out-of-range or malformed points are
silently dropped via :func:`_safe_point` — the public surface reports
them through the same ``quality`` channel as the text parsers.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional
from xml.etree.ElementTree import Element, fromstring
from xml.etree.ElementTree import ParseError as XmlParseError

from bhulan.analytics.insights import PointIn
from bhulan.analytics.parsers import ParseError, _parse_ts, _safe_point

# FIT files encode lat/lon as "semicircles": int32 where 2**31 semicircles
# == 180 degrees. See Garmin's FIT SDK. Cache the conversion factor.
_SEMICIRCLES_TO_DEG = 180.0 / (2 ** 31)


def parse_gpx_bytes(data: bytes) -> List[PointIn]:
    """Parse a GPX file (bytes) into a list of points.

    Reads ``<trkpt>`` (track points), then ``<rtept>`` (route points), then
    ``<wpt>`` (waypoints) — in that order of preference, concatenated when
    a file has more than one source. Elevations are not preserved because
    the downstream analytics are 2D.
    """
    try:
        import gpxpy  # type: ignore
    except ImportError as exc:  # pragma: no cover - guarded by pyproject
        raise ParseError("gpxpy is required to parse GPX files") from exc

    try:
        gpx = gpxpy.parse(io.BytesIO(data).read().decode("utf-8"))
    except Exception as exc:
        raise ParseError(f"Invalid GPX: {exc}") from exc

    out: List[PointIn] = []

    for track in gpx.tracks:
        for seg in track.segments:
            for pt in seg.points:
                _append_gpx_point(out, pt.latitude, pt.longitude, pt.time, pt.speed)

    for route in gpx.routes:
        for rpt in route.points:
            # GPX 1.1 doesn't define speed on <rtept> either; gpxpy's
            # GPXRoutePoint mirrors GPXWaypoint and omits the attribute,
            # so go through getattr like the waypoint branch below.
            _append_gpx_point(
                out, rpt.latitude, rpt.longitude, rpt.time, getattr(rpt, "speed", None)
            )

    for wpt in gpx.waypoints:
        # Waypoints don't carry speed in the GPX 1.1 schema — gpxpy's
        # GPXWaypoint doesn't expose the attribute at all, unlike GPXTrackPoint.
        _append_gpx_point(
            out, wpt.latitude, wpt.longitude, wpt.time, getattr(wpt, "speed", None)
        )

    return out


def _append_gpx_point(
    out: List[PointIn],
    lat: Optional[float],
    lon: Optional[float],
    ts: Optional[datetime],
    speed: Optional[float],
) -> None:
    if lat is None or lon is None:
        return
    # gpxpy returns tz-aware datetimes already (UTC for ISO-8601 GPX).
    p = _safe_point(float(lat), float(lon), ts, speed)
    if p is not None:
        out.append(p)


# Namespace map for KML + gx extensions.
_KML_NS = {
    "kml": "http://www.opengis.net/kml/2.2",
    "kml21": "http://earth.google.com/kml/2.1",
    "gx": "http://www.google.com/kml/ext/2.2",
}


def parse_kml_bytes(data: bytes) -> List[PointIn]:
    """Parse a KML file (bytes) into a list of points.

    Handles three shapes commonly produced by Google My Maps / Earth / OSM:

    1. ``<LineString><coordinates>lon,lat[,alt] lon,lat ...</coordinates>``
    2. ``<Point><coordinates>lon,lat[,alt]</coordinates>`` (repeated)
    3. ``<gx:Track>`` with interleaved ``<when>`` and ``<gx:coord>`` tags
       (``gx:coord`` is ``lon lat alt`` — space-separated, not comma).

    Namespace handling: KML files are inconsistent. Some use the current
    2.2 namespace, some use 2.1, some use no default namespace at all.
    We check all three via :func:`_iter_elems`.
    """
    try:
        root = fromstring(data)
    except XmlParseError as exc:
        raise ParseError(f"Invalid KML/XML: {exc}") from exc

    points: List[PointIn] = []

    # 1) <LineString><coordinates> ... </coordinates></LineString>
    for elem in _iter_elems(root, "LineString"):
        coords_el = _find_child(elem, "coordinates")
        if coords_el is not None and coords_el.text:
            _extend_from_coord_string(points, coords_el.text, None)

    # 2) <Point><coordinates>lon,lat</coordinates></Point>, dated from the
    #    enclosing <Placemark>'s <TimeStamp><when> / <TimeSpan><begin>.
    #    Resolve each Point's Placemark through a single parent map built once,
    #    rather than rescanning the whole document per Point (the old O(n^2)
    #    path — see _build_parent_map). Timestamps are cached per Placemark so a
    #    Placemark holding many Points still costs one scan.
    point_elems = list(_iter_elems(root, "Point"))
    if point_elems:
        parent_map = _build_parent_map(root)
        pm_ts_cache: Dict[int, Optional[datetime]] = {}
        for elem in point_elems:
            coords_el = _find_child(elem, "coordinates")
            if coords_el is None or not coords_el.text:
                continue
            pm = _enclosing_placemark(elem, parent_map)
            if pm is None:
                ts: Optional[datetime] = None
            else:
                pm_key = id(pm)
                if pm_key not in pm_ts_cache:
                    pm_ts_cache[pm_key] = _placemark_timestamp(pm)
                ts = pm_ts_cache[pm_key]
            _extend_from_coord_string(points, coords_el.text, ts)

    # 3) <gx:Track>: interleaved <when>...</when> and <gx:coord>...</gx:coord>
    for track in _iter_elems(root, "Track", ns_prefixes=("gx",)):
        whens: List[Optional[datetime]] = []
        coords: List[str] = []
        for child in list(track):
            tag = _local(child.tag)
            if tag == "when":
                whens.append(_parse_ts(child.text) if child.text else None)
            elif tag == "coord":
                coords.append(child.text or "")
        for raw, when in zip(coords, whens + [None] * (len(coords) - len(whens))):
            parts = raw.split()
            if len(parts) < 2:
                continue
            try:
                lon, lat = float(parts[0]), float(parts[1])
            except ValueError:
                continue
            p = _safe_point(lat, lon, when)
            if p is not None:
                points.append(p)

    return points


def _extend_from_coord_string(
    out: List[PointIn], raw: str, ts: Optional[datetime]
) -> None:
    """Split a KML ``<coordinates>`` block and append every valid tuple.

    KML ``<coordinates>`` is whitespace-delimited (any mix of spaces,
    tabs, newlines), each tuple is ``lon,lat[,altitude]`` (comma-separated
    inside a tuple).
    """
    for token in raw.split():
        bits = token.split(",")
        if len(bits) < 2:
            continue
        try:
            lon = float(bits[0])
            lat = float(bits[1])
        except ValueError:
            continue
        p = _safe_point(lat, lon, ts)
        if p is not None:
            out.append(p)


def _local(tag: str) -> str:
    """Strip namespace off an ElementTree tag name."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _iter_elems(
    root: Element,
    local_name: str,
    ns_prefixes: Iterable[str] = ("kml", "kml21", ""),
) -> Iterable[Element]:
    """Iterate elements matching ``local_name`` under any supported namespace."""
    seen: set = set()
    for prefix in ns_prefixes:
        ns = _KML_NS.get(prefix, "")
        path = f".//{{{ns}}}{local_name}" if ns else f".//{local_name}"
        for e in root.findall(path):
            key = id(e)
            if key in seen:
                continue
            seen.add(key)
            yield e


def _find_child(parent: Element, local_name: str) -> Optional[Element]:
    for child in parent.iter():
        if _local(child.tag) == local_name:
            return child
    return None


def _build_parent_map(root: Element) -> Dict[int, Element]:
    """Map every element (by ``id``) to its parent, in a single pass.

    ``xml.etree.ElementTree`` elements carry no parent pointer, so resolving a
    ``<Point>``'s enclosing ``<Placemark>`` otherwise means rescanning the whole
    document per Point — the O(n^2) blow-up that let an ordinary "saved places"
    KML export (one dated ``<Placemark><Point>`` each) tie up a worker for
    minutes. Building this once makes each lookup O(depth), i.e. O(n) overall.
    Keys are ``id(elem)`` rather than the elements themselves so this never
    depends on Element hashing/equality semantics.
    """
    return {id(child): parent for parent in root.iter() for child in parent}


def _enclosing_placemark(elem: Element, parent_map: Dict[int, Element]) -> Optional[Element]:
    """Nearest ``<Placemark>`` ancestor of ``elem`` via ``parent_map``, if any."""
    cur = parent_map.get(id(elem))
    while cur is not None:
        if _local(cur.tag) == "Placemark":
            return cur
        cur = parent_map.get(id(cur))
    return None


def _placemark_timestamp(pm: Element) -> Optional[datetime]:
    """Timestamp for a ``<Placemark>``: first ``<when>``, else ``<begin>``.

    Same resolution order as the previous per-Point scan; the analytics handle
    undated points gracefully, so ``None`` is fine when neither is present.
    """
    for when in _iter_elems(pm, "when"):
        return _parse_ts(when.text) if when.text else None
    for begin in _iter_elems(pm, "begin"):
        return _parse_ts(begin.text) if begin.text else None
    return None


def parse_fit_bytes(data: bytes) -> List[PointIn]:
    """Parse a FIT file (bytes) into a list of points.

    Iterates ``record`` frames from the FIT stream, converting
    ``position_lat`` / ``position_long`` from Garmin semicircles to degrees.
    Speed is preserved when present (FIT reports m/s already). Frames
    without both lat and lon are skipped.
    """
    try:
        import fitdecode  # type: ignore
    except ImportError as exc:  # pragma: no cover - guarded by pyproject
        raise ParseError("fitdecode is required to parse FIT files") from exc

    out: List[PointIn] = []
    try:
        with fitdecode.FitReader(io.BytesIO(data)) as fit:
            for frame in fit:
                if not isinstance(frame, fitdecode.FitDataMessage):
                    continue
                if frame.name != "record":
                    continue
                lat_sc = _fit_field(frame, "position_lat")
                lon_sc = _fit_field(frame, "position_long")
                if lat_sc is None or lon_sc is None:
                    continue
                try:
                    lat = float(lat_sc) * _SEMICIRCLES_TO_DEG
                    lon = float(lon_sc) * _SEMICIRCLES_TO_DEG
                except (TypeError, ValueError):
                    continue
                ts_val = _fit_field(frame, "timestamp")
                ts: Optional[datetime] = None
                if isinstance(ts_val, datetime):
                    ts = ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=timezone.utc)
                speed = _fit_field(frame, "speed")
                speed_mps = float(speed) if isinstance(speed, (int, float)) else None
                p = _safe_point(lat, lon, ts, speed_mps)
                if p is not None:
                    out.append(p)
    except fitdecode.exceptions.FitError as exc:
        raise ParseError(f"Invalid FIT: {exc}") from exc
    except Exception as exc:
        # fitdecode surfaces a few non-FitError subclasses (struct.error on
        # truncated files); normalize everything to ParseError so callers
        # don't have to special-case.
        raise ParseError(f"Could not parse FIT file: {exc}") from exc

    return out


def _fit_field(frame, name: str):
    """Read a field from a fitdecode record, returning None if missing."""
    try:
        return frame.get_value(name)
    except KeyError:
        return None


def parse_file_bytes(
    filename: str, data: bytes
) -> List[PointIn]:
    """Route uploaded bytes to the right parser based on extension.

    The ``/v1/parse/file`` endpoint uses this as a single dispatch point so
    the HTTP handler doesn't have to care about file types.
    """
    name = (filename or "").lower()
    if name.endswith(".gpx"):
        return parse_gpx_bytes(data)
    if name.endswith(".kml"):
        return parse_kml_bytes(data)
    if name.endswith(".fit"):
        return parse_fit_bytes(data)
    # Fall through: maybe the caller uploaded CSV/JSON/GeoJSON as a file.
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ParseError(
            f"Unknown file type for '{filename}'. Supported: .gpx, .kml, .fit, "
            f"or UTF-8 CSV/JSON/plain text."
        ) from exc
    from bhulan.analytics.parsers import parse_any
    return parse_any(text)
