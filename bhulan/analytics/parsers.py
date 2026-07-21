"""
Lenient parsers for user-submitted GPS input.

The public website accepts coordinates pasted in a variety of shapes. This
module produces a uniform list of :class:`bhulan.analytics.insights.PointIn`
records from any of the supported forms:

* CSV with a header row (``lat,lon`` and optionally ``ts`` / ``timestamp``).
* CSV without a header row, columns in ``lat,lon[,ts[,speed]]`` order.
* JSON array of objects.
* Newline-delimited ``lat,lon`` pairs.
* GeoJSON ``FeatureCollection`` of ``Point`` / ``LineString`` features.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from bhulan.analytics.insights import PointIn


class ParseError(ValueError):
    """Raised when the input cannot be parsed as coordinates."""


# Maximum bracket/brace nesting depth accepted from untrusted text before we
# even hand it to ``json.loads``. A real GPS/track document (a GeoJSON
# ``FeatureCollection`` of ``LineString`` features, or a flat array of point
# dicts) nests only a handful of levels deep; 200 is orders of magnitude beyond
# anything legitimate yet comfortably below CPython's ~1000 recursion limit and
# the C json decoder's own ceiling. A tiny ``[[[…1…]]]`` payload (O(depth)
# bytes) would otherwise make ``json.loads`` raise an uncaught ``RecursionError``
# that surfaces as an HTTP 500. See spec/adrs/0010-depth-guarded-untrusted-json.md.
MAX_JSON_NESTING_DEPTH = 200


def _max_json_nesting_depth(text: str) -> int:
    """Return the deepest ``[``/``{`` nesting in ``text`` (ignoring strings).

    A single linear O(len) scan of the raw characters that tracks string
    state so brackets appearing *inside* JSON string literals are not
    counted. This runs before ``json.loads`` so a pathologically deep
    payload is rejected without ever driving the decoder's recursion.
    """
    depth = 0
    max_depth = 0
    in_string = False
    escaped = False
    for ch in text:
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "[" or ch == "{":
            depth += 1
            if depth > max_depth:
                max_depth = depth
        elif ch == "]" or ch == "}":
            if depth > 0:
                depth -= 1
    return max_depth


def json_nesting_exceeds(text: str, max_depth: int) -> bool:
    """Return ``True`` as soon as ``[``/``{`` nesting in ``text`` exceeds
    ``max_depth`` — a bounded, early-exiting variant of
    :func:`_max_json_nesting_depth`.

    It stops the instant the running depth passes ``max_depth`` (cheap: a
    deep-nesting attack payload is tiny, so this returns after ~``max_depth``
    characters) and never needs the true maximum. Callers that face a large but
    shallow legitimate body should pass only a bounded prefix of it (a shallow
    body never trips ``max_depth`` no matter how long, so scanning it in full is
    wasted work — and a body that pads shallow content past the prefix before
    nesting deep still fails safely downstream, where the JSON decoder's own
    ``RecursionError`` surfaces as a clean 4xx). Brackets inside JSON string
    literals are ignored.
    """
    depth = 0
    in_string = False
    escaped = False
    for ch in text:
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "[" or ch == "{":
            depth += 1
            if depth > max_depth:
                return True
        elif ch == "]" or ch == "}":
            if depth > 0:
                depth -= 1
    return False


def _loads_untrusted(text: str, max_depth: int = MAX_JSON_NESTING_DEPTH) -> Any:
    """``json.loads`` for untrusted text, guarded against deep-nesting crashes.

    Pre-scans the raw text's maximum bracket/brace nesting depth and rejects
    anything past ``max_depth`` with a :class:`ParseError` (a ``ValueError``
    the API layer maps to a clean 4xx) *before* calling ``json.loads`` — a
    deeply-nested payload would otherwise make the decoder raise an uncaught
    ``RecursionError`` (HTTP 500). ``RecursionError`` is also caught as a
    belt-and-braces fallback and converted to the same clean error.
    """
    if _max_json_nesting_depth(text) > max_depth:
        raise ParseError(
            f"Input JSON is nested too deeply (limit is {max_depth} levels)."
        )
    try:
        return json.loads(text)
    except RecursionError as exc:  # pragma: no cover - guarded by the pre-scan
        raise ParseError("Input JSON is nested too deeply to parse.") from exc


_LAT_KEYS = {"lat", "latitude", "y"}
_LON_KEYS = {"lon", "lng", "long", "longitude", "x"}
_TS_KEYS = {"ts", "ts_utc", "timestamp", "time", "datetime"}
_SPEED_KEYS = {"speed", "speed_mps", "speed_kmh", "speed_mph", "velocity"}


def _as_float(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_ts(val: Any) -> Optional[datetime]:
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, (int, float)):
        seconds = val / 1000.0 if val > 1e12 else float(val)
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(val, str):
        text = val.strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                from dateutil import parser as dateutil_parser

                dt = dateutil_parser.parse(text)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                return None
    return None


def _safe_point(
    lat: Optional[float],
    lon: Optional[float],
    ts: Optional[datetime] = None,
    speed_mps: Optional[float] = None,
) -> Optional[PointIn]:
    """Build a :class:`PointIn`, returning None instead of raising.

    Rows with out-of-range latitude/longitude (or any other pydantic
    validation failure) are silently skipped so the public parsers can
    surface the count via the quality channel rather than 500ing.
    """
    if lat is None or lon is None:
        return None
    if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
        return None
    try:
        return PointIn(lat=lat, lon=lon, ts_utc=ts, speed_mps=speed_mps)
    except Exception:
        return None


def _dict_to_point(row: Dict[str, Any]) -> Optional[PointIn]:
    lower = {str(k).strip().lower(): v for k, v in row.items()}
    lat = next((lower[k] for k in _LAT_KEYS if k in lower), None)
    lon = next((lower[k] for k in _LON_KEYS if k in lower), None)
    if lat is None or lon is None:
        return None
    lat_f = _as_float(lat)
    lon_f = _as_float(lon)
    if lat_f is None or lon_f is None:
        return None

    ts_raw = next((lower[k] for k in _TS_KEYS if k in lower), None)
    ts = _parse_ts(ts_raw)

    speed_val: Optional[float] = None
    for key in _SPEED_KEYS:
        if key in lower:
            raw = _as_float(lower[key])
            if raw is None:
                continue
            if key == "speed_kmh":
                speed_val = raw / 3.6
            elif key == "speed_mph":
                speed_val = raw * 0.44704
            else:
                speed_val = raw
            break

    return _safe_point(lat_f, lon_f, ts, speed_val)


def parse_csv_text(text: str) -> List[PointIn]:
    """Parse CSV content, with or without a header row."""
    sample = text[:2048]
    try:
        sniffer = csv.Sniffer()
        has_header = sniffer.has_header(sample)
    except csv.Error:
        has_header = any(c.isalpha() for c in sample.splitlines()[0] if c not in "-:TZ .")

    reader = csv.reader(io.StringIO(text))
    rows = [row for row in reader if row and any(cell.strip() for cell in row)]
    if not rows:
        return []

    if has_header:
        header = [h.strip().lower() for h in rows[0]]
        body = rows[1:]
        points: List[PointIn] = []
        for r in body:
            as_dict = {header[i]: r[i] for i in range(min(len(header), len(r)))}
            p = _dict_to_point(as_dict)
            if p is not None:
                points.append(p)
        return points

    points = []
    for r in rows:
        if len(r) < 2:
            continue
        lat = _as_float(r[0])
        lon = _as_float(r[1])
        if lat is None or lon is None:
            continue
        ts = _parse_ts(r[2]) if len(r) > 2 else None
        speed = _as_float(r[3]) if len(r) > 3 else None
        p = _safe_point(lat, lon, ts, speed)
        if p is not None:
            points.append(p)
    return points


def parse_plain_text(text: str) -> List[PointIn]:
    """Parse a file with one ``lat,lon`` (or ``lat lon``) per line."""
    points: List[PointIn] = []
    for line in text.splitlines():
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#"):
            continue
        parts = [p for p in cleaned.replace(";", ",").replace("\t", ",").split(",") if p]
        if len(parts) < 2:
            parts = cleaned.split()
        if len(parts) < 2:
            continue
        lat = _as_float(parts[0])
        lon = _as_float(parts[1])
        if lat is None or lon is None:
            continue
        ts = _parse_ts(parts[2]) if len(parts) > 2 else None
        p = _safe_point(lat, lon, ts)
        if p is not None:
            points.append(p)
    return points


def _geojson_position(pos: Any) -> Tuple[float, float]:
    """Convert a GeoJSON ``[lon, lat, ...]`` position to ``(lat, lon)``.

    GeoJSON positions are ``[lon, lat]`` (optionally with elevation). Anything
    that is not a sequence of at least two numeric values is malformed input;
    raise :class:`ParseError` so callers surface a clean 4xx rather than letting
    a bare ``IndexError``/``TypeError``/``ValueError`` bubble up as a 500.
    """
    if not isinstance(pos, (list, tuple)) or len(pos) < 2:
        raise ParseError(f"Malformed GeoJSON position (need [lon, lat]): {pos!r}")
    try:
        lon = float(pos[0])
        lat = float(pos[1])
    except (TypeError, ValueError) as e:
        raise ParseError(f"Non-numeric GeoJSON coordinates: {pos!r}") from e
    return (lat, lon)


def _geojson_positions(coords: Any, gtype: str) -> List[Any]:
    """Return the outer sequence of positions for a line/multi geometry."""
    if not isinstance(coords, (list, tuple)):
        raise ParseError(f"Malformed GeoJSON coordinates for {gtype}: {coords!r}")
    return list(coords)


def _coords_from_geojson_geom(
    geom: Dict[str, Any]
) -> List[Tuple[float, float]]:
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    if coords is None:
        return []
    if gtype == "Point":
        return [_geojson_position(coords)]
    if gtype == "MultiPoint" or gtype == "LineString":
        return [_geojson_position(c) for c in _geojson_positions(coords, gtype)]
    if gtype == "MultiLineString":
        out: List[Tuple[float, float]] = []
        for line in _geojson_positions(coords, gtype):
            out.extend(_geojson_position(c) for c in _geojson_positions(line, gtype))
        return out
    return []


def parse_geojson(data: Any) -> List[PointIn]:
    """Parse a GeoJSON object (``FeatureCollection``, ``Feature``, or geometry)."""
    if isinstance(data, str):
        data = _loads_untrusted(data)

    points: List[PointIn] = []

    def _from_feature(feat: Dict[str, Any]) -> None:
        geom = feat.get("geometry") or {}
        props = feat.get("properties") or {}
        ts = _parse_ts(
            next((props.get(k) for k in _TS_KEYS if k in props), None)
        )
        for lat, lon in _coords_from_geojson_geom(geom):
            p = _safe_point(lat, lon, ts)
            if p is not None:
                points.append(p)

    gtype = data.get("type") if isinstance(data, dict) else None
    if gtype == "FeatureCollection":
        for feat in data.get("features", []):
            _from_feature(feat)
    elif gtype == "Feature":
        _from_feature(data)
    elif gtype in {"Point", "MultiPoint", "LineString", "MultiLineString"}:
        for lat, lon in _coords_from_geojson_geom(data):
            p = _safe_point(lat, lon, None)
            if p is not None:
                points.append(p)
    else:
        raise ParseError(f"Unrecognized GeoJSON type: {gtype!r}")

    return points


def parse_json(data: Any) -> List[PointIn]:
    """Parse JSON that is either a GeoJSON object or an array of point dicts."""
    if isinstance(data, str):
        data = _loads_untrusted(data)

    if isinstance(data, dict) and data.get("type") in {
        "FeatureCollection",
        "Feature",
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
    }:
        return parse_geojson(data)

    if isinstance(data, list):
        out: List[PointIn] = []
        for item in data:
            if isinstance(item, dict):
                p = _dict_to_point(item)
                if p is not None:
                    out.append(p)
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                lat = _as_float(item[0])
                lon = _as_float(item[1])
                if lat is None or lon is None:
                    continue
                ts = _parse_ts(item[2]) if len(item) > 2 else None
                p = _safe_point(lat, lon, ts)
                if p is not None:
                    out.append(p)
        return out

    raise ParseError("Unrecognized JSON shape; expected array or GeoJSON object")


def parse_any(text: str) -> List[PointIn]:
    """
    Detect the input format and parse.

    Tries JSON first, then CSV, then plain text. Raises :class:`ParseError`
    only when nothing can be extracted; if zero rows parse successfully, the
    return is simply an empty list so the caller can surface that via the
    ``quality`` channel.
    """
    stripped = text.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            return parse_json(text)
        except json.JSONDecodeError:
            # Not actually JSON despite the leading brace/bracket — fall back
            # to CSV/plain-text detection below.
            pass
        # A ``ParseError`` here means the payload *is* valid JSON but is
        # structurally invalid as points/GeoJSON (bad coordinates, unknown
        # geometry). That is a hard input error: propagate it so the caller
        # returns a clean 4xx instead of silently trying to read JSON as CSV.

    try:
        rows = parse_csv_text(text)
        if rows:
            return rows
    except csv.Error:
        pass

    return parse_plain_text(text)


def estimate_input_rows(text: str) -> Optional[int]:
    """Estimate how many data rows the user intended to submit.

    Used to compute a ``rejected`` count for the ``/v1/plot/validate``
    endpoint without threading counters through every parser. Returns
    ``None`` when the format is not amenable to line counting (e.g. a
    GeoJSON ``FeatureCollection`` whose feature count isn't known until
    after parsing).
    """
    stripped = text.lstrip()
    if stripped.startswith("{"):
        return None
    if stripped.startswith("["):
        try:
            data = _loads_untrusted(text)
        except (json.JSONDecodeError, ValueError):
            return None
        if isinstance(data, list):
            return len(data)
        return None
    lines = [
        ln for ln in text.splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    if not lines:
        return 0
    first = lines[0]
    is_header = any(c.isalpha() for c in first if c not in "-:TZ .+eE")
    return max(len(lines) - (1 if is_header else 0), 0)
