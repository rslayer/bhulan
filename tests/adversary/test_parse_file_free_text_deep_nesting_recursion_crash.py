"""
Defect: a deeply-nested JSON array uploaded as free-text (any filename
that doesn't end in ``.gpx``/``.kml``/``.fit``) crashes ``POST
/v1/parse/file`` with an unhandled 500 instead of a clean 4xx. The same
root cause also crashes ``POST /v1/insights`` and ``POST
/v1/plot/validate`` via their ``text`` field, and ``POST /v1/compare``
via a track's ``text`` field.

Root cause: ``bhulan/analytics/file_parsers.py::parse_file_bytes``, for
any filename not ending in ``.gpx``/``.kml``/``.fit``, decodes the bytes
as UTF-8 text and hands them to
``bhulan.analytics.parsers.parse_any``. When the text starts with ``[``
or ``{``, ``parse_any`` -> ``parse_json`` calls stdlib ``json.loads``
*directly* on the raw text (``bhulan/analytics/parsers.py``, ``parse_json``
and ``parse_any``). Unlike the JSON body FastAPI/pydantic parses for
structured ``points`` payloads, there is no ``RequestValidationError``
(and therefore no custom exception handler) in this path at all --
``json.loads`` itself raises ``RecursionError`` once array nesting is
deep enough (empirically, ~10,000 levels with CPython's C JSON decoder;
well below any file-size or point-count limit the service enforces), and
that ``RecursionError`` is not a ``ParseError``, so none of the ``except
ParseError`` handlers in ``bhulan/api/routes/insights.py`` or
``bhulan/api/routes/compare.py`` catch it. It propagates all the way out
as an unhandled 500.

This is a *different* bug from the already-known "deeply nested
``points`` array" crash (see ``test_deeply_nested_json_recursion_crash.py``):
that one is pydantic/``jsonable_encoder`` recursing while rendering a 422
error body for a *structured* payload, and is masked by the app's custom
``RequestValidationError`` handler (which explicitly catches
``RecursionError`` when scrubbing the error detail). This bug instead
happens while *decoding* free-text JSON, entirely outside that
handler's reach, and reproduces on four different public endpoints.

Impact: any unauthenticated caller can 500 ``/v1/parse/file`` (via file
upload), ``/v1/insights``, ``/v1/plot/validate`` (via the ``text``
field), and ``/v1/compare`` (via a track's ``text`` field) with a
~20 KB payload that costs almost nothing to generate -- ``depth`` nested
single-element JSON arrays cost only ``O(depth)`` bytes to send, and no
special file type, size, or point count is involved.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

# Empirically, CPython's C json decoder tolerates depth=9900 but raises
# RecursionError by depth=10000 regardless of the process's Python-level
# sys.setrecursionlimit() -- comfortably under any request-size limit.
_NESTING_DEPTH = 10_000


def _nested_json_array(depth: int) -> str:
    return "[" * depth + "1" + "]" * depth


def test_parse_file_deeply_nested_text_upload_returns_500(client: TestClient):
    """A plain-text/JSON file upload (non-.gpx/.kml/.fit extension) with a
    deeply nested JSON array crashes the parse/file endpoint."""
    body = _nested_json_array(_NESTING_DEPTH).encode("utf-8")
    r = client.post(
        "/v1/parse/file",
        files={"file": ("track.txt", body, "text/plain")},
    )
    assert r.status_code in (400, 422), (
        f"expected a clean 4xx for a deeply nested JSON array uploaded as a "
        f"text file, got an unhandled {r.status_code}: {r.text[:200]}"
    )


def test_parse_file_deeply_nested_json_extension_upload_returns_500(client: TestClient):
    """Same crash via a ``.json``-named upload."""
    body = _nested_json_array(_NESTING_DEPTH).encode("utf-8")
    r = client.post(
        "/v1/parse/file",
        files={"file": ("track.json", body, "application/json")},
    )
    assert r.status_code in (400, 422), (
        f"expected a clean 4xx for a deeply nested JSON array uploaded as "
        f"'track.json', got an unhandled {r.status_code}: {r.text[:200]}"
    )


def test_insights_deeply_nested_text_field_returns_500(client: TestClient):
    """Same root cause, reached through ``/v1/insights``'s free-text
    ``text`` field instead of a file upload."""
    r = client.post(
        "/v1/insights",
        json={"text": _nested_json_array(_NESTING_DEPTH)},
    )
    assert r.status_code in (400, 422), (
        f"expected a clean 4xx for deeply nested JSON in the 'text' field, "
        f"got an unhandled {r.status_code}: {r.text[:200]}"
    )


def test_plot_validate_deeply_nested_text_field_returns_500(client: TestClient):
    r = client.post(
        "/v1/plot/validate",
        json={"text": _nested_json_array(_NESTING_DEPTH)},
    )
    assert r.status_code in (400, 422), (
        f"expected a clean 4xx for deeply nested JSON in the 'text' field, "
        f"got an unhandled {r.status_code}: {r.text[:200]}"
    )


def test_compare_deeply_nested_track_text_field_returns_500(client: TestClient):
    r = client.post(
        "/v1/compare",
        json={
            "tracks": [
                {"text": _nested_json_array(_NESTING_DEPTH)},
                {"points": [{"lat": 1.0, "lon": 1.0}]},
            ]
        },
    )
    assert r.status_code in (400, 422), (
        f"expected a clean 4xx for deeply nested JSON in a track's 'text' "
        f"field, got an unhandled {r.status_code}: {r.text[:200]}"
    )
