"""Unit tests for :mod:`bhulan.auth.service` against a temp SQLite file.

Each test gets its own DB file via pytest's tmp_path so there's no shared
state leaking across tests. We exercise the full token lifecycle (issue →
verify → session → history) because the single-test surface is tiny and
the table relationships are the whole point.
"""

from __future__ import annotations

import time

import pytest

from bhulan.auth import db as auth_db
from bhulan.auth import service as auth_service


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "bhulan_test.db")
    auth_db.reset_db_for_tests(path)
    auth_db.init_db(path)
    yield path
    auth_db.reset_db_for_tests(path)


def _sample_request() -> dict:
    return {
        "points": [
            {"lat": 12.97, "lon": 77.59, "ts_utc": "2024-01-01T00:00:00Z"}
        ],
        "options": {"stop_radius_m": 40},
    }


def _sample_summary() -> dict:
    return {
        "summary": {
            "point_count": 1,
            "accepted_point_count": 1,
            "total_distance_km": 0.0,
        },
        "quality": {"rejected_points": 0, "issues": []},
        "stop_count": 0,
        "trip_count": 0,
        "hotspot_count": 0,
    }


def test_magic_link_roundtrip_creates_user_and_session(db_path: str) -> None:
    token = auth_service.create_magic_link(db_path, "a@b.com", ttl_minutes=15)
    assert token and len(token) > 20

    user, session_token = auth_service.verify_magic_link(
        db_path, token, session_ttl_days=30
    )
    assert user.email == "a@b.com"
    assert user.id > 0
    assert session_token and session_token != token

    # Session resolves back to the same user.
    u2 = auth_service.user_for_session(db_path, session_token)
    assert u2 is not None and u2.id == user.id


def test_magic_link_single_use(db_path: str) -> None:
    token = auth_service.create_magic_link(db_path, "a@b.com", ttl_minutes=15)
    auth_service.verify_magic_link(db_path, token, session_ttl_days=30)
    with pytest.raises(ValueError, match="already used"):
        auth_service.verify_magic_link(db_path, token, session_ttl_days=30)


def test_magic_link_unknown_token(db_path: str) -> None:
    with pytest.raises(ValueError, match="Invalid magic link"):
        auth_service.verify_magic_link(db_path, "not-a-token", session_ttl_days=30)


def test_magic_link_expired(db_path: str, monkeypatch) -> None:
    # Issue a token, then fast-forward the clock past its TTL.
    real_now = time.time()
    token = auth_service.create_magic_link(db_path, "a@b.com", ttl_minutes=1)
    monkeypatch.setattr(
        auth_service, "_now", lambda: int(real_now + 2 * 60)
    )
    with pytest.raises(ValueError, match="expired"):
        auth_service.verify_magic_link(db_path, token, session_ttl_days=30)


def test_verify_returns_existing_user_on_second_login(db_path: str) -> None:
    t1 = auth_service.create_magic_link(db_path, "a@b.com", ttl_minutes=15)
    u1, _ = auth_service.verify_magic_link(db_path, t1, session_ttl_days=30)
    t2 = auth_service.create_magic_link(db_path, "a@b.com", ttl_minutes=15)
    u2, _ = auth_service.verify_magic_link(db_path, t2, session_ttl_days=30)
    assert u1.id == u2.id


def test_revoke_session_returns_none(db_path: str) -> None:
    token = auth_service.create_magic_link(db_path, "a@b.com", ttl_minutes=15)
    _, session = auth_service.verify_magic_link(db_path, token, session_ttl_days=30)
    auth_service.revoke_session(db_path, session)
    assert auth_service.user_for_session(db_path, session) is None


def test_user_for_session_returns_none_for_unknown_token(db_path: str) -> None:
    assert auth_service.user_for_session(db_path, "nope") is None


def test_save_and_list_history(db_path: str) -> None:
    t = auth_service.create_magic_link(db_path, "a@b.com", ttl_minutes=15)
    user, _ = auth_service.verify_magic_link(db_path, t, session_ttl_days=30)

    # Insert two entries; list returns them newest-first.
    h1 = auth_service.save_history(
        db_path, user.id, "insights", "first", _sample_request(), _sample_summary()
    )
    h2 = auth_service.save_history(
        db_path, user.id, "insights", "second", _sample_request(), _sample_summary()
    )
    rows = auth_service.list_history(db_path, user.id)
    assert [r.id for r in rows] == [h2, h1]
    assert rows[0].label == "second"
    assert rows[0].summary["stop_count"] == 0


def test_history_is_isolated_per_user(db_path: str) -> None:
    t1 = auth_service.create_magic_link(db_path, "a@b.com", ttl_minutes=15)
    u1, _ = auth_service.verify_magic_link(db_path, t1, session_ttl_days=30)
    t2 = auth_service.create_magic_link(db_path, "c@d.com", ttl_minutes=15)
    u2, _ = auth_service.verify_magic_link(db_path, t2, session_ttl_days=30)

    auth_service.save_history(
        db_path, u1.id, "insights", None, _sample_request(), _sample_summary()
    )
    assert auth_service.list_history(db_path, u1.id)
    assert auth_service.list_history(db_path, u2.id) == []


def test_get_and_delete_history_entry(db_path: str) -> None:
    t = auth_service.create_magic_link(db_path, "a@b.com", ttl_minutes=15)
    user, _ = auth_service.verify_magic_link(db_path, t, session_ttl_days=30)
    hid = auth_service.save_history(
        db_path, user.id, "insights", "r", _sample_request(), _sample_summary()
    )

    detail = auth_service.get_history_entry(db_path, user.id, hid)
    assert detail is not None
    assert detail["request"]["options"]["stop_radius_m"] == 40
    assert detail["summary"]["stop_count"] == 0

    # Another user's id can't fetch this entry.
    assert auth_service.get_history_entry(db_path, user.id + 999, hid) is None

    assert auth_service.delete_history(db_path, user.id, hid) is True
    assert auth_service.delete_history(db_path, user.id, hid) is False
    assert auth_service.get_history_entry(db_path, user.id, hid) is None


def test_history_payload_cap_rejects_oversized_rows(db_path: str) -> None:
    t = auth_service.create_magic_link(db_path, "a@b.com", ttl_minutes=15)
    user, _ = auth_service.verify_magic_link(db_path, t, session_ttl_days=30)
    big = {"blob": "x" * (auth_service.MAX_HISTORY_JSON_BYTES + 1)}
    with pytest.raises(ValueError):
        auth_service.save_history(
            db_path, user.id, "insights", "big", big, _sample_summary()
        )


def test_history_pruning_keeps_only_latest_rows(db_path: str, monkeypatch) -> None:
    t = auth_service.create_magic_link(db_path, "a@b.com", ttl_minutes=15)
    user, _ = auth_service.verify_magic_link(db_path, t, session_ttl_days=30)

    monkeypatch.setattr(auth_service, "MAX_HISTORY_PER_USER", 3)
    ids = [
        auth_service.save_history(
            db_path,
            user.id,
            "insights",
            f"r{i}",
            _sample_request(),
            _sample_summary(),
        )
        for i in range(5)
    ]
    rows = auth_service.list_history(db_path, user.id)
    assert len(rows) == 3
    # The three newest survived; the two oldest were pruned.
    assert {r.id for r in rows} == set(ids[-3:])
