"""Business logic for user accounts, magic-link login, and history.

Design notes:

- **Opaque session tokens** — we store SHA-256 hashes of both magic-link
  tokens and session tokens. The client holds the plaintext; losing the DB
  doesn't leak live sessions (the hashes are useless without the plaintext).
- **Email == identity** — no usernames, no display names. The frontend
  prompts for an email, we email a magic link, and the email-verify flow
  creates-or-fetches the user row on first login.
- **All timestamps are unix seconds (int)**. The ``history.summary_json``
  column is an opaque JSON blob so we don't have to migrate when the
  :class:`InsightsReport` shape evolves.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Any, List, Optional

from bhulan.auth.db import connect

logger = logging.getLogger(__name__)

# Hard cap on the JSON we store per history row. /v1/insights bodies can
# legitimately reach low-MB on big tracks; a 512 KB cap keeps the DB under
# ~50 MB per 100 rows per user and avoids pathological rows.
MAX_HISTORY_JSON_BYTES = 512 * 1024

# Cap rows kept per user. Rolling window: insert drops the oldest when we
# exceed this count.
MAX_HISTORY_PER_USER = 200


def _now() -> int:
    return int(time.time())


def _hash_token(token: str) -> str:
    """SHA-256 of the token (hex). Deterministic \u2014 we store it, then
    re-derive it on each verify."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def generate_token() -> str:
    """32 bytes (256 bits) of urlsafe randomness \u2014 fits comfortably in
    a URL fragment or a header and is long enough to be unguessable."""
    return secrets.token_urlsafe(32)


@dataclass(frozen=True)
class User:
    id: int
    email: str
    created_at: int


@dataclass(frozen=True)
class HistoryRow:
    id: int
    user_id: int
    created_at: int
    kind: str
    label: Optional[str]
    summary: dict[str, Any]


# --- Magic link + session -----------------------------------------------


def create_magic_link(db_path: str, email: str, ttl_minutes: int) -> str:
    """Issue a new magic-link token for ``email``. Returns the plaintext
    token that should be emailed to the user."""
    token = generate_token()
    token_hash = _hash_token(token)
    now = _now()
    expires_at = now + int(ttl_minutes * 60)
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO magic_links(email, token_hash, created_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (email.strip().lower(), token_hash, now, expires_at),
        )
    return token


def verify_magic_link(
    db_path: str, token: str, session_ttl_days: int
) -> tuple[User, str]:
    """Consume a magic-link token and return ``(user, session_token)``.

    Raises :class:`ValueError` if the token is unknown, expired, or already
    consumed.
    """
    token_hash = _hash_token(token)
    now = _now()
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT id, email, expires_at, consumed_at FROM magic_links "
            "WHERE token_hash = ?",
            (token_hash,),
        ).fetchone()
        if row is None:
            raise ValueError("Invalid magic link")
        if row["consumed_at"] is not None:
            raise ValueError("Magic link already used")
        if row["expires_at"] < now:
            raise ValueError("Magic link expired")

        # Consume the magic link first so a double-submit can't race.
        conn.execute(
            "UPDATE magic_links SET consumed_at = ? WHERE id = ?",
            (now, row["id"]),
        )

        # Upsert the user.
        email = row["email"]
        user_row = conn.execute(
            "SELECT id, email, created_at FROM users WHERE email = ?",
            (email,),
        ).fetchone()
        if user_row is None:
            cur = conn.execute(
                "INSERT INTO users(email, created_at) VALUES (?, ?)",
                (email, now),
            )
            user = User(id=int(cur.lastrowid or 0), email=email, created_at=now)
        else:
            user = User(
                id=user_row["id"],
                email=user_row["email"],
                created_at=user_row["created_at"],
            )

        # Create a session.
        session_token = generate_token()
        session_expires = now + int(session_ttl_days * 86400)
        conn.execute(
            "INSERT INTO sessions(user_id, token_hash, created_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (user.id, _hash_token(session_token), now, session_expires),
        )

    return user, session_token


def user_for_session(db_path: str, session_token: str) -> Optional[User]:
    """Look up the user for a session token. Returns ``None`` if the
    token is unknown, expired, or revoked."""
    if not session_token:
        return None
    token_hash = _hash_token(session_token)
    now = _now()
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT u.id AS uid, u.email, u.created_at, "
            "       s.expires_at, s.revoked_at "
            "FROM sessions s JOIN users u ON u.id = s.user_id "
            "WHERE s.token_hash = ?",
            (token_hash,),
        ).fetchone()
    if row is None:
        return None
    if row["revoked_at"] is not None:
        return None
    if row["expires_at"] < now:
        return None
    return User(id=row["uid"], email=row["email"], created_at=row["created_at"])


def revoke_session(db_path: str, session_token: str) -> None:
    """Mark the session as revoked. Idempotent \u2014 unknown tokens are a no-op."""
    token_hash = _hash_token(session_token)
    with connect(db_path) as conn:
        conn.execute(
            "UPDATE sessions SET revoked_at = ? WHERE token_hash = ? "
            "AND revoked_at IS NULL",
            (_now(), token_hash),
        )


# --- History -----------------------------------------------------------


def _dump_json_capped(data: dict[str, Any]) -> str:
    s = json.dumps(data, separators=(",", ":"), default=str)
    if len(s.encode("utf-8")) > MAX_HISTORY_JSON_BYTES:
        raise ValueError(
            f"History payload exceeds {MAX_HISTORY_JSON_BYTES} bytes"
        )
    return s


def save_history(
    db_path: str,
    user_id: int,
    kind: str,
    label: Optional[str],
    request_payload: dict[str, Any],
    summary_payload: dict[str, Any],
) -> int:
    """Persist a single history row and prune old rows past the cap."""
    req_json = _dump_json_capped(request_payload)
    summary_json = _dump_json_capped(summary_payload)
    now = _now()
    with connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO history(user_id, created_at, kind, label, "
            "request_json, summary_json) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, now, kind, label, req_json, summary_json),
        )
        new_id = int(cur.lastrowid or 0)
        # Prune. Keep the N newest rows per user.
        conn.execute(
            "DELETE FROM history WHERE user_id = ? AND id NOT IN ("
            "SELECT id FROM history WHERE user_id = ? "
            "ORDER BY created_at DESC, id DESC LIMIT ?)",
            (user_id, user_id, MAX_HISTORY_PER_USER),
        )
    return new_id


def list_history(db_path: str, user_id: int, limit: int = 100) -> List[HistoryRow]:
    """Return the user's most recent history rows (summary only, not the
    full request body)."""
    limit = max(1, min(limit, MAX_HISTORY_PER_USER))
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, user_id, created_at, kind, label, summary_json "
            "FROM history WHERE user_id = ? "
            "ORDER BY created_at DESC, id DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
    out: List[HistoryRow] = []
    for r in rows:
        try:
            summary = json.loads(r["summary_json"])
        except json.JSONDecodeError:
            summary = {}
        out.append(
            HistoryRow(
                id=r["id"],
                user_id=r["user_id"],
                created_at=r["created_at"],
                kind=r["kind"],
                label=r["label"],
                summary=summary,
            )
        )
    return out


def get_history_entry(
    db_path: str, user_id: int, entry_id: int
) -> Optional[dict[str, Any]]:
    """Return the full row (including the request body) for replay."""
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT id, user_id, created_at, kind, label, request_json, "
            "summary_json FROM history WHERE id = ? AND user_id = ?",
            (entry_id, user_id),
        ).fetchone()
    if row is None:
        return None
    try:
        request = json.loads(row["request_json"])
    except json.JSONDecodeError:
        request = None
    try:
        summary = json.loads(row["summary_json"])
    except json.JSONDecodeError:
        summary = {}
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "kind": row["kind"],
        "label": row["label"],
        "request": request,
        "summary": summary,
    }


def delete_history(db_path: str, user_id: int, entry_id: int) -> bool:
    """Delete a single history row. Returns True if a row was deleted."""
    with connect(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM history WHERE id = ? AND user_id = ?",
            (entry_id, user_id),
        )
        return cur.rowcount > 0


def history_row_to_dict(row: HistoryRow) -> dict[str, Any]:
    return dataclasses.asdict(row)
