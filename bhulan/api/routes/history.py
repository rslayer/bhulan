"""Per-user request history endpoints.

All endpoints require a valid session (see :func:`~bhulan.api.deps.current_user_required`).
History rows are created as a side effect of calls to ``/v1/insights``
(see :mod:`bhulan.api.routes.insights`) and listed / fetched / deleted
through the routes in this module.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from bhulan.api.deps import current_user_required
from bhulan.auth import db as auth_db
from bhulan.auth import service as auth_service
from bhulan.auth.service import User
from bhulan.config.settings import settings


def _require_auth_enabled() -> None:
    if not settings.BHULAN_AUTH_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication is not enabled on this server",
        )


router = APIRouter(prefix="/v1/history", tags=["history"])


class HistorySummary(BaseModel):
    id: int
    created_at: int
    kind: str
    label: Optional[str] = None
    summary: Dict[str, Any] = Field(default_factory=dict)


class HistoryListResponse(BaseModel):
    entries: List[HistorySummary]


@router.get("", response_model=HistoryListResponse)
async def list_history_endpoint(
    user: User = Depends(current_user_required),
    limit: int = Query(50, ge=1, le=200),
) -> HistoryListResponse:
    _require_auth_enabled()
    auth_db.init_db(settings.BHULAN_DB_PATH)
    rows = auth_service.list_history(settings.BHULAN_DB_PATH, user.id, limit=limit)
    return HistoryListResponse(
        entries=[
            HistorySummary(
                id=r.id,
                created_at=r.created_at,
                kind=r.kind,
                label=r.label,
                summary=r.summary,
            )
            for r in rows
        ]
    )


class HistoryDetail(BaseModel):
    id: int
    created_at: int
    kind: str
    label: Optional[str] = None
    request: Optional[Dict[str, Any]] = None
    summary: Dict[str, Any] = Field(default_factory=dict)


@router.get("/{entry_id}", response_model=HistoryDetail)
async def get_history_entry(
    entry_id: int,
    user: User = Depends(current_user_required),
) -> HistoryDetail:
    _require_auth_enabled()
    auth_db.init_db(settings.BHULAN_DB_PATH)
    entry = auth_service.get_history_entry(
        settings.BHULAN_DB_PATH, user.id, entry_id
    )
    if entry is None:
        raise HTTPException(status_code=404, detail="History entry not found")
    return HistoryDetail(**entry)


class HistoryDeleteResponse(BaseModel):
    ok: bool
    id: int


@router.delete("/{entry_id}", response_model=HistoryDeleteResponse)
async def delete_history_entry(
    entry_id: int,
    user: User = Depends(current_user_required),
) -> HistoryDeleteResponse:
    _require_auth_enabled()
    auth_db.init_db(settings.BHULAN_DB_PATH)
    ok = auth_service.delete_history(settings.BHULAN_DB_PATH, user.id, entry_id)
    if not ok:
        raise HTTPException(status_code=404, detail="History entry not found")
    return HistoryDeleteResponse(ok=True, id=entry_id)
