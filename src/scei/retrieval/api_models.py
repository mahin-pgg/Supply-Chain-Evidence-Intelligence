from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel


class EvidenceSearchResponse(BaseModel):
    organization_id: UUID
    query: str
    total: int
    results: list[dict[str, object]]


class EventSearchResponse(BaseModel):
    organization_id: UUID
    query: str
    total: int
    results: list[dict[str, object]]


def bounded_limit(limit: int) -> int:
    return max(1, min(limit, 100))
