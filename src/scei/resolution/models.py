from __future__ import annotations

from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


class ResolutionState(StrEnum):
    VERIFIED = "VERIFIED"
    PROBABLE = "PROBABLE"
    POSSIBLE = "POSSIBLE"
    CONTRADICTED = "CONTRADICTED"
    UNKNOWN = "UNKNOWN"


class EntityProfile(BaseModel):
    entity_id: UUID
    organization_id: UUID
    entity_type: str = Field(min_length=1, max_length=64)
    canonical_name: str = Field(min_length=1, max_length=512)
    identifiers: list[str] = Field(default_factory=list)
    country_code: str | None = Field(default=None, max_length=2)
    district: str | None = Field(default=None, max_length=128)
    attributes: dict[str, str] = Field(default_factory=dict)


class ResolutionSignal(BaseModel):
    name: str
    score: float = Field(ge=0, le=1)
    explanation: str


class ResolutionCandidate(BaseModel):
    entity_id: UUID
    score: float = Field(ge=0, le=1)
    state: ResolutionState
    signals: list[ResolutionSignal] = Field(default_factory=list)


class ResolutionDecision(BaseModel):
    organization_id: UUID
    entity_type: str
    observed_value: str
    selected_entity_id: UUID | None
    state: ResolutionState
    score: float = Field(ge=0, le=1)
    candidates: list[ResolutionCandidate] = Field(default_factory=list)
    review_required: bool
    rationale: str
