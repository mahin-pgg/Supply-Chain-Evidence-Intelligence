from __future__ import annotations

from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


class DatasetStatus(StrEnum):
    REGISTERED = "registered"
    STAGED = "staged"
    VALIDATED = "validated"
    ACTIVE = "active"
    RETIRED = "retired"
    FAILED = "failed"
    QUARANTINED = "quarantined"


class LoadStatus(StrEnum):
    STAGED = "staged"
    VALIDATED = "validated"
    ACTIVE = "active"
    FAILED = "failed"


class InvestigationContext(BaseModel):
    context_id: UUID
    organization_id: UUID
    workspace_id: UUID
    dataset_version_id: UUID
    status: DatasetStatus = DatasetStatus.STAGED


class ValidationReport(BaseModel):
    valid: bool
    counts: dict[str, int] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
