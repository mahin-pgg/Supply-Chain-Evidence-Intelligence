from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


class RegistryEntityType(StrEnum):
    BATCH = "batch"
    PRODUCT = "product"
    SUPPLIER = "supplier"
    FACILITY = "facility"
    MATERIAL = "material"


class EntityLifecycle(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    QUARANTINED = "quarantined"
    SUPERSEDED = "superseded"


class CanonicalEntity(BaseModel):
    entity_id: UUID
    organization_id: UUID
    entity_type: RegistryEntityType
    canonical_name: str = Field(min_length=1, max_length=512)
    identity_origin: str = Field(min_length=1, max_length=128)
    status: EntityLifecycle = EntityLifecycle.ACTIVE
    superseded_by_entity_id: UUID | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    attributes: dict[str, object] = Field(default_factory=dict)


class EntityIdentifier(BaseModel):
    identifier_id: UUID
    organization_id: UUID
    entity_id: UUID
    identifier_type: str = Field(min_length=1, max_length=128)
    namespace: str = Field(min_length=1, max_length=128)
    raw_value: str = Field(min_length=1, max_length=512)
    normalized_value: str = Field(min_length=1, max_length=512)
    is_primary: bool = False
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    status: EntityLifecycle = EntityLifecycle.ACTIVE


class EntityAlias(BaseModel):
    alias_id: UUID
    organization_id: UUID
    entity_id: UUID
    alias_type: str = Field(min_length=1, max_length=128)
    raw_value: str = Field(min_length=1, max_length=512)
    normalized_value: str = Field(min_length=1, max_length=512)
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    status: EntityLifecycle = EntityLifecycle.ACTIVE
    provenance_id: UUID


class EntityDatasetMembership(BaseModel):
    membership_id: UUID
    organization_id: UUID
    entity_id: UUID
    dataset_version_id: UUID
    source_entity_key: str = Field(min_length=1, max_length=512)
    source_table: str = Field(min_length=1, max_length=128)
    source_row_reference: str | None = None
    membership_status: str = "active"
    provenance_id: UUID


class RegistryProvenance(BaseModel):
    provenance_id: UUID
    organization_id: UUID
    entity_id: UUID
    dataset_version_id: UUID | None = None
    source_file: str
    source_row_reference: str | None = None
    locator: dict[str, object] = Field(default_factory=dict)
    extraction_method: str = "dataset_import"
    confidence: float = Field(default=1.0, ge=0, le=1)


class IdentityConflict(BaseModel):
    conflict_id: UUID
    organization_id: UUID
    identifier_type: str
    namespace: str
    normalized_value: str
    entity_ids: list[UUID]
    conflict_type: str
    status: str = "open"


class EntityCandidate(BaseModel):
    entity: CanonicalEntity
    matched_identifier: EntityIdentifier | None = None
    matched_alias: EntityAlias | None = None
    score: float = Field(ge=0, le=1)
