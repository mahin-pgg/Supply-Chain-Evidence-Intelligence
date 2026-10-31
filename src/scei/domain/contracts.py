from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


class EventType(StrEnum):
    RECEIVE = "RECEIVE"
    SHIP = "SHIP"
    STORE = "STORE"
    TRANSFORM = "TRANSFORM"
    SPLIT = "SPLIT"
    MERGE = "MERGE"
    CONSUME = "CONSUME"
    PRODUCE = "PRODUCE"
    INSPECT = "INSPECT"
    QUARANTINE = "QUARANTINE"


class RelationshipState(StrEnum):
    VERIFIED = "VERIFIED"
    PROBABLE = "PROBABLE"
    POSSIBLE = "POSSIBLE"
    CONTRADICTED = "CONTRADICTED"
    UNKNOWN = "UNKNOWN"


class ProvenanceRef(BaseModel):
    document_id: UUID
    page: int | None = Field(default=None, ge=1)
    locator: str | None = None
    extraction_method: str
    extractor_version: str
    confidence: float = Field(ge=0, le=1)


class Quantity(BaseModel):
    value: float = Field(ge=0)
    unit: str = Field(min_length=1, max_length=32)


class CanonicalEvent(BaseModel):
    organization_id: UUID
    event_id: UUID
    event_type: EventType
    event_time: datetime
    source_entity_id: UUID | None = None
    destination_entity_id: UUID | None = None
    material_id: UUID | None = None
    batch_id: UUID | None = None
    quantity: Quantity | None = None
    relationship_state: RelationshipState = RelationshipState.UNKNOWN
    provenance: list[ProvenanceRef] = Field(min_length=1)
    source_system: str = Field(min_length=1, max_length=128)
    idempotency_key: str = Field(min_length=1, max_length=512)
    schema_version: int = Field(default=1, ge=1)
