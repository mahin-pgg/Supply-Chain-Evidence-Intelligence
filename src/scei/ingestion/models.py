from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class SourceDocumentIngest(BaseModel):
    organization_id: UUID
    document_id: UUID
    document_type: str = Field(min_length=1, max_length=128)
    file_name: str = Field(min_length=1, max_length=512)
    mime_type: str = Field(min_length=1, max_length=256)
    source_channel: str = Field(min_length=1, max_length=128)
    received_at: datetime
    sha256: str = Field(min_length=64, max_length=64)
    language: str = Field(default="en", min_length=2, max_length=16)
    contains_scan: bool = False
    metadata: dict[str, object] = Field(default_factory=dict)


class EvidenceRecordIngest(BaseModel):
    organization_id: UUID
    record_id: UUID
    document_id: UUID
    field_name: str = Field(min_length=1, max_length=128)
    observed_value: str | None = None
    canonical_value: str | None = None
    corruption_type: str = Field(default="none", max_length=64)
    source_locator: dict[str, object] = Field(default_factory=dict)
    gold_reference: str | None = None


class EventIngestEnvelope(BaseModel):
    organization_id: UUID
    event_id: UUID
    event_type: str = Field(min_length=1, max_length=64)
    event_time: datetime
    idempotency_key: str = Field(min_length=1, max_length=512)
    source_system: str = Field(min_length=1, max_length=128)
    payload: dict[str, object] = Field(default_factory=dict)
    provenance: list[dict[str, object]] = Field(min_length=1)


class IngestionReceipt(BaseModel):
    organization_id: UUID
    resource_id: UUID
    accepted: bool
    duplicate: bool
    resource_type: str
