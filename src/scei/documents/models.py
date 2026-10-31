from __future__ import annotations

from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


class DocumentKind(StrEnum):
    TEXT = "text"
    CSV = "csv"
    SPREADSHEET = "spreadsheet"
    DOCX = "docx"
    PDF = "pdf"
    UNKNOWN = "unknown"


class SourceLocator(BaseModel):
    page: int | None = Field(default=None, ge=1)
    sheet: str | None = None
    row: int | None = Field(default=None, ge=1)
    column: int | None = Field(default=None, ge=1)
    paragraph: int | None = Field(default=None, ge=1)
    bounding_box: list[float] | None = None


class ExtractedSegment(BaseModel):
    text: str
    locator: SourceLocator
    extraction_method: str
    confidence: float = Field(ge=0, le=1)


class ExtractedClaim(BaseModel):
    field_name: str
    value: str
    normalized_value: str | None = None
    locator: SourceLocator
    extraction_method: str
    confidence: float = Field(ge=0, le=1)


class ExtractedDocument(BaseModel):
    document_id: UUID
    organization_id: UUID
    file_name: str
    kind: DocumentKind
    mime_type: str
    segments: list[ExtractedSegment] = Field(default_factory=list)
    claims: list[ExtractedClaim] = Field(default_factory=list)
    requires_ocr: bool = False
    warnings: list[str] = Field(default_factory=list)
