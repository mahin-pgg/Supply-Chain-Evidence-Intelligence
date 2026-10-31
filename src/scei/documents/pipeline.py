from __future__ import annotations

import hashlib
import mimetypes
from pathlib import Path
from uuid import UUID, uuid5

from scei.documents.extractor import DocumentExtractor
from scei.documents.models import ExtractedDocument
from scei.ingestion.models import EvidenceRecordIngest, SourceDocumentIngest
from scei.storage.sqlite import SQLiteCanonicalStore


class DocumentIngestionPipeline:
    """Persist an original document plus all deterministic claims with provenance."""

    def __init__(
        self, store: SQLiteCanonicalStore, extractor: DocumentExtractor | None = None
    ) -> None:
        self.store = store
        self.extractor = extractor or DocumentExtractor()

    def ingest_path(
        self, path: str | Path, organization_id: UUID, document_id: UUID
    ) -> ExtractedDocument:
        file_path = Path(path)
        content = file_path.read_bytes()
        extracted = self.extractor.extract(file_path, document_id, organization_id)
        document = SourceDocumentIngest(
            organization_id=organization_id,
            document_id=document_id,
            document_type=extracted.kind.value,
            file_name=file_path.name,
            mime_type=extracted.mime_type
            or mimetypes.guess_type(file_path.name)[0]
            or "application/octet-stream",
            source_channel="local_upload",
            received_at=file_path.stat().st_mtime,
            sha256=hashlib.sha256(content).hexdigest(),
            language="en",
            contains_scan=extracted.requires_ocr,
            metadata={"warnings": extracted.warnings},
        )
        self.store.add_document(document)
        for claim in extracted.claims:
            record = EvidenceRecordIngest(
                organization_id=organization_id,
                record_id=uuid5(
                    document_id,
                    f"{claim.field_name}:{claim.value}:{claim.locator.model_dump_json()}",
                ),
                document_id=document_id,
                field_name=claim.field_name,
                observed_value=claim.value,
                canonical_value=claim.normalized_value,
                corruption_type="none",
                source_locator=claim.locator.model_dump(mode="json"),
                gold_reference=None,
            )
            self.store.add_evidence(record)
        return extracted
