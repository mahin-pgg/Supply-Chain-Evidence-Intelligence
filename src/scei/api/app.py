from __future__ import annotations

import os
from pathlib import Path
from uuid import UUID, uuid4

from fastapi import FastAPI, File, Form, Query, UploadFile

from scei.documents.pipeline import DocumentIngestionPipeline
from scei.documents.semantic import SemanticDocumentEnricher
from scei.ingestion.models import (
    EventIngestEnvelope,
    EvidenceRecordIngest,
    IngestionReceipt,
    SourceDocumentIngest,
)
from scei.investigation.api_models import ContaminationInvestigationRequest, GenealogyQuery
from scei.investigation.genealogy import GenealogyEngine
from scei.retrieval.api_models import EventSearchResponse, EvidenceSearchResponse, bounded_limit
from scei.storage.sqlite import SQLiteCanonicalStore


def create_app(
    store: SQLiteCanonicalStore | None = None,
    semantic_enricher: SemanticDocumentEnricher | None = None,
) -> FastAPI:
    database_path = os.getenv("SCEI_SQLITE_PATH", "data/local/scei.db")
    if store is None and database_path != ":memory:":
        Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    canonical_store = store or SQLiteCanonicalStore(database_path)
    if semantic_enricher is None and os.getenv("SCEI_ENABLE_SEMANTIC", "0").lower() in {
        "1",
        "true",
        "yes",
    }:
        semantic_enricher = SemanticDocumentEnricher()
    app = FastAPI(title="Supply Chain Evidence Intelligence", version="0.1.0")
    app.state.canonical_store = canonical_store

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "scei-api"}

    @app.get("/v1/search/evidence", response_model=EvidenceSearchResponse)
    def search_evidence(
        organization_id: UUID,
        q: str = Query(min_length=1, max_length=256),
        field_name: str | None = Query(default=None, max_length=128),
        limit: int = Query(default=25, ge=1, le=100),
    ) -> EvidenceSearchResponse:
        results = canonical_store.search_evidence(
            organization_id, q, field_name=field_name, limit=bounded_limit(limit)
        )
        return EvidenceSearchResponse(
            organization_id=organization_id, query=q, total=len(results), results=results
        )

    @app.get("/v1/search/events", response_model=EventSearchResponse)
    def search_events(
        organization_id: UUID,
        q: str = Query(min_length=1, max_length=256),
        limit: int = Query(default=25, ge=1, le=100),
    ) -> EventSearchResponse:
        results = canonical_store.search_events(organization_id, q, limit=bounded_limit(limit))
        return EventSearchResponse(
            organization_id=organization_id, query=q, total=len(results), results=results
        )

    @app.post("/v1/ingest/documents", response_model=IngestionReceipt, status_code=202)
    def ingest_document(document: SourceDocumentIngest) -> IngestionReceipt:
        accepted = canonical_store.add_document(document)
        return IngestionReceipt(
            organization_id=document.organization_id,
            resource_id=document.document_id,
            accepted=True,
            duplicate=not accepted,
            resource_type="source_document",
        )

    @app.post("/v1/ingest/evidence", response_model=IngestionReceipt, status_code=202)
    def ingest_evidence(record: EvidenceRecordIngest) -> IngestionReceipt:
        accepted = canonical_store.add_evidence(record)
        return IngestionReceipt(
            organization_id=record.organization_id,
            resource_id=record.record_id,
            accepted=True,
            duplicate=not accepted,
            resource_type="evidence_record",
        )

    @app.post("/v1/ingest/events", response_model=IngestionReceipt, status_code=202)
    def ingest_event(event: EventIngestEnvelope) -> IngestionReceipt:
        accepted = canonical_store.add_event(event)
        return IngestionReceipt(
            organization_id=event.organization_id,
            resource_id=event.event_id,
            accepted=True,
            duplicate=not accepted,
            resource_type="canonical_event",
        )

    @app.post("/v1/documents/ingest")
    async def ingest_uploaded_document(
        organization_id: UUID = Form(...),  # noqa: B008
        file: UploadFile = File(...),  # noqa: B008
        document_id: UUID | None = Form(default=None),  # noqa: B008
    ):
        import tempfile

        document_id = document_id or uuid4()
        suffix = Path(file.filename or "document.bin").suffix
        temporary_path: Path
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(await file.read())
            temporary.flush()
        try:
            extracted = DocumentIngestionPipeline(
                canonical_store, semantic_enricher=semantic_enricher
            ).ingest_path(
                temporary_path, organization_id, document_id
            )
        finally:
            temporary_path.unlink(missing_ok=True)
        return {
            "document_id": document_id,
            "kind": extracted.kind.value,
            "claims": len(extracted.claims),
            "requires_ocr": extracted.requires_ocr,
            "warnings": extracted.warnings,
        }

    @app.post("/v1/investigations/contamination")
    def investigate_contamination(request: ContaminationInvestigationRequest):
        return GenealogyEngine(canonical_store).analyze_contamination(
            request.organization_id, request.contaminated_node_id, max_hops=request.max_hops
        )

    @app.post("/v1/genealogy/traverse")
    def traverse_genealogy(request: GenealogyQuery):
        return GenealogyEngine(canonical_store).traverse(
            request.organization_id,
            request.node_id,
            direction=request.direction,
            max_hops=request.max_hops,
        )

    return app


app = create_app()
