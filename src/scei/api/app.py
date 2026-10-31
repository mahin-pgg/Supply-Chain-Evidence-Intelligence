from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI

from scei.ingestion.models import (
    EventIngestEnvelope,
    EvidenceRecordIngest,
    IngestionReceipt,
    SourceDocumentIngest,
)
from scei.storage.sqlite import SQLiteCanonicalStore


def create_app(store: SQLiteCanonicalStore | None = None) -> FastAPI:
    database_path = os.getenv("SCEI_SQLITE_PATH", "data/local/scei.db")
    if store is None and database_path != ":memory:":
        Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    canonical_store = store or SQLiteCanonicalStore(database_path)
    app = FastAPI(title="Supply Chain Evidence Intelligence", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "scei-api"}

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

    return app


app = create_app()
