from __future__ import annotations

import os
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field

from scei.documents.pipeline import DocumentIngestionPipeline
from scei.documents.semantic import SemanticDocumentEnricher
from scei.graph.application_service import ResolutionApplicationError, ResolutionApplicationService
from scei.ingestion.models import (
    EventIngestEnvelope,
    EvidenceRecordIngest,
    IngestionReceipt,
    SourceDocumentIngest,
)
from scei.investigation.api_models import (
    BatchInvestigationRequest,
    ContaminationInvestigationRequest,
    GenealogyQuery,
    IncidentInvestigationRequest,
)
from scei.investigation.genealogy import GenealogyEngine
from scei.retrieval.api_models import EventSearchResponse, EvidenceSearchResponse, bounded_limit
from scei.storage.sqlite import SQLiteCanonicalStore


class ResolutionReviewDecision(BaseModel):
    action: Literal["approve", "reject"]
    reviewer_id: str = Field(min_length=1, max_length=128)
    selected_entity_id: UUID | None = None
    allow_out_of_candidates: bool = False


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

    @app.get("/v1/datasets/contexts")
    def list_dataset_contexts(organization_id: UUID, workspace_id: UUID | None = None) -> dict[str, object]:
        try:
            query = "SELECT * FROM investigation_contexts WHERE organization_id=?"
            params: list[object] = [str(organization_id)]
            if workspace_id is not None:
                query += " AND workspace_id=?"
                params.append(str(workspace_id))
            query += " ORDER BY created_at DESC"
            rows = [dict(row) for row in canonical_store.connection.execute(query, params)]
        except Exception as error:
            raise HTTPException(status_code=404, detail="dataset context store not available") from error
        return {"organization_id": organization_id, "total": len(rows), "contexts": rows}

    @app.get("/v1/datasets/contexts/{context_id}")
    def get_dataset_context(context_id: UUID, organization_id: UUID) -> dict[str, object]:
        row = canonical_store.connection.execute(
            "SELECT * FROM investigation_contexts WHERE context_id=? AND organization_id=?",
            (str(context_id), str(organization_id)),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="dataset context not found")
        result = dict(row)
        result["projection_runs"] = [dict(item) for item in canonical_store.connection.execute(
            "SELECT * FROM projection_runs WHERE context_id=? ORDER BY started_at", (str(context_id),)
        )]
        return result

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

    @app.get("/v1/reviews/resolution")
    def list_resolution_reviews(
        organization_id: UUID,
        status: Literal[
            "pending", "approved", "rejected", "applying", "applied", "application_failed"
        ] = "pending",
        limit: int = Query(default=50, ge=1, le=100),
    ) -> dict[str, object]:
        results = canonical_store.list_resolution_reviews(organization_id, status, limit)
        return {"organization_id": organization_id, "status": status, "total": len(results), "results": results}

    @app.post("/v1/reviews/resolution/{review_id}")
    def decide_resolution_review(
        review_id: UUID,
        organization_id: UUID,
        decision: ResolutionReviewDecision,
    ) -> dict[str, object]:
        review = canonical_store.get_resolution_review(organization_id, review_id)
        if review is None:
            raise HTTPException(status_code=404, detail="review not found")
        if review["status"] != "pending":
            raise HTTPException(status_code=409, detail="review has already been decided")
        if decision.action == "approve" and decision.selected_entity_id is None:
            raise HTTPException(status_code=422, detail="selected_entity_id is required for approval")
        if decision.action == "approve":
            if not canonical_store.entity_belongs_to_tenant(
                organization_id, decision.selected_entity_id
            ):
                raise HTTPException(status_code=422, detail="selected entity is outside the tenant")
            candidate_ids = {
                str(candidate.get("entity_id")) for candidate in review["candidates"]
            }
            if (
                candidate_ids
                and str(decision.selected_entity_id) not in candidate_ids
                and not decision.allow_out_of_candidates
            ):
                raise HTTPException(
                    status_code=422,
                    detail="selected entity is not a candidate; explicitly set allow_out_of_candidates",
                )
        result = canonical_store.resolve_resolution_review(
            organization_id,
            review_id,
            "approved" if decision.action == "approve" else "rejected",
            decision.reviewer_id,
            decision.selected_entity_id,
        )
        if result is None:
            raise HTTPException(status_code=404, detail="pending review not found")
        return result

    @app.post("/v1/reviews/resolution/{review_id}/apply")
    def apply_resolution_review(review_id: UUID, organization_id: UUID, context_id: UUID | None = None) -> dict[str, object]:
        try:
            return ResolutionApplicationService(canonical_store).apply(organization_id, review_id, context_id)
        except ResolutionApplicationError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error

    @app.get("/v1/reviews/resolution/{review_id}/history")
    def resolution_review_history(review_id: UUID, organization_id: UUID) -> dict[str, object]:
        review = canonical_store.get_resolution_review(organization_id, review_id)
        if review is None:
            raise HTTPException(status_code=404, detail="review not found")
        return {
            "organization_id": organization_id,
            "review_id": review_id,
            "decisions": canonical_store.list_review_decisions(organization_id, review_id),
        }

    @app.post("/v1/investigations/batch")
    def investigate_batch(request: BatchInvestigationRequest) -> dict[str, object]:
        evidence = canonical_store.search_evidence(
            request.organization_id,
            request.batch_query,
            field_name="batch_identifier",
            limit=100,
        )
        identifiers = list(
            dict.fromkeys(
                str(hit["canonical_value"] or hit["observed_value"])
                for hit in evidence
                if hit["canonical_value"] or hit["observed_value"]
            )
        )
        node_id = canonical_store.find_graph_node_by_label(request.organization_id, identifiers)
        gaps: list[str] = []
        genealogy = None
        impact = None
        if node_id is None:
            gaps.append("no_graph_node_projection")
        else:
            engine = GenealogyEngine(canonical_store)
            genealogy = engine.traverse(
                request.organization_id,
                node_id,
                max_hops=request.max_hops,
                include_possible=request.include_possible,
                context_id=request.context_id,
            ).model_dump(mode="json")
            impact = engine.analyze_contamination(
                request.organization_id,
                node_id,
                max_hops=request.max_hops,
                include_possible=request.include_possible,
                context_id=request.context_id,
            ).model_dump(mode="json")
        return {
            "organization_id": request.organization_id,
            "query": request.batch_query,
            "resolved_identifiers": identifiers,
            "evidence": evidence,
            "graph_node_id": node_id,
            "genealogy": genealogy,
            "contamination_impact": impact,
            "gaps": gaps,
        }

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
            request.organization_id, request.contaminated_node_id, max_hops=request.max_hops,
            include_possible=request.include_possible, context_id=request.context_id,
        )

    @app.post("/v1/investigations/incident")
    def investigate_incident(request: IncidentInvestigationRequest) -> dict[str, object]:
        incident = canonical_store.get_contamination_incident(
            request.organization_id, request.incident_id
        )
        if incident is None:
            raise HTTPException(status_code=404, detail="contamination incident not found")
        node_id = canonical_store.find_graph_node_by_label(
            request.organization_id, [str(incident["contaminated_batch_code"])], context_id=request.context_id
        )
        if node_id is None:
            raise HTTPException(status_code=409, detail="contaminated batch is not projected in context")
        engine = GenealogyEngine(canonical_store)
        genealogy = engine.traverse(
            request.organization_id, node_id, direction="downstream", max_hops=request.max_hops,
            include_possible=request.include_possible, context_id=request.context_id,
        )
        impact = engine.analyze_contamination(
            request.organization_id, node_id, max_hops=request.max_hops,
            include_possible=request.include_possible, context_id=request.context_id,
        )
        return {"incident": incident, "graph_node_id": node_id, "genealogy": genealogy, "contamination_impact": impact}

    @app.post("/v1/investigations/incident/{incident_id}/review")
    def create_incident_review(incident_id: str, organization_id: UUID) -> dict[str, object]:
        review = canonical_store.create_incident_review(organization_id, incident_id)
        if review is None:
            raise HTTPException(status_code=404, detail="incident or matching event not found")
        return review

    @app.get("/v1/investigations/incident/{incident_id}/report")
    def incident_report(
        incident_id: str, organization_id: UUID, context_id: UUID | None = None,
        max_hops: int = Query(default=20, ge=1, le=100),
        include_possible: bool = False,
    ) -> dict[str, object]:
        incident = canonical_store.get_contamination_incident(organization_id, incident_id)
        if incident is None:
            raise HTTPException(status_code=404, detail="contamination incident not found")
        node_id = canonical_store.find_graph_node_by_label(
            organization_id, [str(incident["contaminated_batch_code"])], context_id=context_id
        )
        if node_id is None:
            raise HTTPException(status_code=409, detail="contaminated batch is not projected in context")
        engine = GenealogyEngine(canonical_store)
        upstream = engine.traverse(organization_id, node_id, direction="upstream", max_hops=max_hops, include_possible=include_possible, context_id=context_id)
        downstream = engine.traverse(organization_id, node_id, direction="downstream", max_hops=max_hops, include_possible=include_possible, context_id=context_id)
        impact = engine.analyze_contamination(organization_id, node_id, max_hops=max_hops, include_possible=include_possible, context_id=context_id)
        nodes = canonical_store.list_graph_nodes(organization_id, context_id=context_id)
        labels = {str(row["node_id"]): str(row["label"]) for row in nodes if row["node_type"] == "batch"}
        observed = sorted({labels.get(str(path.node_ids[-1]), str(path.node_ids[-1])) for path in downstream.paths})
        expected = [str(value) for value in incident["expected_affected_batch_codes"]]
        matched = [code for code in expected if code in observed]
        warnings = sorted(set(upstream.completeness_warnings + downstream.completeness_warnings + impact.unresolved_gaps))
        return {
            "incident": incident,
            "context_id": context_id,
            "contaminated_node_id": node_id,
            "upstream": upstream,
            "downstream": downstream,
            "contamination_impact": impact,
            "expected_affected_batch_codes": expected,
            "observed_projected_expected_batches": matched,
            "missing_expected_batches": [code for code in expected if code not in matched],
            "completeness_warnings": warnings,
        }

    @app.post("/v1/genealogy/traverse")
    def traverse_genealogy(request: GenealogyQuery):
        return GenealogyEngine(canonical_store).traverse(
            request.organization_id,
            request.node_id,
            direction=request.direction,
            max_hops=request.max_hops,
            include_possible=request.include_possible,
            context_id=request.context_id,
        )

    return app


app = create_app()
