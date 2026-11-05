from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import UUID

from scei.llm.ollama import OllamaClient, OllamaUnavailable
from scei.storage.sqlite import SQLiteCanonicalStore


@dataclass(frozen=True)
class ReviewAssistance:
    review_id: UUID
    mode: str
    recommendation: str
    rationale: str
    confidence: float
    citations: list[dict[str, object]]
    warning: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "review_id": str(self.review_id),
            "mode": self.mode,
            "recommendation": self.recommendation,
            "rationale": self.rationale,
            "confidence": self.confidence,
            "citations": self.citations,
            "warning": self.warning,
            "advisory_only": True,
        }


class ReviewAssistanceService:
    def __init__(self, store: SQLiteCanonicalStore, client: OllamaClient | None = None) -> None:
        self.store = store
        self.client = client or OllamaClient()

    def assist(self, organization_id: UUID, review_id: UUID) -> ReviewAssistance:
        review = self.store.get_resolution_review(organization_id, review_id)
        if review is None:
            raise ValueError("review not found")
        event = self.store.connection.execute(
            "SELECT * FROM canonical_events WHERE organization_id=? AND event_id=?",
            (str(organization_id), str(review["event_id"])),
        ).fetchone()
        evidence = self.store.search_evidence(
            organization_id, str(review["observed_value"]), limit=10
        )
        citations = [
            {
                "evidence_record_id": item["evidence_record_id"],
                "document_id": item["document_id"],
                "locator": item["source_locator"],
            }
            for item in evidence
        ]
        source = {
            "review": {
                "field_name": review["field_name"],
                "entity_type": review["entity_type"],
                "observed_value": review["observed_value"],
                "candidates": review["candidates"],
            },
            "event": {
                "event_id": review["event_id"],
                "payload": json.loads(event["payload_json"]) if event else {},
                "provenance": json.loads(event["provenance_json"]) if event else [],
            },
            "evidence": evidence,
        }
        schema = {
            "type": "object",
            "properties": {
                "recommendation": {"type": "string", "enum": ["approve", "reject", "insufficient_evidence"]},
                "rationale": {"type": "string"},
                "confidence": {"type": "number"},
                "citation_ids": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["recommendation", "rationale", "confidence", "citation_ids"],
        }
        prompt = (
            "You are an advisory reviewer. Use only the supplied JSON. Do not invent identifiers or facts. "
            "Recommend approve, reject, or insufficient_evidence. Cite only evidence_record_id values supplied. "
            "This output never changes review status.\n\n" + json.dumps(source, default=str)
        )
        try:
            result = self.client.generate_json(prompt, schema)
            allowed = {str(item["evidence_record_id"]) for item in evidence}
            citation_ids = [str(value) for value in result.get("citation_ids", []) if str(value) in allowed]
            citations = [item for item in citations if str(item["evidence_record_id"]) in citation_ids]
            confidence = max(0.0, min(1.0, float(result.get("confidence", 0.0))))
            return ReviewAssistance(
                review_id=review_id, mode="ollama", recommendation=str(result.get("recommendation", "insufficient_evidence")),
                rationale=str(result.get("rationale", "No rationale returned.")), confidence=confidence, citations=citations,
            )
        except (OllamaUnavailable, ValueError, TypeError, json.JSONDecodeError) as error:
            return ReviewAssistance(
                review_id=review_id, mode="deterministic_fallback", recommendation="insufficient_evidence",
                rationale="Ollama assistance was unavailable; reviewer must use deterministic evidence and candidates.",
                confidence=0.0, citations=citations, warning=str(error),
            )
