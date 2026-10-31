from uuid import uuid4

from scei.documents.pipeline import DocumentIngestionPipeline
from scei.documents.semantic import SemanticDocumentEnricher
from scei.storage.sqlite import SQLiteCanonicalStore


class FakeSemanticClient:
    def generate_json(self, prompt, schema):
        return {
            "document_type": "batch_release_certificate",
            "claims": [
                {"field_name": "contamination_signal", "value": "contamination", "confidence": 0.88},
                {"field_name": "unsupported", "value": "not in source", "confidence": 0.99},
            ],
        }


def test_document_pipeline_persists_document_and_claims(tmp_path):
    path = tmp_path / "certificate.txt"
    path.write_text("LOT-A-102 received 500 kg on 2026-08-01", encoding="utf-8")
    organization_id, document_id = uuid4(), uuid4()
    store = SQLiteCanonicalStore()
    extracted = DocumentIngestionPipeline(store).ingest_path(path, organization_id, document_id)
    assert extracted.claims
    assert store.count("source_documents", organization_id) == 1
    assert store.count("evidence_records", organization_id) == len(extracted.claims)
    store.close()


def test_semantic_claims_are_anchored_before_persistence(tmp_path):
    path = tmp_path / "certificate.txt"
    path.write_text("LOT-A-102 passed testing; contamination signal detected", encoding="utf-8")
    organization_id, document_id = uuid4(), uuid4()
    store = SQLiteCanonicalStore()
    extracted = DocumentIngestionPipeline(
        store, semantic_enricher=SemanticDocumentEnricher(FakeSemanticClient())
    ).ingest_path(path, organization_id, document_id)
    semantic_claims = [claim for claim in extracted.claims if claim.extraction_method == "ollama:semantic"]
    assert len(semantic_claims) == 1
    assert semantic_claims[0].locator.paragraph == 1
    assert store.count("evidence_records", organization_id) == len(extracted.claims)
    store.close()
