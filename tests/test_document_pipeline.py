from uuid import uuid4

from scei.documents.pipeline import DocumentIngestionPipeline
from scei.storage.sqlite import SQLiteCanonicalStore


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
