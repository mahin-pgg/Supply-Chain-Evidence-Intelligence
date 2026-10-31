from datetime import UTC, datetime
from uuid import uuid4

from scei.ingestion.models import EventIngestEnvelope, SourceDocumentIngest
from scei.storage.sqlite import SQLiteCanonicalStore


def document(organization_id, document_id, digest):
    return SourceDocumentIngest(
        organization_id=organization_id,
        document_id=document_id,
        document_type="batch_certificate",
        file_name="certificate.pdf",
        mime_type="application/pdf",
        source_channel="supplier_portal",
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        sha256=digest,
        language="en",
    )


def event(organization_id, event_id, key):
    return EventIngestEnvelope(
        organization_id=organization_id,
        event_id=event_id,
        event_type="RECEIVE",
        event_time=datetime(2026, 1, 1, tzinfo=UTC),
        idempotency_key=key,
        source_system="TEST",
        payload={"batch_code": "BATCH-BD-00000001"},
        provenance=[{"document_id": str(uuid4())}],
    )


def test_event_ingestion_is_idempotent_and_tenant_scoped():
    store = SQLiteCanonicalStore()
    tenant_a, tenant_b = uuid4(), uuid4()
    assert store.add_document(document(tenant_a, uuid4(), "a" * 64)) is True
    assert store.add_event(event(tenant_a, uuid4(), "tenant-a-event")) is True
    assert store.add_event(event(tenant_a, uuid4(), "tenant-a-event")) is False
    assert store.add_event(event(tenant_b, uuid4(), "tenant-b-event")) is True
    assert store.count("canonical_events", tenant_a) == 1
    assert store.count("canonical_events", tenant_b) == 1
    assert store.count("canonical_events") == 2
    store.close()
