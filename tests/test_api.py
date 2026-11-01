from fastapi.testclient import TestClient

from scei.api.app import create_app
from scei.storage.sqlite import SQLiteCanonicalStore


def test_health_and_ingestion_endpoint():
    client = TestClient(create_app(SQLiteCanonicalStore()))
    assert client.get("/health").json()["status"] == "ok"
    response = client.post(
        "/v1/ingest/events",
        json={
            "organization_id": "00000000-0000-0000-0000-000000000001",
            "event_id": "00000000-0000-0000-0000-000000000002",
            "event_type": "RECEIVE",
            "event_time": "2026-01-01T00:00:00Z",
            "idempotency_key": "test-event",
            "source_system": "TEST",
            "payload": {"batch_code": "BATCH-BD-00000001"},
            "provenance": [{"document_id": "DOC-1"}],
        },
    )
    assert response.status_code == 202
    assert response.json()["duplicate"] is False


def test_document_upload_endpoint():
    client = TestClient(create_app(SQLiteCanonicalStore()))
    response = client.post(
        "/v1/documents/ingest",
        data={"organization_id": "00000000-0000-0000-0000-000000000001"},
        files={"file": ("certificate.txt", b"LOT-A-102 received 500 kg", "text/plain")},
    )
    assert response.status_code == 200
    assert response.json()["claims"] == 2


def test_evidence_search_normalizes_identifiers_and_is_tenant_scoped():
    store = SQLiteCanonicalStore()
    client = TestClient(create_app(store))
    organization_id = "00000000-0000-0000-0000-000000000011"
    other_organization_id = "00000000-0000-0000-0000-000000000012"
    document_id = "00000000-0000-0000-0000-000000000013"
    for org, doc, digest in [
        (organization_id, document_id, "a" * 64),
        (other_organization_id, "00000000-0000-0000-0000-000000000014", "b" * 64),
    ]:
        assert client.post(
            "/v1/ingest/documents",
            json={
                "organization_id": org,
                "document_id": doc,
                "document_type": "certificate",
                "file_name": "release_certificate.txt",
                "mime_type": "text/plain",
                "source_channel": "test",
                "received_at": "2026-01-01T00:00:00Z",
                "sha256": digest,
                "language": "en",
            },
        ).status_code == 202
    assert client.post(
        "/v1/ingest/evidence",
        json={
            "organization_id": organization_id,
            "record_id": "00000000-0000-0000-0000-000000000015",
            "document_id": document_id,
            "field_name": "batch_identifier",
            "observed_value": "BATCH-BD-00001234",
            "canonical_value": "BATCH-BD-00001234",
            "corruption_type": "none",
            "source_locator": {"paragraph": 2},
        },
    ).status_code == 202
    assert client.post(
        "/v1/ingest/evidence",
        json={
            "organization_id": other_organization_id,
            "record_id": "00000000-0000-0000-0000-000000000016",
            "document_id": "00000000-0000-0000-0000-000000000014",
            "field_name": "batch_identifier",
            "observed_value": "BATCH-BD-00001234",
            "canonical_value": "BATCH-BD-00001234",
            "corruption_type": "none",
            "source_locator": {"paragraph": 1},
        },
    ).status_code == 202

    response = client.get(
        "/v1/search/evidence",
        params={"organization_id": organization_id, "q": "batch bd 00001234"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["results"][0]["citation"]["file_name"] == "release_certificate.txt"
    assert body["results"][0]["source_locator"] == {"paragraph": 2}
    store.close()
