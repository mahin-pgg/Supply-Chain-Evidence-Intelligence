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
