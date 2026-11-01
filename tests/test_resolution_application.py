from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from scei.api.app import create_app
from scei.graph.application_service import ResolutionApplicationService
from scei.graph.models import GraphNode
from scei.ingestion.models import EventIngestEnvelope
from scei.storage.sqlite import SQLiteCanonicalStore


def review(store, tenant, review_id, event_id, field="batch_code", value="BATCH-REVIEW"):
    store.add_resolution_review(
        organization_id=tenant,
        review_id=review_id,
        event_id=event_id,
        field_name=field,
        entity_type="batch",
        observed_value=value,
        candidates=[],
        rationale="No exact identifier match.",
    )


def event(tenant, event_id, value="BATCH-REVIEW"):
    return EventIngestEnvelope(
        organization_id=tenant,
        event_id=event_id,
        event_type="RECEIVE",
        event_time=datetime.now(UTC),
        idempotency_key=str(event_id),
        source_system="TEST",
        payload={"batch_code": value},
        provenance=[{"document_id": str(uuid4()), "page": 1}],
    )


def test_approved_review_creates_mapping_and_reprojects_multiple_events():
    tenant = uuid4()
    entity_id, review_id, review_event_id = uuid4(), uuid4(), uuid4()
    store = SQLiteCanonicalStore()
    store.upsert_graph_node(
        GraphNode(
            organization_id=tenant,
            node_id=entity_id,
            node_type="batch",
            label="BATCH-REVIEW",
        )
    )
    review(store, tenant, review_id, review_event_id)
    first, second = event(tenant, uuid4()), event(tenant, uuid4())
    store.add_event(first)
    store.add_event(second)
    client = TestClient(create_app(store))
    response = client.post(
        f"/v1/reviews/resolution/{review_id}",
        params={"organization_id": str(tenant)},
        json={
            "action": "approve",
            "reviewer_id": "reviewer-1",
            "selected_entity_id": str(entity_id),
            "allow_out_of_candidates": True,
        },
    )
    assert response.status_code == 200
    applied = client.post(
        f"/v1/reviews/resolution/{review_id}/apply",
        params={"organization_id": str(tenant)},
    )
    assert applied.status_code == 200
    assert applied.json()["status"] == "applied"
    assert applied.json()["events_reprojected"] == 2
    assert store.get_resolution_mapping(tenant, "BATCH-REVIEW", "batch_code", "batch")
    assert len(store.list_graph_nodes(tenant, node_type="batch")) == 1
    repeated = client.post(
        f"/v1/reviews/resolution/{review_id}/apply",
        params={"organization_id": str(tenant)},
    )
    assert repeated.status_code == 200
    assert repeated.json()["status"] == "applied"
    store.close()


def test_rejected_review_cannot_be_applied():
    tenant = uuid4()
    entity_id, review_id, review_event_id = uuid4(), uuid4(), uuid4()
    store = SQLiteCanonicalStore()
    store.upsert_graph_node(
        GraphNode(organization_id=tenant, node_id=entity_id, node_type="batch", label="BATCH-REVIEW")
    )
    review(store, tenant, review_id, review_event_id)
    client = TestClient(create_app(store))
    assert client.post(
        f"/v1/reviews/resolution/{review_id}",
        params={"organization_id": str(tenant)},
        json={"action": "reject", "reviewer_id": "reviewer-1"},
    ).status_code == 200
    response = client.post(
        f"/v1/reviews/resolution/{review_id}/apply",
        params={"organization_id": str(tenant)},
    )
    assert response.status_code == 409
    assert store.get_resolution_mapping(tenant, "BATCH-REVIEW", "batch_code", "batch") is None
    store.close()


def test_approval_rejects_cross_tenant_entity():
    tenant, other_tenant = uuid4(), uuid4()
    review_id, review_event_id, other_entity = uuid4(), uuid4(), uuid4()
    store = SQLiteCanonicalStore()
    store.upsert_graph_node(
        GraphNode(
            organization_id=other_tenant,
            node_id=other_entity,
            node_type="batch",
            label="BATCH-OTHER",
        )
    )
    review(store, tenant, review_id, review_event_id)
    client = TestClient(create_app(store))
    response = client.post(
        f"/v1/reviews/resolution/{review_id}",
        params={"organization_id": str(tenant)},
        json={
            "action": "approve",
            "reviewer_id": "reviewer-1",
            "selected_entity_id": str(other_entity),
            "allow_out_of_candidates": True,
        },
    )
    assert response.status_code == 422
    store.close()


def test_application_failure_is_retryable(monkeypatch):
    tenant = uuid4()
    entity_id, review_id, review_event_id = uuid4(), uuid4(), uuid4()
    store = SQLiteCanonicalStore()
    store.upsert_graph_node(
        GraphNode(organization_id=tenant, node_id=entity_id, node_type="batch", label="BATCH-REVIEW")
    )
    review(store, tenant, review_id, review_event_id)
    store.resolve_resolution_review(tenant, review_id, "approved", "reviewer-1", entity_id)
    original = store.mark_review_applied

    def fail_once(*args, **kwargs):
        monkeypatch.setattr(store, "mark_review_applied", original)
        raise RuntimeError("simulated crash after projection")

    monkeypatch.setattr(store, "mark_review_applied", fail_once)
    with pytest.raises(RuntimeError, match="simulated crash"):
        ResolutionApplicationService(store).apply(tenant, review_id)
    assert store.get_resolution_review(tenant, review_id)["status"] == "application_failed"
    result = ResolutionApplicationService(store).apply(tenant, review_id)
    assert result["status"] == "applied"
    store.close()
