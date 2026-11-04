from uuid import uuid4

from fastapi.testclient import TestClient

from scei.api.app import create_app
from scei.graph.models import GraphEdge, GraphNode
from scei.resolution.models import ResolutionState
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


def test_batch_investigation_combines_evidence_and_graph_impact():
    store = SQLiteCanonicalStore()
    client = TestClient(create_app(store))
    organization_id = uuid4()
    batch_id, product_id, event_id = uuid4(), uuid4(), uuid4()
    for node_id, node_type in [(batch_id, "batch"), (product_id, "product")]:
        store.upsert_graph_node(
            GraphNode(
                organization_id=organization_id,
                node_id=node_id,
                node_type=node_type,
                label="BATCH-BD-00999" if node_id == batch_id else "PRODUCT-00999",
            )
        )
    store.add_graph_edge(
        GraphEdge(
            organization_id=organization_id,
            edge_id=uuid4(),
            source_node_id=batch_id,
            target_node_id=product_id,
            edge_type="PRODUCES",
            event_id=event_id,
            relationship_state=ResolutionState.VERIFIED,
            provenance=[{"document_id": str(uuid4())}],
        )
    )
    document_id = uuid4()
    client.post(
        "/v1/ingest/documents",
        json={
            "organization_id": str(organization_id),
            "document_id": str(document_id),
            "document_type": "certificate",
            "file_name": "batch.pdf",
            "mime_type": "application/pdf",
            "source_channel": "test",
            "received_at": "2026-01-01T00:00:00Z",
            "sha256": "c" * 64,
            "language": "en",
        },
    )
    client.post(
        "/v1/ingest/evidence",
        json={
            "organization_id": str(organization_id),
            "record_id": str(uuid4()),
            "document_id": str(document_id),
            "field_name": "batch_identifier",
            "observed_value": "batch bd 00999",
            "canonical_value": "BATCH-BD-00999",
            "source_locator": {"page": 1},
        },
    )
    response = client.post(
        "/v1/investigations/batch",
        json={"organization_id": str(organization_id), "batch_query": "BATCH BD 00999"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["graph_node_id"] == str(batch_id)
    assert body["evidence"][0]["citation"]["file_name"] == "batch.pdf"
    assert body["contamination_impact"]["confirmed_products"][0]["product_id"] == str(product_id)
    store.close()


def test_incident_investigation_resolves_tenant_scoped_contaminated_batch():
    store = SQLiteCanonicalStore()
    client = TestClient(create_app(store))
    organization_id = uuid4()
    batch_id, product_id = uuid4(), uuid4()
    store.upsert_graph_node(GraphNode(
        organization_id=organization_id, node_id=batch_id, node_type="batch", label="BATCH-BD-00028"
    ))
    store.upsert_graph_node(GraphNode(
        organization_id=organization_id, node_id=product_id, node_type="product", label="PRODUCT-1"
    ))
    event_id = uuid4()
    store.add_graph_edge(GraphEdge(
        organization_id=organization_id, edge_id=uuid4(), source_node_id=batch_id,
        target_node_id=product_id, edge_type="PRODUCES", event_id=event_id,
        relationship_state=ResolutionState.VERIFIED, provenance=[{"document_id": str(uuid4())}],
    ))
    store.add_contamination_incident(
        organization_id, "INC-BD-1", "high", "BATCH-BD-00028", ["BATCH-BD-00029"],
        "potential_api_contamination", True,
    )
    response = client.post("/v1/investigations/incident", json={
        "organization_id": str(organization_id), "incident_id": "INC-BD-1",
    })
    assert response.status_code == 200
    body = response.json()
    assert body["incident"]["incident_id"] == "INC-BD-1"
    assert body["graph_node_id"] == str(batch_id)
    assert body["contamination_impact"]["confirmed_products"][0]["product_id"] == str(product_id)
    other = client.post("/v1/investigations/incident", json={
        "organization_id": str(uuid4()), "incident_id": "INC-BD-1",
    })
    assert other.status_code == 404
    store.close()


def test_incident_report_combines_upstream_downstream_and_expected_batches():
    store = SQLiteCanonicalStore()
    client = TestClient(create_app(store))
    tenant = uuid4()
    raw, contaminated, downstream = uuid4(), uuid4(), uuid4()
    context = uuid4()
    for node_id, label in [(raw, "BATCH-RAW"), (contaminated, "BATCH-CONTAMINATED"), (downstream, "BATCH-AFFECTED")]:
        store.upsert_graph_node(GraphNode(organization_id=tenant, node_id=node_id, node_type="batch", label=label, attributes={}), context_id=context)
    for source, target in [(raw, contaminated), (contaminated, downstream)]:
        store.add_graph_edge(GraphEdge(
            organization_id=tenant, edge_id=uuid4(), source_node_id=source, target_node_id=target,
            edge_type="TRANSFORMS_INTO", event_id=uuid4(), relationship_state=ResolutionState.VERIFIED,
            provenance=[{"document_id": str(uuid4())}],
        ), context_id=context)
    store.add_contamination_incident(tenant, "INC-REPORT", "high", "BATCH-CONTAMINATED", ["BATCH-AFFECTED"], "test", True)
    response = client.get("/v1/investigations/incident/INC-REPORT/report", params={"organization_id": str(tenant), "context_id": str(context)})
    assert response.status_code == 200
    body = response.json()
    assert body["upstream"]["paths"]
    assert body["downstream"]["paths"]
    assert body["observed_projected_expected_batches"] == ["BATCH-AFFECTED"]
    assert body["missing_expected_batches"] == []
    store.close()


def test_resolution_review_queue_is_tenant_scoped_and_decidable():
    store = SQLiteCanonicalStore()
    client = TestClient(create_app(store))
    organization_id = uuid4()
    other_organization_id = uuid4()
    review_id, event_id = uuid4(), uuid4()
    store.add_resolution_review(
        organization_id=organization_id,
        review_id=review_id,
        event_id=event_id,
        field_name="batch_code",
        entity_type="batch",
        observed_value="BATCH-BD-O123",
        candidates=[],
        rationale="No exact identifier match.",
    )
    response = client.get(
        "/v1/reviews/resolution", params={"organization_id": str(other_organization_id)}
    )
    assert response.status_code == 200
    assert response.json()["total"] == 0
    response = client.get(
        "/v1/reviews/resolution", params={"organization_id": str(organization_id)}
    )
    assert response.json()["total"] == 1
    response = client.post(
        f"/v1/reviews/resolution/{review_id}",
        params={"organization_id": str(organization_id)},
        json={"action": "reject", "reviewer_id": "qa-user-1"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "rejected"
    assert client.get(
        "/v1/reviews/resolution",
        params={"organization_id": str(organization_id), "status": "pending"},
    ).json()["total"] == 0
    store.close()
