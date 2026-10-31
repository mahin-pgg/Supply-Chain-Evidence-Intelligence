from datetime import UTC, datetime
from uuid import uuid4

from scei.graph.projector import GenealogyProjector
from scei.ingestion.models import EventIngestEnvelope
from scei.resolution.models import EntityProfile
from scei.resolution.resolver import EntityResolver
from scei.storage.sqlite import SQLiteCanonicalStore


def test_transform_event_projects_only_resolved_relationships():
    tenant = uuid4()
    source_profile = EntityProfile(
        entity_id=uuid4(),
        organization_id=tenant,
        entity_type="batch",
        canonical_name="source",
        identifiers=["BATCH-A"],
    )
    target_profile = EntityProfile(
        entity_id=uuid4(),
        organization_id=tenant,
        entity_type="batch",
        canonical_name="target",
        identifiers=["BATCH-B"],
    )
    resolver = EntityResolver([source_profile, target_profile])
    bindings = {value: resolver.resolve(tenant, "batch", value) for value in ["BATCH-A", "BATCH-B"]}
    event = EventIngestEnvelope(
        organization_id=tenant,
        event_id=uuid4(),
        event_type="TRANSFORM",
        event_time=datetime.now(UTC),
        idempotency_key="transform-1",
        source_system="MES",
        payload={
            "source_batch_code": "BATCH-A",
            "destination_batch_code": "BATCH-B",
            "quantity": {"value": 100, "unit": "kg"},
        },
        provenance=[{"document_id": str(uuid4()), "page": 1}],
    )
    store = SQLiteCanonicalStore()
    result = GenealogyProjector(store).project_event(event, bindings)
    assert result.edges_written == 1
    assert result.gaps == []
    assert len(store.list_graph_edges(tenant)) == 1
    store.close()


def test_unresolved_transform_creates_a_gap_without_an_edge():
    tenant = uuid4()
    source = EntityProfile(
        entity_id=uuid4(),
        organization_id=tenant,
        entity_type="batch",
        canonical_name="source",
        identifiers=["BATCH-A"],
    )
    binding = EntityResolver([source]).resolve(tenant, "batch", "BATCH-A")
    event = EventIngestEnvelope(
        organization_id=tenant,
        event_id=uuid4(),
        event_type="TRANSFORM",
        event_time=datetime.now(UTC),
        idempotency_key="transform-2",
        source_system="MES",
        payload={"source_batch_code": "BATCH-A", "destination_batch_code": "UNKNOWN"},
        provenance=[{"document_id": str(uuid4())}],
    )
    store = SQLiteCanonicalStore()
    result = GenealogyProjector(store).project_event(event, {"BATCH-A": binding})
    assert result.edges_written == 0
    assert any(item.startswith("unresolved_destination_batch_code") for item in result.gaps)
    assert store.list_graph_edges(tenant) == []
    store.close()
