from uuid import uuid4

from scei.graph.models import GraphEdge, GraphNode
from scei.investigation.genealogy import GenealogyEngine
from scei.resolution.models import ResolutionState
from scei.storage.sqlite import SQLiteCanonicalStore


def edge(
    tenant,
    source,
    target,
    event_id,
    state=ResolutionState.VERIFIED,
    edge_type="TRANSFORMS_INTO",
    quantity=None,
):
    return GraphEdge(
        organization_id=tenant,
        edge_id=uuid4(),
        source_node_id=source,
        target_node_id=target,
        edge_type=edge_type,
        event_id=event_id,
        relationship_state=state,
        quantity=quantity,
        provenance=[{"document_id": str(uuid4())}],
    )


def test_downstream_traversal_and_contamination_impact():
    tenant = uuid4()
    raw, api, product = uuid4(), uuid4(), uuid4()
    store = SQLiteCanonicalStore()
    for node_id, node_type in [(raw, "batch"), (api, "batch"), (product, "product")]:
        store.upsert_graph_node(
            GraphNode(
                organization_id=tenant, node_id=node_id, node_type=node_type, label=str(node_id)
            )
        )
    store.add_graph_edge(edge(tenant, raw, api, uuid4()))
    store.add_graph_edge(edge(tenant, api, product, uuid4(), edge_type="PRODUCES"))
    result = GenealogyEngine(store).analyze_contamination(tenant, raw)
    assert [item.product_id for item in result.confirmed_products] == [product]
    assert result.confirmed_products[0].shortest_hops == 2
    store.close()


def test_possible_edge_is_separated_from_confirmed_impact():
    tenant = uuid4()
    raw, product = uuid4(), uuid4()
    store = SQLiteCanonicalStore()
    for node_id, node_type in [(raw, "batch"), (product, "product")]:
        store.upsert_graph_node(
            GraphNode(
                organization_id=tenant, node_id=node_id, node_type=node_type, label=str(node_id)
            )
        )
    store.add_graph_edge(
        edge(tenant, raw, product, uuid4(), state=ResolutionState.POSSIBLE, edge_type="PRODUCES")
    )
    result = GenealogyEngine(store).analyze_contamination(tenant, raw)
    assert result.confirmed_products == []
    assert [item.product_id for item in result.possible_products] == [product]
    store.close()


def test_cycle_is_counted_and_does_not_loop_forever():
    tenant = uuid4()
    first, second = uuid4(), uuid4()
    store = SQLiteCanonicalStore()
    for node_id in [first, second]:
        store.upsert_graph_node(
            GraphNode(
                organization_id=tenant, node_id=node_id, node_type="batch", label=str(node_id)
            )
        )
    store.add_graph_edge(edge(tenant, first, second, uuid4()))
    store.add_graph_edge(edge(tenant, second, first, uuid4()))
    result = GenealogyEngine(store).traverse(tenant, first)
    assert result.cycle_count == 1
    assert len(result.paths) == 1
    store.close()


def test_multihop_path_contains_provenance_quantity_and_completeness_warnings():
    tenant = uuid4()
    raw, intermediate, product = uuid4(), uuid4(), uuid4()
    event_one, event_two = uuid4(), uuid4()
    store = SQLiteCanonicalStore()
    for node_id, node_type in [(raw, "batch"), (intermediate, "batch"), (product, "product")]:
        store.upsert_graph_node(
            GraphNode(organization_id=tenant, node_id=node_id, node_type=node_type, label=str(node_id))
        )
    store.add_graph_edge(
        edge(
            tenant,
            raw,
            intermediate,
            event_one,
            quantity={"value": 100, "unit": "kg"},
        )
    )
    store.add_graph_edge(
        edge(
            tenant,
            intermediate,
            product,
            event_two,
            edge_type="PRODUCES",
            quantity={"value": 90, "unit": "kg"},
        )
    )
    store.add_resolution_review(
        organization_id=tenant,
        review_id=uuid4(),
        event_id=uuid4(),
        field_name="batch_code",
        entity_type="batch",
        observed_value="UNRESOLVED-BATCH",
        candidates=[],
        rationale="test completeness warning",
    )
    result = GenealogyEngine(store).traverse(
        tenant, raw, direction="downstream", max_hops=3, include_possible=False
    )
    path = next(path for path in result.paths if path.hop_count == 2)
    assert [item["event_id"] for item in path.edge_details] == [str(event_one), str(event_two)]
    assert len(path.edge_details[0]["provenance"]) == 1
    assert [item["value"] for item in path.quantity_flow] == [100, 90]
    assert path.quantity_warnings == []
    assert any(item.startswith("pending_resolution_reviews:") for item in result.completeness_warnings)
    store.close()
