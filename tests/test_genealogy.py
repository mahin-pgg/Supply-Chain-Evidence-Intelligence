from uuid import uuid4

from scei.graph.models import GraphEdge, GraphNode
from scei.investigation.genealogy import GenealogyEngine
from scei.resolution.models import ResolutionState
from scei.storage.sqlite import SQLiteCanonicalStore


def edge(
    tenant, source, target, event_id, state=ResolutionState.VERIFIED, edge_type="TRANSFORMS_INTO"
):
    return GraphEdge(
        organization_id=tenant,
        edge_id=uuid4(),
        source_node_id=source,
        target_node_id=target,
        edge_type=edge_type,
        event_id=event_id,
        relationship_state=state,
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
