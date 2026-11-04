from __future__ import annotations

from uuid import uuid5

from scei.graph.models import GraphEdge, GraphNode, ProjectionResult
from scei.ingestion.models import EventIngestEnvelope
from scei.resolution.models import ResolutionDecision, ResolutionState
from scei.storage.sqlite import SQLiteCanonicalStore

EDGE_PRIORITY = {
    ResolutionState.VERIFIED: 0,
    ResolutionState.PROBABLE: 1,
    ResolutionState.POSSIBLE: 2,
    ResolutionState.CONTRADICTED: 3,
    ResolutionState.UNKNOWN: 4,
}


class GenealogyProjector:
    """Projects only resolved event relationships into a tenant-scoped graph."""

    def __init__(self, store: SQLiteCanonicalStore) -> None:
        self.store = store

    def project_event(
        self, event: EventIngestEnvelope, bindings: dict[str, ResolutionDecision],
        context_id=None, dataset_version_id=None, projection_run_id=None,
    ) -> ProjectionResult:
        nodes: list[GraphNode] = []
        edges: list[GraphEdge] = []
        gaps: list[str] = []
        payload = event.payload
        if event.event_type == "TRANSFORM":
            self._require_binding(
                bindings, payload.get("source_batch_code"), "source_batch_code", gaps
            )
            self._require_binding(
                bindings, payload.get("destination_batch_code"), "destination_batch_code", gaps
            )
            source = bindings.get(str(payload.get("source_batch_code")))
            target = bindings.get(str(payload.get("destination_batch_code")))
            if (
                source
                and target
                and source.selected_entity_id
                and target.selected_entity_id
                and not gaps
            ):
                nodes.extend(
                    [
                        self._node(event, source, "batch", str(payload["source_batch_code"])),
                        self._node(event, target, "batch", str(payload["destination_batch_code"])),
                    ]
                )
                edges.append(self._edge(event, source, target, "TRANSFORMS_INTO"))
        elif event.event_type == "PRODUCE":
            self._require_binding(
                bindings, payload.get("input_batch_code"), "input_batch_code", gaps
            )
            self._require_binding(bindings, payload.get("product_code"), "product_code", gaps)
            source = bindings.get(str(payload.get("input_batch_code")))
            target = bindings.get(str(payload.get("product_code")))
            if (
                source
                and target
                and source.selected_entity_id
                and target.selected_entity_id
                and not gaps
            ):
                nodes.extend(
                    [
                        self._node(event, source, "batch", str(payload["input_batch_code"])),
                        self._node(event, target, "product", str(payload["product_code"])),
                    ]
                )
                edges.append(self._edge(event, source, target, "PRODUCES"))
        elif event.event_type in {"RECEIVE", "STORE", "SHIP", "QUARANTINE"}:
            self._require_binding(bindings, payload.get("batch_code"), "batch_code", gaps)
            batch = bindings.get(str(payload.get("batch_code")))
            if batch and batch.selected_entity_id:
                nodes.append(self._node(event, batch, "batch", str(payload["batch_code"])))
        else:
            gaps.append(f"event_type_not_supported:{event.event_type}")
        for node in nodes:
            self.store.upsert_graph_node(node, context_id, dataset_version_id, projection_run_id)
        for edge in edges:
            self.store.add_graph_edge(edge, context_id, dataset_version_id, projection_run_id)
        return ProjectionResult(
            event_id=event.event_id, nodes_written=len(nodes), edges_written=len(edges), gaps=gaps
        )

    @staticmethod
    def _require_binding(
        bindings: dict[str, ResolutionDecision], value: object, field: str, gaps: list[str]
    ) -> None:
        if not value:
            gaps.append(f"missing_{field}")
        elif str(value) not in bindings:
            gaps.append(f"unresolved_{field}:{value}")
        elif bindings[str(value)].selected_entity_id is None:
            gaps.append(f"unsafe_{field}:{value}")

    @staticmethod
    def _node(
        event: EventIngestEnvelope, decision: ResolutionDecision, node_type: str, label: str
    ) -> GraphNode:
        assert decision.selected_entity_id is not None
        return GraphNode(
            organization_id=event.organization_id,
            node_id=decision.selected_entity_id,
            node_type=node_type,
            label=label,
            attributes={
                "resolution_state": decision.state.value,
                "resolution_score": decision.score,
            },
        )

    @staticmethod
    def _edge(
        event: EventIngestEnvelope,
        source: ResolutionDecision,
        target: ResolutionDecision,
        edge_type: str,
    ) -> GraphEdge:
        assert source.selected_entity_id is not None and target.selected_entity_id is not None
        state = (
            source.state
            if EDGE_PRIORITY[source.state] >= EDGE_PRIORITY[target.state]
            else target.state
        )
        return GraphEdge(
            organization_id=event.organization_id,
            edge_id=uuid5(event.event_id, edge_type),
            source_node_id=source.selected_entity_id,
            target_node_id=target.selected_entity_id,
            edge_type=edge_type,
            event_id=event.event_id,
            relationship_state=state,
            quantity=event.payload.get("quantity")
            if isinstance(event.payload.get("quantity"), dict)
            else None,
            provenance=event.provenance,
        )
