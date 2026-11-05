from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from scei.investigation.genealogy import GenealogyEngine
from scei.storage.sqlite import SQLiteCanonicalStore


@dataclass(frozen=True)
class EvaluationSummary:
    organization_id: UUID
    review_counts: dict[str, int]
    graph_counts: dict[str, int]
    tenant_isolation: dict[str, object]
    investigation: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "organization_id": str(self.organization_id),
            "review_counts": self.review_counts,
            "graph_counts": self.graph_counts,
            "tenant_isolation": self.tenant_isolation,
            "investigation": self.investigation,
        }


def evaluate_organization(
    store: SQLiteCanonicalStore, organization_id: UUID, start_node_id: UUID | None = None,
    context_id: UUID | None = None,
) -> EvaluationSummary:
    statuses = ("pending", "approved", "rejected", "applying", "applied", "application_failed")
    review_counts = {
        status: store.count_resolution_reviews(organization_id, status) for status in statuses
    }
    nodes = store.list_graph_nodes(organization_id, context_id=context_id)
    edges = store.list_graph_edges(organization_id, context_id=context_id)
    graph_counts = {
        "nodes": len(nodes),
        "edges": len(edges),
        "edges_with_provenance": sum(bool(row["provenance_json"]) for row in edges),
        "edges_with_context": sum(bool(row["context_id"]) for row in edges),
    }
    other_tenant = store.connection.execute(
        """SELECT COUNT(*) FROM graph_edges e
           WHERE e.organization_id=? AND (
             NOT EXISTS (SELECT 1 FROM graph_nodes n WHERE n.organization_id=e.organization_id AND n.node_id=e.source_node_id)
             OR NOT EXISTS (SELECT 1 FROM graph_nodes n WHERE n.organization_id=e.organization_id AND n.node_id=e.target_node_id)
           )""",
        (str(organization_id),),
    ).fetchone()[0]
    tenant_isolation = {
        "cross_tenant_node_collisions": int(other_tenant),
        "passed": int(other_tenant) == 0,
    }
    investigation: dict[str, object] = {"paths": 0, "visited_nodes": 0, "warnings": []}
    if start_node_id is not None:
        result = GenealogyEngine(store).traverse(
            organization_id, start_node_id, context_id=context_id, max_hops=20, max_paths=500,
        )
        investigation = {
            "paths": len(result.paths),
            "visited_nodes": len(result.visited_node_ids),
            "warnings": result.completeness_warnings,
            "projection_run_ids": [str(value) for value in result.projection_run_ids],
        }
    return EvaluationSummary(organization_id, review_counts, graph_counts, tenant_isolation, investigation)
