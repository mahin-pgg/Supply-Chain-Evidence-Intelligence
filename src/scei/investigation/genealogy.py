from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from uuid import UUID

from scei.investigation.models import GenealogyPath, GenealogyResult, ImpactAnalysis, ImpactProduct
from scei.resolution.models import ResolutionState
from scei.storage.sqlite import SQLiteCanonicalStore


@dataclass(frozen=True)
class _Frontier:
    node_id: UUID
    node_path: tuple[UUID, ...]
    edge_path: tuple[UUID, ...]
    states: tuple[ResolutionState, ...]


class GenealogyEngine:
    """Cycle-safe, tenant-scoped graph traversal over the canonical projection."""

    def __init__(self, store: SQLiteCanonicalStore) -> None:
        self.store = store

    def traverse(
        self,
        organization_id: UUID,
        start_node_id: UUID,
        *,
        direction: str = "downstream",
        max_hops: int = 20,
        max_paths: int = 5000,
        include_possible: bool = True,
        context_id: UUID | None = None,
    ) -> GenealogyResult:
        if direction not in {"upstream", "downstream"}:
            raise ValueError("direction must be upstream or downstream")
        edges = self.store.list_graph_edges(organization_id, limit=max_paths * 4, context_id=context_id)
        tenant_nodes = {
            UUID(row["node_id"])
            for row in self.store.list_graph_nodes(organization_id, context_id=context_id)
        }
        node_edges: dict[UUID, list[dict[str, object]]] = {}
        edge_index: dict[UUID, dict[str, object]] = {}
        gaps: list[str] = []
        for edge in edges:
            edge_id = UUID(edge["edge_id"])
            source_id = UUID(edge["source_node_id"])
            target_id = UUID(edge["target_node_id"])
            if source_id not in tenant_nodes or target_id not in tenant_nodes:
                gaps.append(f"cross_tenant_or_missing_node:{edge_id}")
                continue
            key = UUID(
                edge["source_node_id"] if direction == "downstream" else edge["target_node_id"]
            )
            node_edges.setdefault(key, []).append(edge)
            edge_index[edge_id] = edge
        queue: deque[_Frontier] = deque([_Frontier(start_node_id, (start_node_id,), (), ())])
        paths: list[GenealogyPath] = []
        visited: set[UUID] = {start_node_id}
        cycle_count = 0
        truncated = False
        allowed_states = (
            {ResolutionState.VERIFIED, ResolutionState.PROBABLE, ResolutionState.POSSIBLE}
            if include_possible
            else {ResolutionState.VERIFIED, ResolutionState.PROBABLE}
        )
        while queue:
            frontier = queue.popleft()
            if len(frontier.edge_path) >= max_hops:
                if node_edges.get(frontier.node_id):
                    truncated = True
                continue
            for edge in node_edges.get(frontier.node_id, []):
                state = ResolutionState(edge["relationship_state"])
                next_id = UUID(
                    edge["target_node_id"] if direction == "downstream" else edge["source_node_id"]
                )
                if state not in allowed_states:
                    gaps.append(f"edge_not_traversed:{edge['edge_id']}:{state.value}")
                    continue
                if next_id in frontier.node_path:
                    cycle_count += 1
                    continue
                node_path = frontier.node_path + (next_id,)
                edge_path = frontier.edge_path + (UUID(edge["edge_id"]),)
                states = frontier.states + (state,)
                visited.add(next_id)
                path_edges = [edge_index[edge_id] for edge_id in edge_path]
                edge_details = [self._edge_detail(item) for item in path_edges]
                quantity_flow, quantity_warnings = self._quantity_flow(path_edges)
                paths.append(
                    GenealogyPath(
                        node_ids=list(node_path),
                        edge_ids=list(edge_path),
                        relationship_states=list(states),
                        edge_details=edge_details,
                        quantity_flow=quantity_flow,
                        quantity_warnings=quantity_warnings,
                        hop_count=len(edge_path),
                    )
                )
                if len(paths) >= max_paths:
                    truncated = True
                    queue.clear()
                    break
                queue.append(_Frontier(next_id, node_path, edge_path, states))
        warnings = []
        pending_reviews = self.store.count_resolution_reviews(organization_id)
        if pending_reviews:
            warnings.append(f"pending_resolution_reviews:{pending_reviews}")
        if gaps:
            warnings.append("unresolved_projection_gaps_may_reduce_completeness")
        if cycle_count:
            warnings.append(f"cycles_detected:{cycle_count}")
        if truncated:
            warnings.append("traversal_limit_reached")
        return GenealogyResult(
            organization_id=organization_id,
            start_node_id=start_node_id,
            direction=direction,
            context_id=context_id,
            max_paths=max_paths,
            dataset_version_id=self._dataset_version(edges),
            projection_run_ids=self._projection_runs(edges),
            paths=paths,
            visited_node_ids=list(visited),
            cycle_count=cycle_count,
            truncated=truncated,
            gaps=sorted(set(gaps)),
            completeness_warnings=sorted(set(warnings)),
        )

    @staticmethod
    def _edge_detail(edge: dict[str, object]) -> dict[str, object]:
        return {
            "edge_id": edge["edge_id"],
            "source_node_id": edge["source_node_id"],
            "target_node_id": edge["target_node_id"],
            "edge_type": edge["edge_type"],
            "event_id": edge["event_id"],
            "context_id": edge.get("context_id"),
            "dataset_version_id": edge.get("dataset_version_id"),
            "projection_run_id": edge.get("projection_run_id"),
            "relationship_state": edge["relationship_state"],
            "quantity": json.loads(edge["quantity_json"]) if edge["quantity_json"] else None,
            "provenance": json.loads(edge["provenance_json"]),
        }

    @staticmethod
    def _quantity_flow(edges: list[dict[str, object]]) -> tuple[list[dict[str, object]], list[str]]:
        flow = []
        units: set[str] = set()
        warnings = []
        for edge in edges:
            quantity = json.loads(edge["quantity_json"]) if edge["quantity_json"] else None
            if not quantity or "value" not in quantity or "unit" not in quantity:
                warnings.append(f"quantity_missing:{edge['edge_id']}")
                continue
            unit = str(quantity["unit"])
            units.add(unit)
            flow.append(
                {
                    "edge_id": edge["edge_id"],
                    "event_id": edge["event_id"],
                    "value": quantity["value"],
                    "unit": unit,
                }
            )
        if len(units) > 1:
            warnings.append("quantity_units_mismatch:" + ",".join(sorted(units)))
        return flow, warnings

    def analyze_contamination(
        self,
        organization_id: UUID,
        contaminated_node_id: UUID,
        *,
        max_hops: int = 20,
        include_possible: bool = True,
        context_id: UUID | None = None,
        max_paths: int = 5000,
    ) -> ImpactAnalysis:
        traversal = self.traverse(
            organization_id,
            contaminated_node_id,
            direction="downstream",
            max_hops=max_hops,
            include_possible=include_possible,
            context_id=context_id,
        )
        node_types = {
            UUID(row["node_id"]): row["node_type"]
            for row in self.store.list_graph_nodes(organization_id, context_id=context_id)
        }
        products: dict[UUID, list[GenealogyPath]] = {}
        facilities: set[UUID] = set()
        for path in traversal.paths:
            endpoint = path.node_ids[-1]
            if node_types.get(endpoint) == "product":
                products.setdefault(endpoint, []).append(path)
            facilities.update(
                node_id for node_id in path.node_ids if node_types.get(node_id) == "facility"
            )
        confirmed, possible = [], []
        for product_id, paths in products.items():
            best = min(
                paths,
                key=lambda path: (
                    max(
                        (self._state_priority(state) for state in path.relationship_states),
                        default=0,
                    ),
                    path.hop_count,
                ),
            )
            state = max(
                best.relationship_states, key=self._state_priority, default=ResolutionState.VERIFIED
            )
            target = (
                confirmed
                if state in {ResolutionState.VERIFIED, ResolutionState.PROBABLE}
                else possible
            )
            target.append(
                ImpactProduct(
                    product_id=product_id,
                    state=state,
                    shortest_hops=min(path.hop_count for path in paths),
                    supporting_paths=paths[:10],
                )
            )
        return ImpactAnalysis(
            organization_id=organization_id,
            contaminated_node_id=contaminated_node_id,
            context_id=context_id,
            dataset_version_id=self._dataset_version_from_paths(traversal.paths),
            confirmed_products=sorted(confirmed, key=lambda item: item.shortest_hops),
            possible_products=sorted(possible, key=lambda item: item.shortest_hops),
            facilities=sorted(facilities, key=str),
            traversed_nodes=len(traversal.visited_node_ids),
            traversed_edges=len({edge_id for path in traversal.paths for edge_id in path.edge_ids}),
            unresolved_gaps=traversal.gaps
            + (["traversal_truncated"] if traversal.truncated else []),
        )

    @staticmethod
    def _state_priority(state: ResolutionState) -> int:
        return {
            ResolutionState.VERIFIED: 0,
            ResolutionState.PROBABLE: 1,
            ResolutionState.POSSIBLE: 2,
            ResolutionState.CONTRADICTED: 3,
            ResolutionState.UNKNOWN: 4,
        }[state]

    @staticmethod
    def _dataset_version(edges: list[dict[str, object]]) -> UUID | None:
        values = {UUID(row["dataset_version_id"]) for row in edges if row.get("dataset_version_id")}
        return next(iter(values)) if len(values) == 1 else None

    @staticmethod
    def _projection_runs(edges: list[dict[str, object]]) -> list[UUID]:
        return sorted(
            {UUID(row["projection_run_id"]) for row in edges if row.get("projection_run_id")},
            key=str,
        )

    @staticmethod
    def _dataset_version_from_paths(paths: list[GenealogyPath]) -> UUID | None:
        versions = {
            UUID(detail["dataset_version_id"])
            for path in paths for detail in path.edge_details
            if detail.get("dataset_version_id")
        }
        return next(iter(versions)) if len(versions) == 1 else None
