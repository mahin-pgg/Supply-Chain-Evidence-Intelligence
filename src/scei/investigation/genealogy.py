from __future__ import annotations

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
    ) -> GenealogyResult:
        if direction not in {"upstream", "downstream"}:
            raise ValueError("direction must be upstream or downstream")
        edges = self.store.list_graph_edges(organization_id, limit=max_paths * 4)
        node_edges: dict[UUID, list[dict[str, object]]] = {}
        for edge in edges:
            key = UUID(
                edge["source_node_id"] if direction == "downstream" else edge["target_node_id"]
            )
            node_edges.setdefault(key, []).append(edge)
        queue: deque[_Frontier] = deque([_Frontier(start_node_id, (start_node_id,), (), ())])
        paths: list[GenealogyPath] = []
        visited: set[UUID] = {start_node_id}
        cycle_count = 0
        gaps: list[str] = []
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
                paths.append(
                    GenealogyPath(
                        node_ids=list(node_path),
                        edge_ids=list(edge_path),
                        relationship_states=list(states),
                        hop_count=len(edge_path),
                    )
                )
                if len(paths) >= max_paths:
                    truncated = True
                    queue.clear()
                    break
                queue.append(_Frontier(next_id, node_path, edge_path, states))
        return GenealogyResult(
            organization_id=organization_id,
            start_node_id=start_node_id,
            direction=direction,
            paths=paths,
            visited_node_ids=list(visited),
            cycle_count=cycle_count,
            truncated=truncated,
            gaps=sorted(set(gaps)),
        )

    def analyze_contamination(
        self, organization_id: UUID, contaminated_node_id: UUID, *, max_hops: int = 20
    ) -> ImpactAnalysis:
        traversal = self.traverse(
            organization_id,
            contaminated_node_id,
            direction="downstream",
            max_hops=max_hops,
            include_possible=True,
        )
        node_types = {
            UUID(row["node_id"]): row["node_type"]
            for row in self.store.list_graph_nodes(organization_id)
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
