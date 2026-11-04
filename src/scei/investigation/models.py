from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field

from scei.resolution.models import ResolutionState


class GenealogyPath(BaseModel):
    node_ids: list[UUID] = Field(min_length=1)
    edge_ids: list[UUID] = Field(default_factory=list)
    relationship_states: list[ResolutionState] = Field(default_factory=list)
    edge_details: list[dict[str, object]] = Field(default_factory=list)
    quantity_flow: list[dict[str, object]] = Field(default_factory=list)
    quantity_warnings: list[str] = Field(default_factory=list)
    hop_count: int = Field(ge=0)


class GenealogyResult(BaseModel):
    organization_id: UUID
    start_node_id: UUID
    direction: str
    context_id: UUID | None = None
    dataset_version_id: UUID | None = None
    projection_run_ids: list[UUID] = Field(default_factory=list)
    paths: list[GenealogyPath] = Field(default_factory=list)
    visited_node_ids: list[UUID] = Field(default_factory=list)
    cycle_count: int = 0
    truncated: bool = False
    gaps: list[str] = Field(default_factory=list)
    completeness_warnings: list[str] = Field(default_factory=list)


class ImpactProduct(BaseModel):
    product_id: UUID
    state: ResolutionState
    shortest_hops: int
    supporting_paths: list[GenealogyPath] = Field(default_factory=list)


class ImpactAnalysis(BaseModel):
    organization_id: UUID
    contaminated_node_id: UUID
    context_id: UUID | None = None
    dataset_version_id: UUID | None = None
    confirmed_products: list[ImpactProduct] = Field(default_factory=list)
    possible_products: list[ImpactProduct] = Field(default_factory=list)
    facilities: list[UUID] = Field(default_factory=list)
    traversed_nodes: int = 0
    traversed_edges: int = 0
    unresolved_gaps: list[str] = Field(default_factory=list)
