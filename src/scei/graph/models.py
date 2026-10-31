from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field

from scei.resolution.models import ResolutionState


class GraphNode(BaseModel):
    organization_id: UUID
    node_id: UUID
    node_type: str = Field(min_length=1, max_length=64)
    label: str = Field(min_length=1, max_length=512)
    attributes: dict[str, object] = Field(default_factory=dict)


class GraphEdge(BaseModel):
    organization_id: UUID
    edge_id: UUID
    source_node_id: UUID
    target_node_id: UUID
    edge_type: str = Field(min_length=1, max_length=64)
    event_id: UUID
    relationship_state: ResolutionState
    quantity: dict[str, object] | None = None
    provenance: list[dict[str, object]] = Field(min_length=1)


class ProjectionResult(BaseModel):
    event_id: UUID
    nodes_written: int
    edges_written: int
    gaps: list[str] = Field(default_factory=list)
